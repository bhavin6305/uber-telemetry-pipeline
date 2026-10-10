"""
producer.py - Replays Uber trips from CSV into Kafka as JSON events.

Features:
  * Connection retries with exponential backoff + jitter (broker may still be booting)
  * Fail-fast check that the target topic exists
  * Durable sends: acks=all, retries, ordered in-flight requests, delivery callbacks
  * Drift-free pacing (monotonic clock) so "one event per 0.5s" stays accurate
  * Deterministic ride_id (uuid5) so re-sent events are true duplicates
  * Optional late (out-of-order) and duplicate events to exercise Silver/Gold logic
  * Graceful shutdown on Ctrl+C / SIGTERM: flushes buffered messages before exiting

Usage:
  python3 producer.py --limit 200 --interval 0.05      # quick smoke test
  python3 producer.py --limit 5000                      # ~42 min at 0.5s/event
"""
import argparse
import csv
import heapq
import json
import logging
import os
import random
import signal
import sys
import threading
import time
import uuid

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

log = logging.getLogger("producer")
stop = threading.Event()  # set by signal handlers; also used as an interruptible sleep


def parse_args():
    p = argparse.ArgumentParser(description="Stream Uber trip events into Kafka.")
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    p.add_argument("--topic", default="uber-ride-events")
    p.add_argument("--input", default="data/uber_trips.csv")
    p.add_argument("--interval", type=float, default=0.5, help="Seconds between events (0 = as fast as possible)")
    p.add_argument("--limit", type=int, default=0, help="Max source rows to read (0 = all)")
    p.add_argument("--dup-pct", type=float, default=0.02, help="Fraction of events re-sent as duplicates")
    p.add_argument("--late-pct", type=float, default=0.02, help="Fraction of events delivered late/out of order")
    p.add_argument("--late-min", type=int, default=100, help="Minimum lateness, in number of events")
    p.add_argument("--late-max", type=int, default=1500, help="Maximum lateness, in number of events")
    p.add_argument("--seed", type=int, default=42, help="RNG seed for reproducible runs")
    p.add_argument("--log-every", type=int, default=100, help="Progress log frequency (events)")
    p.add_argument("--connect-attempts", type=int, default=10)
    return p.parse_args()


def connect_with_retry(bootstrap, max_attempts, base_delay=1.0, max_delay=30.0):
    """Create a KafkaProducer, retrying with exponential backoff + jitter."""
    delay = base_delay
    for attempt in range(1, max_attempts + 1):
        if stop.is_set():
            sys.exit("Interrupted while connecting.")
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap,
                acks="all",                              # wait for the full ISR to confirm
                retries=5,                               # transient send failures are retried
                max_in_flight_requests_per_connection=1, # retries can't reorder messages
                linger_ms=20,                            # tiny batching window
                key_serializer=lambda k: k.encode("utf-8"),
                value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
            )
            log.info("Connected to Kafka at %s (attempt %d)", bootstrap, attempt)
            return producer
        except (NoBrokersAvailable, KafkaError) as exc:
            wait = min(delay, max_delay) + random.uniform(0, 1)
            log.warning("Kafka not reachable (%s). Attempt %d/%d, retrying in %.1fs",
                        type(exc).__name__, attempt, max_attempts, wait)
            stop.wait(wait)
            delay *= 2
    sys.exit(f"Could not connect to Kafka at {bootstrap} after {max_attempts} attempts.")


def _float(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except ValueError:
        return None


def _int(v):
    f = _float(v)
    return int(f) if f is not None else None


def _ts(v):
    """'2025-09-01 00:24:18' -> '2025-09-01T00:24:18' (ISO 8601); empty -> None."""
    return v.strip().replace(" ", "T") if v and v.strip() else None


def row_to_event(idx, row):
    pickup = _ts(row.get("pickup_datetime"))
    pu, do = _int(row.get("PULocationID")), _int(row.get("DOLocationID"))
    # Deterministic ID: the same source row always yields the same ride_id,
    # so a re-sent event is a genuine duplicate that Silver can drop.
    ride_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"uber-ride/{idx}/{pickup}/{pu}/{do}"))
    return {
        "ride_id": ride_id,
        "event_time": pickup,  # event time = pickup time (what watermarks will use)
        "request_time": _ts(row.get("request_datetime")),
        "dropoff_time": _ts(row.get("dropoff_datetime")),
        "pickup_zone_id": pu,
        "dropoff_zone_id": do,
        "trip_miles": _float(row.get("trip_miles")),
        "trip_time_sec": _int(row.get("trip_time")),
        "base_fare": _float(row.get("base_passenger_fare")),
        "tips": _float(row.get("tips")),
        "driver_pay": _float(row.get("driver_pay")),
        "base_num": (row.get("dispatching_base_num") or None),
    }


def main():
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    def _handle_signal(signum, _frame):
        log.info("Signal %s received, shutting down gracefully...", signum)
        stop.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    if not os.path.exists(args.input):
        sys.exit(f"Input file not found: {args.input} (run prep_sample.py first)")

    rng = random.Random(args.seed)
    producer = connect_with_retry(args.bootstrap, args.connect_attempts)

    # Fail fast if the topic is missing (auto-create is disabled on our broker).
    if producer.partitions_for(args.topic) is None:
        producer.close()
        sys.exit(f"Topic '{args.topic}' does not exist. Create it first (see Milestone 1).")

    stats = {"produced": 0, "acked": 0, "failed": 0, "late": 0, "dups": 0}

    def on_ok(_metadata):
        stats["acked"] += 1

    def on_err(exc):
        stats["failed"] += 1
        log.error("Delivery failed: %s", exc)

    def emit(event):
        producer.send(args.topic, key=event["ride_id"], value=event).add_callback(on_ok).add_errback(on_err)
        stats["produced"] += 1

    delayed = []  # min-heap of (release_at_index, seq, event)
    seq = 0
    next_tick = time.monotonic()

    try:
        with open(args.input, newline="", encoding="utf-8") as f:
            for idx, row in enumerate(csv.DictReader(f)):
                if stop.is_set() or (args.limit and idx >= args.limit):
                    break

                # 1) Release late/duplicate events that are now due.
                while delayed and delayed[0][0] <= idx:
                    emit(heapq.heappop(delayed)[2])

                event = row_to_event(idx, row)

                # 2) Either deliver on time, or hold it back to arrive late (out of order).
                if rng.random() < args.late_pct:
                    seq += 1
                    heapq.heappush(delayed, (idx + rng.randint(args.late_min, args.late_max), seq, event))
                    stats["late"] += 1
                else:
                    emit(event)

                # 3) Occasionally re-send the same event again shortly after.
                if rng.random() < args.dup_pct:
                    seq += 1
                    heapq.heappush(delayed, (idx + rng.randint(1, 50), seq, event))
                    stats["dups"] += 1

                if (idx + 1) % args.log_every == 0:
                    log.info("rows=%d produced=%d acked=%d failed=%d held_back=%d",
                             idx + 1, stats["produced"], stats["acked"], stats["failed"], len(delayed))

                # 4) Drift-free pacing.
                if args.interval > 0:
                    next_tick += args.interval
                    stop.wait(max(0.0, next_tick - time.monotonic()))

        # Drain anything still held back, unless the user interrupted.
        if not stop.is_set():
            while delayed:
                emit(heapq.heappop(delayed)[2])
    finally:
        log.info("Flushing buffered messages...")
        producer.flush(timeout=30)
        producer.close()
        log.info("Done. produced=%d acked=%d failed=%d (late=%d, duplicates=%d)",
                 stats["produced"], stats["acked"], stats["failed"], stats["late"], stats["dups"])

    sys.exit(1 if stats["failed"] else 0)


if __name__ == "__main__":
    main()