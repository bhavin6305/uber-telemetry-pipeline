"""
stream_utils.py - Small helpers shared by the Silver and Gold streaming jobs.
"""

# One watermark setting for the whole pipeline. Events arriving more than this far behind
# the newest event time seen so far are considered "too late" and are dropped by stateful operators.
DEFAULT_WATERMARK = "10 minutes"


def run_query(query, available_now, label):
    """Block until a streaming query finishes. Handles Ctrl+C and prints progress in continuous mode."""
    try:
        if available_now:
            query.awaitTermination()
        else:
            # Poll with a timeout so Ctrl+C is handled promptly (a bare awaitTermination blocks in the JVM).
            while query.isActive:
                query.awaitTermination(15)
                p = query.lastProgress
                if p:
                    wm = p.get("eventTime", {}).get("watermark", "n/a")
                    print(f"[{label}] batch={p['batchId']} rows={p['numInputRows']} watermark={wm}")
    except KeyboardInterrupt:
        print(f"[{label}] stopping stream...")
        query.stop()