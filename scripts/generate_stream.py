"""
Simulates a streaming source by reading the TLC parquet file and writing
small batches to the landing directory on a configurable interval.

Spark's structured streaming with the file source will pick these up.

I went back and forth on whether to use Kafka for this. Kafka is "more real"
but the operational overhead of running a broker for a demo pipeline felt
excessive. File-based streaming is actually what a lot of teams use for
slower-moving data (hourly files from an FTP drop, etc).
"""

import argparse
import logging
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


def stream_parquet(
    source_path: str,
    output_dir: str,
    batch_size: int = 5000,
    interval_seconds: float = 2.0,
    shuffle: bool = True,
) -> None:
    """Read a parquet file and write it out in small batches over time."""
    src = Path(source_path)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    log.info("reading source file: %s", src)
    df = pd.read_parquet(src)
    log.info("loaded %d rows from source", len(df))

    if shuffle:
        # randomize so we don't just replay time-ordered data
        df = df.sample(frac=1, random_state=42).reset_index(drop=True)

    total_batches = (len(df) + batch_size - 1) // batch_size
    log.info("will emit %d batches of size %d every %.1fs",
             total_batches, batch_size, interval_seconds)

    for batch_idx in range(total_batches):
        start = batch_idx * batch_size
        end = min(start + batch_size, len(df))
        batch_df = df.iloc[start:end].copy()

        # add an ingestion timestamp so we can measure end-to-end latency later
        batch_df["_ingested_at"] = datetime.utcnow().isoformat()

        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
        output_file = out / f"trips_{ts}_batch{batch_idx:06d}.parquet"

        # write to temp then rename — avoids spark reading half-written files
        tmp_file = output_file.with_suffix(".parquet.tmp")
        batch_df.to_parquet(tmp_file, index=False)
        tmp_file.rename(output_file)

        log.info("wrote batch %d/%d (%d rows) -> %s",
                 batch_idx + 1, total_batches, len(batch_df), output_file.name)

        if batch_idx < total_batches - 1:
            time.sleep(interval_seconds)

    log.info("done streaming all batches")


def main():
    parser = argparse.ArgumentParser(
        description="Simulate a streaming source from a TLC parquet file"
    )
    parser.add_argument("--source", required=True,
                        help="path to TLC parquet file")
    parser.add_argument("--output-dir", default="data/raw/streaming",
                        help="where to write streaming batches")
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--interval", type=float, default=2.0,
                        help="seconds between batches")
    parser.add_argument("--no-shuffle", action="store_true")
    args = parser.parse_args()

    try:
        stream_parquet(
            source_path=args.source,
            output_dir=args.output_dir,
            batch_size=args.batch_size,
            interval_seconds=args.interval,
            shuffle=not args.no_shuffle,
        )
    except KeyboardInterrupt:
        log.info("stopped by user")


if __name__ == "__main__":
    main()
