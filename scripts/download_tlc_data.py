"""
Downloads NYC TLC yellow taxi trip data.

The TLC moved to parquet format in 2022 so we only support 2022+.
Older data is CSV and has a different schema, not worth supporting.
"""

import argparse
import logging
import sys
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


def download_file(url: str, dest: Path, chunk_size: int = 1024 * 1024) -> None:
    """Stream a download so we don't load the whole thing into memory."""
    log.info("downloading %s -> %s", url, dest)
    dest.parent.mkdir(parents=True, exist_ok=True)

    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        downloaded = 0

        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total:
                        pct = 100 * downloaded / total
                        print(f"  {pct:5.1f}% ({downloaded / 1e6:.1f} MB)",
                              end="\r", flush=True)
        print()
    log.info("download complete: %.1f MB", downloaded / 1e6)


def download_tlc_month(year: int, month: int, output_dir: Path,
                      taxi_type: str = "yellow") -> Path:
    if taxi_type not in {"yellow", "green", "fhv", "fhvhv"}:
        raise ValueError(f"unsupported taxi_type: {taxi_type}")
    if year < 2022:
        raise ValueError("only 2022+ data supported (schema issues pre-2022)")

    filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    url = f"{BASE_URL}/{filename}"
    dest = output_dir / filename

    if dest.exists():
        log.info("file already exists, skipping: %s", dest)
        return dest

    download_file(url, dest)
    return dest


def download_zone_lookup(output_dir: Path) -> Path:
    dest = output_dir / "taxi_zone_lookup.csv"
    if dest.exists():
        log.info("zone lookup already exists")
        return dest
    download_file(ZONE_URL, dest)
    return dest


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--taxi-type", default="yellow",
                        choices=["yellow", "green", "fhv", "fhvhv"])
    parser.add_argument("--output-dir", default="data/raw")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)

    try:
        trip_file = download_tlc_month(
            args.year, args.month, output_dir, args.taxi_type,
        )
        zone_file = download_zone_lookup(output_dir)
        log.info("all done. trip data: %s", trip_file)
        log.info("zone lookup: %s", zone_file)
    except requests.HTTPError as e:
        log.error("download failed: %s", e)
        log.error("the TLC occasionally renames files, check "
                  "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page")
        sys.exit(1)


if __name__ == "__main__":
    main()
