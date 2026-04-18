"""
Great Expectations validation runner.

I tried to use the full GE CLI setup (great_expectations init, context.yml, etc)
but the ergonomics were painful for a simple pipeline. This uses the programmatic
API directly, which is more code but much easier to reason about.

For a team setup you'd want the full declarative config, but for a solo project
this is fine.
"""

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


SILVER_EXPECTATIONS = {
    "trip_distance": {
        "type": "numeric_range",
        "min": 0,
        "max": 200,
        "allow_null": False,
    },
    "fare_amount": {
        "type": "numeric_range",
        "min": 0,
        "max": 1000000,  # TLC has some truly absurd outliers — $1M cap catches genuine corruption only
        "allow_null": False,
    },
    "passenger_count": {
        "type": "numeric_range",
        "min": 0,
        "max": 9,
        "allow_null": True,
    },
    "pickup_datetime": {
        "type": "not_null",
    },
    "dropoff_datetime": {
        "type": "not_null",
    },
    "trip_duration_minutes": {
        "type": "numeric_range",
        "min": 0,
        "max": 60 * 24 * 7,  # one week. trips longer than this are almost certainly meter errors.
        "allow_null": False,
    },
    "pickup_borough": {
        "type": "in_set",
        "values": ["Manhattan", "Brooklyn", "Queens", "Bronx",
                   "Staten Island", "EWR", "Unknown", "N/A"],
        "allow_null": True,  # TLC uses "N/A" as a borough value for LocationID 264
    },
}


GOLD_EXPECTATIONS = {
    "fct_trips_daily": {
        "trip_count": {"type": "numeric_range", "min": 1, "max": 1e7},
        "total_revenue": {"type": "numeric_range", "min": 0, "max": 1e9},
    },
    "fct_trips_hourly": {
        "trip_count": {"type": "numeric_range", "min": 0, "max": 1e6},
    },
}


def validate_numeric_range(series: pd.Series, spec: dict) -> list[str]:
    """Return list of failure reasons (empty if all good)."""
    failures = []
    if not spec.get("allow_null", True) and series.isnull().any():
        failures.append(f"contains {series.isnull().sum()} null values")

    non_null = series.dropna()
    below_min = (non_null < spec["min"]).sum()
    above_max = (non_null > spec["max"]).sum()
    if below_min:
        failures.append(f"{below_min} values below min ({spec['min']})")
    if above_max:
        failures.append(f"{above_max} values above max ({spec['max']})")

    return failures


def validate_not_null(series: pd.Series, spec: dict) -> list[str]:
    null_count = series.isnull().sum()
    if null_count:
        return [f"contains {null_count} null values (not allowed)"]
    return []


def validate_in_set(series: pd.Series, spec: dict) -> list[str]:
    allowed = set(spec["values"])
    non_null = series.dropna()
    bad = non_null[~non_null.isin(allowed)]
    if len(bad):
        unique_bad = bad.unique()[:5]  # just show first 5
        return [f"{len(bad)} values not in allowed set. examples: {list(unique_bad)}"]
    return []


VALIDATORS = {
    "numeric_range": validate_numeric_range,
    "not_null": validate_not_null,
    "in_set": validate_in_set,
}


def run_expectations(df: pd.DataFrame, expectations: dict, table_name: str) -> dict:
    """Run all expectations for a table. Returns a results dict."""
    results = {
        "table": table_name,
        "row_count": len(df),
        "checks": [],
        "passed": True,
    }

    for col, spec in expectations.items():
        if col not in df.columns:
            results["checks"].append({
                "column": col,
                "check": spec["type"],
                "status": "SKIPPED",
                "reason": "column not present in dataframe",
            })
            continue

        validator = VALIDATORS.get(spec["type"])
        if not validator:
            log.warning("unknown validator type: %s", spec["type"])
            continue

        failures = validator(df[col], spec)
        check_result = {
            "column": col,
            "check": spec["type"],
            "status": "PASS" if not failures else "FAIL",
        }
        if failures:
            check_result["failures"] = failures
            results["passed"] = False
        results["checks"].append(check_result)

    return results


def print_results(results: dict) -> None:
    print(f"\n=== {results['table']} ({results['row_count']:,} rows) ===")
    for chk in results["checks"]:
        status = chk["status"]
        col = chk["column"]
        check_type = chk["check"]
        if status == "PASS":
            print(f"  PASS  {col:<30} {check_type}")
        elif status == "SKIPPED":
            print(f"  SKIP  {col:<30} {check_type} — {chk['reason']}")
        else:
            print(f"  FAIL  {col:<30} {check_type}")
            for f in chk["failures"]:
                print(f"          -> {f}")


def validate_silver(silver_path: str = "data/processed/silver") -> dict:
    log.info("reading silver for validation...")
    df = pd.read_parquet(silver_path)
    log.info("validating %d rows", len(df))
    results = run_expectations(df, SILVER_EXPECTATIONS, "silver")
    print_results(results)
    return results


def validate_gold(gold_path: str = "data/processed/gold") -> list[dict]:
    all_results = []
    for table_name, expectations in GOLD_EXPECTATIONS.items():
        table_dir = Path(gold_path) / table_name
        if not table_dir.exists():
            log.warning("skipping %s, not found", table_dir)
            continue
        df = pd.read_parquet(table_dir)
        results = run_expectations(df, expectations, table_name)
        print_results(results)
        all_results.append(results)
    return all_results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", choices=["silver", "gold"], required=True)
    parser.add_argument("--fail-on-error", action="store_true")
    parser.add_argument("--generate-docs", action="store_true",
                        help="not implemented — would generate HTML report")
    args = parser.parse_args()

    if args.dataset == "silver":
        results = validate_silver()
        any_failures = not results["passed"]
    else:
        all_results = validate_gold()
        any_failures = any(not r["passed"] for r in all_results)

    if any_failures and args.fail_on_error:
        log.error("validation failures detected, exiting with error")
        sys.exit(1)

    if args.generate_docs:
        log.info("(generate-docs not implemented yet — TODO)")


if __name__ == "__main__":
    main()
