"""Run the sample Athena queries and print the results as text tables.

  python scripts/query.py --workgroup W --database D                 # all queries
  python scripts/query.py --workgroup W --database D queries/03_*.sql
  python scripts/query.py --workgroup W --database D --sql "SELECT 1"
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

QUERIES_DIR = Path(__file__).resolve().parents[1] / "queries"
USD_PER_TB = 5.0
MIN_BILLED_BYTES = 10 * 1024 ** 2  # Athena bills at least 10 MB per query


def format_table(rows: list[list[str]], max_width: int = 32) -> str:
    if not rows:
        return "(no rows)"
    rows = [[(c if len(c) <= max_width else c[: max_width - 1] + "…") for c in row] for row in rows]
    widths = [max(len(row[i]) for row in rows) for i in range(len(rows[0]))]
    line = lambda row: " | ".join(c.ljust(w) for c, w in zip(row, widths))  # noqa: E731
    sep = "-+-".join("-" * w for w in widths)
    return "\n".join([line(rows[0]), sep] + [line(r) for r in rows[1:]])


def estimated_cost(bytes_scanned: int) -> float:
    return max(bytes_scanned, MIN_BILLED_BYTES) / 1024 ** 4 * USD_PER_TB


def run_query(athena, sql: str, workgroup: str, database: str) -> tuple[list[list[str]], dict]:
    qid = athena.start_query_execution(
        QueryString=sql,
        WorkGroup=workgroup,
        QueryExecutionContext={"Database": database},
    )["QueryExecutionId"]
    delay = 0.5
    while True:
        execution = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = execution["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(delay)
        delay = min(delay * 1.5, 3)
    if state != "SUCCEEDED":
        raise RuntimeError(execution["Status"].get("StateChangeReason", state))

    rows = []
    for page in athena.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
        for row in page["ResultSet"]["Rows"]:
            rows.append([d.get("VarCharValue", "") for d in row["Data"]])
    return rows, execution["Statistics"]


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("files", nargs="*", help="SQL files (default: every file in queries/)")
    p.add_argument("--workgroup", required=True)
    p.add_argument("--database", required=True)
    p.add_argument("--region", default=None)
    p.add_argument("--sql", help="run this SQL string instead of files")
    args = p.parse_args(argv)

    import boto3

    athena = boto3.client("athena", region_name=args.region)
    if args.sql:
        jobs = [("inline", args.sql)]
    else:
        files = [Path(f) for f in args.files] or sorted(QUERIES_DIR.glob("*.sql"))
        jobs = [(f.name, f.read_text()) for f in files]

    failures, total_cost = 0, 0.0
    for name, sql in jobs:
        print(f"\n=== {name} ===")
        try:
            rows, stats = run_query(athena, sql.strip().rstrip(";"), args.workgroup, args.database)
        except Exception as exc:  # keep going with the other queries
            failures += 1
            print(f"FAILED: {exc}", file=sys.stderr)
            continue
        cost = estimated_cost(stats.get("DataScannedInBytes", 0))
        total_cost += cost
        print(format_table(rows))
        print(f"({len(rows) - 1} rows, {stats.get('DataScannedInBytes', 0) / 1024:.1f} KB scanned, "
              f"{stats.get('TotalExecutionTimeInMillis', 0)} ms, ~${cost:.6f})")
    print(f"\nEstimated Athena cost for this run: ~${total_cost:.6f}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
