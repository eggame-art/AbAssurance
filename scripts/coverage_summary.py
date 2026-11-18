import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

REPORTS = {
    "backend": ROOT / "coverage/backend/coverage.json",
    "frontend": ROOT / "client/coverage/coverage-summary.json",
}


def main() -> int:
    if len(sys.argv) != 2 or sys.argv[1] not in REPORTS:
        print("Usage: python coverage_summary.py <backend|frontend>", file=sys.stderr)
        return 1

    kind = sys.argv[1]
    path = REPORTS[kind]

    print(f"### {kind.capitalize()} coverage\n")

    if not path.exists():
        print("Report unavailable: check the test step logs.")
        return 0

    data = json.loads(path.read_text(encoding="utf-8"))

    if kind == "backend":
        totals = data["totals"]

        statements = totals["num_statements"]
        branches_total = totals["num_branches"]

        lines = totals["covered_lines"] / statements * 100 if statements else 100
        branches = totals["covered_branches"] / branches_total * 100 if branches_total else 100
    else:
        totals = data["total"]

        lines = totals["lines"]["pct"]
        branches = totals["branches"]["pct"]

    print("| Metric | Coverage |")
    print("|---|---:|")
    print(f"| Lines | {lines:.2f}% |")
    print(f"| Branches | {branches:.2f}% |")

    print("\nDownload the coverage artifact and open index.html for the per-file report.")

    if kind == "backend":
        print(
            "\nSee the test log for the selected Spark tests. Other external-service tests are not part of this report."
        )
    else:
        print("\nIncludes React components; current tests mainly exercise API services and controllers.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
