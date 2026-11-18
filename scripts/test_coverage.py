import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
TESTS_DIR = ROOT / "server" / "tests"
COVERAGE_DIR = ROOT / "coverage" / "backend"


def main() -> int:
    os.chdir(ROOT)
    sys.path.insert(0, str(ROOT / "server" / "src"))

    COVERAGE_DIR.mkdir(parents=True, exist_ok=True)

    arguments = sys.argv[1:].copy()
    with_isolation = "--with-customer-isolation" in arguments

    if with_isolation:
        arguments.remove("--with-customer-isolation")

        targets = [str(path) for path in sorted(TESTS_DIR.glob("test_*.py"))]

        targets.extend([str(TESTS_DIR / "spark" / "test_customer_isolation.py"), "--run-spark-integration"])
    else:
        targets = ["server/tests", "--ignore=server/tests/spark", "--ignore=server/tests/integration"]

    coverage_options = [
        "--cov=abassurance",
        "--cov-branch",
        "--cov-report=term:skip-covered",
        "--cov-report=html:coverage/backend/html",
        "--cov-report=xml:coverage/backend/coverage.xml",
        "--cov-report=json:coverage/backend/coverage.json",
        "--junitxml=coverage/backend/tests.xml",
    ]

    return int(pytest.main([*targets, *coverage_options, *arguments]))


if __name__ == "__main__":
    raise SystemExit(main())
