import logging
import subprocess
from pathlib import Path

from shared.errors import InitializationError, error_context, run_cli

ROOT = Path(__file__).resolve().parents[1]
LOGGER = logging.getLogger(__name__)


def _run_compose(arguments: list[str], *, sql: str | None = None) -> None:
    subprocess.run(["docker", "compose", *arguments], input=sql, text=True, encoding="utf-8", cwd=ROOT, check=True)


def initialize_databases() -> None:
    with error_context("Database initialization failed", (OSError, subprocess.CalledProcessError), InitializationError):
        _run_compose(["up", "-d", "--wait", "oracle", "sqlserver"])

        folder = ROOT / "database" / "oracle" / "init"
        oracle_sql = "\n".join(
            (folder / filename).read_text(encoding="utf-8") for filename in ("schema.sql", "seed.sql")
        )

        _run_compose(["exec", "-T", "oracle", "sqlplus", "-s", "-L", "/", "as", "sysdba"], sql=oracle_sql + "\nEXIT\n")
        _run_compose(["up", "--no-deps", "--exit-code-from", "sqlserver-init", "sqlserver-init"])


def main() -> int:
    logging.basicConfig(format="%(levelname)s %(message)s")

    status = run_cli(initialize_databases, LOGGER, "Initialization failed")

    if (status == 0):
        print("Oracle and SQL Server initialized; existing data preserved.")

    return status


if (__name__ == "__main__"):
    raise SystemExit(main())
