from pathlib import Path

import pytest

from extraction.config import OracleConfig, Settings, SQLServerConfig


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="Run integration tests against Oracle and SQL Server",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list) -> None:
    if (not config.getoption("--run-integration")):
        skip = pytest.mark.skip(reason="Pass --run-integration to use Docker databases")
        for item in items:
            if ("integration" in item.keywords):
                item.add_marker(skip)


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        oracle=OracleConfig("test_user", "oracle-test-secret", "oracle:1521/test"),
        sqlserver=SQLServerConfig("sqlserver", 1433, "test", "test_user", "sql-test-secret"),
        output_dir=tmp_path / "exports",
    )
