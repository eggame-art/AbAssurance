from pathlib import Path

import pytest

from extraction.config import OracleConfig, Settings, SQLServerConfig


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--run-spark-integration", action="store_true", default=False, help="Run local Spark integration tests"
    )
    parser.addoption(
        "--run-spark-kafka-integration",
        action="store_true",
        default=False,
        help="Run real Spark/Kafka/database integration tests",
    )
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="Run integration tests against Oracle and SQL Server",
    )
    parser.addoption(
        "--run-kafka-integration",
        action="store_true",
        default=False,
        help="Run Kafka integration tests against Docker databases and broker",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list) -> None:
    for marker, option in (
        ("spark_integration", "--run-spark-integration"),
        ("spark_kafka_integration", "--run-spark-kafka-integration"),
    ):
        if not config.getoption(option):
            for item in items:
                if marker in item.keywords:
                    item.add_marker(pytest.mark.skip(reason=f"Pass {option} to enable"))
    if not config.getoption("--run-kafka-integration"):
        for item in items:
            if "kafka_integration" in item.keywords:
                item.add_marker(pytest.mark.skip(reason="Pass --run-kafka-integration to use Docker Kafka"))
    if not config.getoption("--run-integration"):
        skip = pytest.mark.skip(reason="Pass --run-integration to use Docker databases")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip)


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        oracle=OracleConfig("test_user", "oracle-test-secret", "oracle:1521/test"),
        sqlserver=SQLServerConfig("sqlserver", 1433, "test", "test_user", "sql-test-secret"),
        output_dir=tmp_path / "exports",
    )
