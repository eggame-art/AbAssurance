import re
from dataclasses import dataclass
from pathlib import Path

from extraction.config import PROJECT_ROOT
from extraction.oracle import ORACLE_TABLES
from extraction.sqlserver import SQLSERVER_TABLES
from shared.environment import Environment
from shared.errors import ConfigurationError

SOURCE_TABLES = {"abassurance": ORACLE_TABLES, "assureplus": SQLSERVER_TABLES}


@dataclass(frozen=True, slots=True)
class KafkaConfig:
    bootstrap_servers: str = "127.0.0.1:9092"
    topic_prefix: str = "insurance"
    group_id: str = "abassurance-check"
    timeout: int = 30

    def __post_init__(self) -> None:
        if not all(server.strip() for server in self.bootstrap_servers.split(",")):
            raise ConfigurationError("KAFKA_BOOTSTRAP_SERVERS must contain broker addresses")

        if not re.fullmatch(r"[a-zA-Z0-9_-][a-zA-Z0-9._-]*", self.topic_prefix) or len(self.topic_prefix) > 225:
            raise ConfigurationError("KAFKA_TOPIC_PREFIX must use Kafka topic characters and at most 225 characters")

        if not self.group_id.strip():
            raise ConfigurationError("KAFKA_GROUP_ID must not be blank")

        if not 1 <= self.timeout <= 300:
            raise ConfigurationError("KAFKA_TIMEOUT must be between 1 and 300")

    def topic(self, source: str, entity: str) -> str:
        if source not in SOURCE_TABLES or entity not in SOURCE_TABLES[source]:
            raise ValueError(f"Unknown source/entity: {source}/{entity}")

        return f"{self.topic_prefix}.{source}.{entity}"

    def topics(self) -> list[str]:
        return [self.topic(source, entity) for source, tables in SOURCE_TABLES.items() for entity in tables]


def load_kafka_config(env_file: Path | None = None) -> KafkaConfig:
    env = Environment.from_file(env_file if env_file is not None else PROJECT_ROOT / ".env")

    return KafkaConfig(
        bootstrap_servers=env.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
        topic_prefix=env.get("KAFKA_TOPIC_PREFIX", "insurance"),
        group_id=env.get("KAFKA_GROUP_ID", "abassurance-check"),
        timeout=env.integer("KAFKA_TIMEOUT", "30", 300),
    )
