import os
import re
from dataclasses import dataclass
from urllib.parse import unquote

from shared.environment import Environment
from shared.errors import ConfigurationError
from spark_pipeline.hdfs import validate_hdfs_root


@dataclass(frozen=True)
class APIConfig:
    model_root: str = "hdfs://namenode:9000/abassurance-models/churn"
    model_id: str | None = None
    allow_experimental: bool = False
    spark_master: str = "local[2]"

    def __post_init__(self) -> None:
        root = validate_hdfs_root(self.model_root)

        if unquote(root) != root:
            raise ConfigurationError("Encoded HDFS paths are not supported")

        object.__setattr__(self, "model_root", root)

        if self.model_id is not None and not re.fullmatch(r"[0-9a-f]{32}", self.model_id):
            raise ConfigurationError("Invalid API model identity")

        if type(self.allow_experimental) is not bool:
            raise ConfigurationError("allow_experimental must be boolean")

        if not self.spark_master.strip():
            raise ConfigurationError("API Spark master must not be empty")

    @classmethod
    def from_environment(cls) -> "APIConfig":
        env = Environment(os.environ)
        return cls(
            model_root=env.get("MODEL_OUTPUT_DIR", cls.model_root),
            model_id=env.get("API_MODEL_ID") or None,
            allow_experimental=env.boolean("API_ALLOW_EXPERIMENTAL", "false"),
            spark_master=env.get("API_SPARK_MASTER", "local[2]"),
        )
