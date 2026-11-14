import math
import re
from dataclasses import dataclass
from urllib.parse import unquote, urlsplit

from dataset_preparation.config import FEATURE_COLUMNS
from shared.errors import ConfigurationError
from spark_pipeline.hdfs import validate_hdfs_root

VERSION = 1
ALGORITHMS = ("logistic-regression", "random-forest")


@dataclass(frozen=True)
class TrainingConfig:
    dataset_root: str
    output_root: str
    dataset_id: str | None = None
    mode: str = "production"
    algorithm: str = "logistic-regression"
    features: tuple[str, ...] = tuple(FEATURE_COLUMNS)
    imputation: str = "median"
    standardize: bool = True
    class_weight: str = "balanced"
    threshold: float = 0.5
    max_iter: int = 100
    reg_param: float = 0.1
    elastic_net_param: float = 0.0
    tolerance: float = 1e-6
    seed: int = 42
    num_trees: int = 100
    max_depth: int = 5

    def __post_init__(self) -> None:
        for key in ("dataset_root", "output_root"):
            root = validate_hdfs_root(getattr(self, key))

            if unquote(root) != root:
                raise ConfigurationError("Encoded HDFS paths are not supported")

            object.__setattr__(self, key, root)

        source, output = (urlsplit(root).path for root in (self.dataset_root, self.output_root))

        if source == output or source.startswith(output + "/") or output.startswith(source + "/"):
            raise ConfigurationError("Dataset and model HDFS roots must not overlap")

        if self.dataset_id is not None and not re.fullmatch(r"[0-9a-f]{64}", self.dataset_id):
            raise ConfigurationError("Invalid dataset-id")

        if self.mode not in ("production", "experimental") or self.algorithm not in ALGORITHMS:
            raise ConfigurationError("Unknown training mode or algorithm")

        if (
            not self.features
            or len(set(self.features)) != len(self.features)
            or not set(self.features) <= set(FEATURE_COLUMNS)
        ):
            raise ConfigurationError("Features must be a unique nonempty subset of US09 FEATURE_COLUMNS")

        if self.imputation not in ("mean", "median") or self.class_weight not in ("none", "balanced"):
            raise ConfigurationError("Unknown imputation or class weighting strategy")

        if type(self.standardize) is not bool:
            raise ConfigurationError("standardize must be boolean")

        for value in (self.threshold, self.reg_param, self.elastic_net_param, self.tolerance):
            if type(value) not in (float, int) or not math.isfinite(value):
                raise ConfigurationError("Model parameters must be finite numbers")

        if not 0 < self.threshold < 1 or self.reg_param < 0 or not 0 <= self.elastic_net_param <= 1:
            raise ConfigurationError("Invalid threshold or regularization")

        if self.tolerance <= 0:
            raise ConfigurationError("tolerance must be positive")

        for value, upper in ((self.max_iter, 10000), (self.num_trees, 10000), (self.max_depth, 30)):
            if type(value) is not int or not 1 <= value <= upper:
                raise ConfigurationError("Invalid iteration/tree count or depth")

        if type(self.seed) is not int or not -(2**31) <= self.seed < 2**31:
            raise ConfigurationError("seed must be a signed 32-bit integer")
