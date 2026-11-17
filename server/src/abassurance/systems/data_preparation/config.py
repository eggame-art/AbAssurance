from dataclasses import dataclass
from datetime import date, timedelta
from math import ceil, log
from urllib.parse import unquote, urlsplit

from abassurance.shared.errors import ConfigurationError
from abassurance.shared.models.history import CANCELLATION_TYPES
from abassurance.systems.spark_pipeline.hdfs import validate_hdfs_root

VERSION = 3

TRAINING_BLOCKERS = [
    "No effective cancellation date or dated contract status history",
    "No complete follow-up period to distinguish negatives from censored contracts",
    "No business availability timestamps or point-in-time versions for features",
    "Source snapshots retain first valid records, not successive business updates",
]

FEATURE_COLUMNS = [
    "contract_tenure_days",
    "claim_count",
    "payment_count",
    "payment_amount_known_total",
    "payment_amount_missing_count",
    "days_since_last_payment",
]


@dataclass(frozen=True)
class PreparationConfig:
    source_root: str
    output_root: str
    observation_date: str
    purpose: str = "training"
    history_root: str | None = None
    horizon_days: int = 90
    cancellation_types: tuple[str, ...] = CANCELLATION_TYPES
    observation_dates: tuple[str, ...] = ()
    evaluation_start: str | None = None
    embargo_days: int = 0
    evaluation_confidence: float = 0.95
    evaluation_margin: float = 0.10

    @property
    def dates(self) -> tuple[str, ...]:
        return self.observation_dates or (self.observation_date,)

    @property
    def required_evaluation_customers_per_class(self) -> int:
        return ceil(log(2 / (1 - self.evaluation_confidence)) / (2 * self.evaluation_margin**2))

    def __post_init__(self) -> None:
        for root in (self.source_root, self.output_root, *([self.history_root] if self.history_root else [])):
            validate_hdfs_root(root)

            if unquote(root) != root:
                raise ConfigurationError("Encoded HDFS paths are not supported")

        source = urlsplit(self.source_root).path.rstrip("/")
        output = urlsplit(self.output_root).path.rstrip("/")

        if source == output or source.startswith(output + "/") or output.startswith(source + "/"):
            raise ConfigurationError("Source and output HDFS roots must not overlap")

        try:
            for value in (*self.dates, *([self.evaluation_start] if self.evaluation_start else [])):
                if date.fromisoformat(value).isoformat() != value:
                    raise ValueError
        except (ValueError, TypeError) as exc:
            raise ConfigurationError("observation-date must use YYYY-MM-DD") from exc

        if self.history_root:
            history = urlsplit(self.history_root).path.rstrip("/")

            if history == output or history.startswith(output + "/") or output.startswith(history + "/"):
                raise ConfigurationError("History and output HDFS roots must not overlap")

        if type(self.horizon_days) is not int or not 1 <= self.horizon_days <= 3650:
            raise ConfigurationError("horizon-days must be between 1 and 3650")

        if len(set(self.dates)) != len(self.dates):
            raise ConfigurationError("Duplicate observation dates")

        if self.observation_dates:
            object.__setattr__(self, "observation_dates", tuple(sorted(self.observation_dates)))
            object.__setattr__(self, "observation_date", self.observation_dates[0])

        if type(self.embargo_days) is not int or self.embargo_days < 0:
            raise ConfigurationError("embargo-days must be non-negative")

        if not 0.5 < self.evaluation_confidence < 1 or not 0 < self.evaluation_margin < 0.5:
            raise ConfigurationError("Evaluation confidence must be in (0.5, 1), margin in (0, 0.5)")

        if len(self.dates) > 1 and not self.evaluation_start:
            raise ConfigurationError("Multiple observation dates require --evaluation-start")

        if self.evaluation_start:
            training = [value for value in self.dates if value < self.evaluation_start]
            evaluation = [value for value in self.dates if value >= self.evaluation_start]

            if not training or not evaluation:
                raise ConfigurationError("Split requires observation dates on both sides of evaluation-start")

            try:
                valid = any(
                    date.fromisoformat(value) + timedelta(days=self.horizon_days + self.embargo_days)
                    <= date.fromisoformat(self.evaluation_start)
                    for value in training
                )
            except OverflowError as exc:
                raise ConfigurationError("Observation horizon overflows the supported date range") from exc

            if not valid:
                raise ConfigurationError("All training label horizons overlap evaluation or its embargo")

        if not self.cancellation_types or not set(self.cancellation_types) <= set(CANCELLATION_TYPES):
            raise ConfigurationError("Unknown or empty cancellation-types")

        object.__setattr__(self, "cancellation_types", tuple(sorted(set(self.cancellation_types))))

        if self.purpose not in ("training", "technical-validation", "synthetic-training"):
            raise ConfigurationError("Unknown dataset purpose")

        if self.purpose == "technical-validation" and self.history_root:
            raise ConfigurationError("Use training or synthetic-training with --history-dir")

        if self.purpose == "technical-validation" and len(self.dates) != 1:
            raise ConfigurationError("Technical validation cannot certify multiple historical observations")

    def require_supported_purpose(self) -> None:
        if self.purpose != "technical-validation" and not self.history_root:
            raise ConfigurationError(
                "Training dataset unavailable without --history-dir: "
                + "; ".join(TRAINING_BLOCKERS)
                + ". Supply certified history with --history-dir, or use --purpose technical-validation."
            )
