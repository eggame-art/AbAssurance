from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from abassurance.shared.errors import ConfigurationError
from abassurance.systems.data_preparation.config import PreparationConfig

OBSERVATION_KEY = ["contract_id", "source_system", "observation_date"]
CUSTOMER_KEY = ["customer_id", "source_system"]
CONTRACT_KEY = ["contract_id", "source_system"]


def assign_split(data: DataFrame, config: PreparationConfig) -> DataFrame:
    if not config.evaluation_start:
        return data.withColumn(
            "dataset_split", F.when(F.col("training_eligible"), "training").otherwise("excluded")
        ).withColumn(
            "split_reason",
            F.when(F.col("training_eligible"), "single_observation_no_evaluation").otherwise("ineligible_observation"),
        )

    boundary = F.lit(config.evaluation_start).cast("date")
    evaluation = data.filter(F.col("observation_date") >= boundary)

    customers = evaluation.select(*CUSTOMER_KEY).distinct().withColumn("_evaluation_customer", F.lit(True))

    contracts = evaluation.select(*CONTRACT_KEY).distinct().withColumn("_evaluation_contract", F.lit(True))

    joined = data.join(customers, CUSTOMER_KEY, "left").join(contracts, CONTRACT_KEY, "left")

    reason = (
        F.when(~F.col("training_eligible"), "ineligible_observation")
        .when(F.col("observation_date") >= boundary, "evaluation")
        .when(F.date_add("horizon_end", config.embargo_days) > boundary, "purged_label_horizon")
        .when(F.col("_evaluation_customer"), "customer_reserved_for_evaluation")
        .when(F.col("_evaluation_contract"), "contract_reserved_for_evaluation")
        .otherwise("training")
    )

    return (
        joined.withColumn("split_reason", reason)
        .withColumn(
            "dataset_split",
            F.when(F.col("split_reason").isin("training", "evaluation"), F.col("split_reason")).otherwise("excluded"),
        )
        .drop("_evaluation_customer", "_evaluation_contract")
    )


def split_summary(data: DataFrame, config: PreparationConfig) -> dict:
    result = {}

    for name in ("training", "evaluation"):
        part = data.filter(F.col("dataset_split") == name)

        limits_row = part.agg(
            F.count("contract_id").alias("records"),
            F.min("observation_date").alias("first_observation"),
            F.max("observation_date").alias("last_observation"),
            F.max("horizon_end").alias("last_label_end"),
        ).first()

        if limits_row is None:
            raise RuntimeError(f"Dataset split summary is unavailable: {name}")

        limits = limits_row.asDict()

        for field in ("first_observation", "last_observation", "last_label_end"):
            limits[field] = str(limits[field]) if limits[field] else None

        limits["class_counts"] = {
            str(row.is_cancelled): row["count"] for row in part.groupBy("is_cancelled").count().collect()
        }

        limits["customers_per_class"] = {
            str(row.is_cancelled): row["count"]
            for row in part.select(*CUSTOMER_KEY, "is_cancelled").distinct().groupBy("is_cancelled").count().collect()
        }

        limits["customers"] = part.select(*CUSTOMER_KEY).distinct().count()
        limits["contracts"] = part.select(*CONTRACT_KEY).distinct().count()

        result[name] = limits

    result["excluded_reasons"] = {
        row.split_reason: row["count"]
        for row in data.filter("dataset_split = 'excluded'").groupBy("split_reason").count().collect()
    }

    return result


def validate_split(data: DataFrame, config: PreparationConfig, *, require_nonempty: bool = True) -> dict:
    if data.groupBy(*OBSERVATION_KEY).count().filter("count > 1").limit(1).count():
        raise ConfigurationError("Duplicate contract/source/observation key")

    train = data.filter("dataset_split = 'training'")
    evaluation = data.filter("dataset_split = 'evaluation'")

    invalid = (
        ~F.col("dataset_split").isin("training", "evaluation", "excluded")
        | F.col("dataset_split").isNull()
        | (
            F.col("dataset_split").isin("training", "evaluation")
            & (
                ~F.col("training_eligible")
                | F.col("is_cancelled").isNull()
                | ~F.col("is_cancelled").isin(0, 1)
                | F.col("customer_id").isNull()
            )
        )
    )

    if config.evaluation_start:
        boundary = F.lit(config.evaluation_start).cast("date")

        invalid = (
            invalid
            | (
                (F.col("dataset_split") == "training")
                & (
                    (F.col("observation_date") >= boundary)
                    | (F.date_add("horizon_end", config.embargo_days) > boundary)
                )
            )
            | ((F.col("dataset_split") == "evaluation") & (F.col("observation_date") < boundary))
        )

        if train.select(*CUSTOMER_KEY).join(evaluation.select(*CUSTOMER_KEY), CUSTOMER_KEY).limit(1).count():
            raise ConfigurationError("Customer leakage between training and evaluation")

        if train.select(*CONTRACT_KEY).join(evaluation.select(*CONTRACT_KEY), CONTRACT_KEY).limit(1).count():
            raise ConfigurationError("Contract leakage between training and evaluation")

        assigned = assign_split(data.drop("dataset_split", "split_reason"), config)

        if (
            assigned.select(*OBSERVATION_KEY, "dataset_split", "split_reason")
            .exceptAll(data.select(*OBSERVATION_KEY, "dataset_split", "split_reason"))
            .limit(1)
            .count()
        ):
            raise ConfigurationError("Persisted split does not respect the configured policy")

        if require_nonempty and (not train.limit(1).count() or not evaluation.limit(1).count()):
            raise ConfigurationError(
                "Impossible independent split: training or evaluation has no eligible observations"
            )

    if data.filter(F.coalesce(invalid, F.lit(True))).limit(1).count():
        raise ConfigurationError("Invalid temporal split assignment")

    return {
        "temporal_non_overlap": bool(config.evaluation_start),
        "customer_disjoint": bool(config.evaluation_start),
        "contract_disjoint": bool(config.evaluation_start),
        "observation_key_unique": True,
    }


def readiness(config: PreparationConfig, summary: dict, *, historical: bool, synthetic: bool) -> dict:
    blockers = []

    if not historical:
        blockers.append("Uncertified technical snapshot: reliable labels/history unavailable")

    if not config.evaluation_start:
        blockers.append("Single observation: no independent chronological evaluation")

    required = config.required_evaluation_customers_per_class

    for name in ("training", "evaluation"):
        if not summary[name]["records"]:
            blockers.append(f"No eligible {name} observations")

        for target in ("0", "1"):
            count = summary[name]["customers_per_class"].get(target, 0)
            minimum = required if name == "evaluation" else 1

            if count < minimum:
                blockers.append(f"Insufficient {name} customers for class {target}: {count} < {minimum}")

    ready = not blockers

    return {
        "training_ready": ready and not synthetic,
        "synthetic_training_ready": ready and synthetic,
        "training_blockers": blockers + (["Synthetic data cannot establish business performance"] if synthetic else []),
        "evaluation_sample_plan": {
            "method": "Two-sided Hoeffding bound per class; independent customer groups",
            "confidence_per_class": config.evaluation_confidence,
            "absolute_margin": config.evaluation_margin,
            "required_customers_per_class": required,
            "assumptions": "One Bernoulli outcome per independent customer per class; not a model performance estimate",
            "limitations": (
                "Repeated observations are correlated; evaluation must aggregate/resample by customer. "
                "Source identities may represent the same person. Training sample adequacy depends on the model."
            ),
            "sufficient": all(
                summary["evaluation"]["customers_per_class"].get(str(value), 0) >= required for value in (0, 1)
            ),
        },
    }
