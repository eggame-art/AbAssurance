import re

from pyspark.ml import PipelineModel
from pyspark.sql import DataFrame, SparkSession

from model_training.config import VERSION
from model_training.evaluation import prediction_values
from shared.errors import ConfigurationError
from spark_pipeline.hdfs import HdfsState


def publish_model(spark: SparkSession, root: str, model, metadata: dict, evaluation: DataFrame) -> dict:
    output = HdfsState(spark, root)
    model_id = metadata["model_id"]

    if not re.fullmatch(r"[0-9a-f]{32}", model_id):
        raise ConfigurationError("Invalid model identity")

    attempt, destination = f"attempts/{model_id}", f"runs/{model_id}"

    with output.lock():
        output.mkdir(attempt)
        model.write().save(output.path(f"{attempt}/pipeline"))

        restored = PipelineModel.load(output.path(f"{attempt}/pipeline"))

        expected = prediction_values(model.transform(evaluation))
        actual = prediction_values(restored.transform(evaluation))

        if expected.exceptAll(actual).limit(1).count() or actual.exceptAll(expected).limit(1).count():
            raise ConfigurationError("Reloaded model predictions differ")

        report = {**metadata, "pipeline_path": output.path(f"{destination}/pipeline"), "reload_verified": True}

        output.write_json_atomic(f"{attempt}/report.json", report)

        if output.read_json(f"{attempt}/report.json") != report:
            raise ConfigurationError("Model metadata round-trip failed")

        output.mkdir("runs")

        rename = output.jvm.org.apache.hadoop.fs.Options.Rename
        options = output.gateway.new_array(rename, 1)
        options[0] = rename.NONE

        output.context.rename(output.java_path(attempt), output.java_path(destination), options)

        output.write_json_atomic("CURRENT.json", report)

        return report


def load_model(spark: SparkSession, root: str, model_id: str | None = None, *, allow_experimental: bool = False):
    output = HdfsState(spark, root)

    if model_id is None:
        model_id = output.read_json("CURRENT.json").get("model_id")

    if not isinstance(model_id, str) or not re.fullmatch(r"[0-9a-f]{32}", model_id):
        raise ConfigurationError("Invalid committed model identity")

    folder = f"runs/{model_id}"
    report = output.read_json(f"{folder}/report.json")

    if (
        report.get("version") != VERSION
        or report.get("model_id") != model_id
        or report.get("reload_verified") is not True
        or report.get("pipeline_path") != output.path(f"{folder}/pipeline")
        or report.get("status") not in ("experimental", "data_validated")
    ):
        raise ConfigurationError("Invalid committed model metadata")

    if report["status"] == "experimental" and not allow_experimental:
        raise ConfigurationError("Experimental model cannot be used without explicit opt-in")

    return PipelineModel.load(report["pipeline_path"]), report
