import argparse
from pathlib import Path

from abassurance.shared.storage import write_json_atomic
from abassurance.systems.messaging.config import load_kafka_config
from abassurance.systems.spark_pipeline.hdfs import read_snapshot
from abassurance.systems.spark_pipeline.run import create_session, run_available
from abassurance.systems.spark_pipeline.schemas import entity_schema


def verify_snapshot(spark, state: str, manifest: dict) -> None:
    for entity, expected in manifest["counts"]["accepted"].items():
        frame = read_snapshot(spark, state, entity, manifest=manifest)
        if frame.drop("_kafka").schema.simpleString() != entity_schema(entity).simpleString():
            raise ValueError(f"Persisted schema differs for {entity}")
        actual = frame.count()

        if actual != expected:
            raise ValueError(f"Persisted snapshot count differs for {entity}")

    rejected = read_snapshot(spark, state, "rejected", manifest=manifest).count()

    if rejected != manifest["counts"]["rejected"]:
        raise ValueError("Persisted rejection count differs")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("state")
    parser.add_argument("result", type=Path)

    args = parser.parse_args()
    spark = create_session()

    try:
        spark.sparkContext.setLogLevel("WARN")

        manifest = run_available(spark, load_kafka_config(), args.state)

        verify_snapshot(spark, args.state, manifest)
        write_json_atomic(args.result, manifest)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
