import argparse
import json
import os
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()

    parser.add_argument("--state-dir", default=os.environ.get("SPARK_STATE_DIR", "data/spark"))
    parser.add_argument("--entity", choices=("customers", "contracts", "claims", "payments", "rejected"))
    parser.add_argument("--limit", type=int, default=20)

    args = parser.parse_args()

    if not 1 <= args.limit <= 1000:
        parser.error("--limit must be between 1 and 1000")

    if (args.state_dir.startswith("hdfs:")):
        from spark_pipeline.hdfs import HdfsState, read_snapshot
        from spark_pipeline.run import create_session

        spark = create_session(kafka=False)

        try:
            manifest = HdfsState(spark, args.state_dir).read_json("CURRENT.json")

            print(json.dumps(manifest, ensure_ascii=False, indent=2))

            if (args.entity):
                frame = read_snapshot(spark, args.state_dir, args.entity, manifest=manifest)

                for row in frame.limit(args.limit).toJSON().toLocalIterator():
                    print(row)

        finally:
            spark.stop()

        return 0

    state = Path(args.state_dir).resolve()
    manifest = json.loads((state / "CURRENT.json").read_text("utf-8"))

    print(json.dumps(manifest, ensure_ascii=False, indent=2))

    if args.entity:
        from spark_pipeline.run import create_session

        spark = create_session(kafka=False)

        try:
            path = state / "snapshots" / manifest["snapshot"] / args.entity

            if any(file.stat().st_size for file in path.glob("part-*.json")):
                frame = spark.read.json(str(path))

                for row in frame.limit(args.limit).toJSON().toLocalIterator():
                    print(row)

        finally:
            spark.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
