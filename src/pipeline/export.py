import json
from pathlib import Path
from tempfile import TemporaryDirectory

from shared.errors import PipelineError, error_context
from shared.types import Datasets


def export_datasets(datasets: Datasets, destination: Path) -> None:
    with error_context("Mapped data contains a non-JSON value", (TypeError, ValueError), PipelineError):
        serialized = {
            entity: json.dumps(rows, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
            for entity, rows in datasets.items()
        }

    destination.mkdir(parents=True, exist_ok=True)

    with TemporaryDirectory(prefix=".pipeline-", dir=destination) as temporary:
        staging = Path(temporary)

        for entity, content in serialized.items():
            (staging / f"{entity}.json").write_text(content, encoding="utf-8")

        for entity in datasets:
            filename = f"{entity}.json"

            (staging / filename).replace(destination / filename)
