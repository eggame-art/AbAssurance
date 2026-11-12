import argparse
import logging
from pathlib import Path

from cleaning import clean_datasets
from extraction.config import Settings, load_settings
from extraction.oracle import fetch_oracle
from extraction.sqlserver import fetch_sqlserver
from mapping import map_claim, map_contract, map_customer, map_payment
from pipeline.export import export_datasets
from pipeline.validation import validate_datasets
from shared.errors import run_cli
from shared.types import Datasets, Mapper

LOGGER = logging.getLogger(__name__)

MAPPERS: dict[str, Mapper] = {
    "customers": map_customer,
    "contracts": map_contract,
    "claims": map_claim,
    "payments": map_payment,
}


def run_pipeline(output_dir: Path | None = None, *, settings: Settings | None = None) -> Datasets:
    settings = settings if settings is not None else load_settings()
    destination = Path(output_dir) if output_dir is not None else settings.output_dir
    sources: dict[str, Datasets] = {"abassurance": {}, "assureplus": {}}

    for entity in MAPPERS:
        sources["abassurance"][entity] = fetch_oracle(entity, settings.oracle)
        sources["assureplus"][entity] = fetch_sqlserver(entity, settings.sqlserver)

    datasets, rejected = clean_datasets(sources, MAPPERS)
    for entity, rows in datasets.items():
        LOGGER.info(
            "%s: %d Oracle + %d SQL Server = %d unified",
            entity,
            len(sources["abassurance"][entity]),
            len(sources["assureplus"][entity]),
            len(rows),
        )

    validate_datasets(datasets)
    report = [{key: value for key, value in row.items() if key != "record"} for row in rejected]
    export_datasets({**datasets, "rejected": rejected, "cleaning_report": report}, destination)

    LOGGER.info("Cleaning rejected %d records; see cleaning_report.json", len(rejected))
    LOGGER.info("JSON exports written to %s", destination)

    return datasets


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract, clean and export Oracle and SQL Server data.")
    parser.add_argument("--env-file", type=Path, help="Alternative environment file")
    parser.add_argument("--output-dir", type=Path, help="Output directory for JSON datasets and cleaning reports")

    arguments = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    def execute() -> None:
        run_pipeline(arguments.output_dir, settings=load_settings(arguments.env_file))

    return run_cli(execute, LOGGER, "Pipeline failed")


if __name__ == "__main__":
    raise SystemExit(main())
