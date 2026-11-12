import argparse
import logging
from pathlib import Path

from extraction.config import Settings, load_settings
from extraction.oracle import fetch_oracle
from extraction.sqlserver import fetch_sqlserver
from mapping import map_claim, map_contract, map_customer, map_payment
from pipeline.export import export_datasets
from pipeline.transform import map_records
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
    datasets: Datasets = {}

    for entity, mapper in MAPPERS.items():
        oracle_records = fetch_oracle(entity, settings.oracle)
        sqlserver_records = fetch_sqlserver(entity, settings.sqlserver)

        datasets[entity] = map_records(oracle_records, "abassurance", entity, mapper) + map_records(
            sqlserver_records, "assureplus", entity, mapper
        )

        LOGGER.info(
            "%s: %d Oracle + %d SQL Server = %d unified",
            entity,
            len(oracle_records),
            len(sqlserver_records),
            len(datasets[entity]),
        )

    validate_datasets(datasets)
    export_datasets(datasets, destination)

    LOGGER.info("JSON exports written to %s", destination)

    return datasets


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract, map and export Oracle and SQL Server data.")
    parser.add_argument("--env-file", type=Path, help="Alternative environment file")
    parser.add_argument("--output-dir", type=Path, help="Output directory for the four JSON datasets")

    arguments = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    def execute() -> None:
        run_pipeline(arguments.output_dir, settings=load_settings(arguments.env_file))

    return run_cli(execute, LOGGER, "Pipeline failed")


if __name__ == "__main__":
    raise SystemExit(main())
