from mapping.identifiers import make_id
from shared.errors import PipelineError
from shared.types import Datasets, Record

ID_FIELDS = {"customers": "customer_id", "contracts": "contract_id", "claims": "claim_id", "payments": "payment_id"}
RELATIONSHIPS = {
    "contracts": ("customers", "customer_id"),
    "claims": ("contracts", "contract_id"),
    "payments": ("contracts", "contract_id"),
}


def validate_record(record: Record, entity: str, identifiers: set[str], parents: dict[str, Record]) -> None:
    identifier = record[ID_FIELDS[entity]]
    source_field = "source_reference" if entity == "contracts" else "source_id"

    if identifier in identifiers:
        raise PipelineError(f"Duplicate identifier in {entity}: {identifier}")

    if identifier != make_id(record["source_system"], record[source_field]):
        raise PipelineError(f"Inconsistent source identifier in {entity}")

    if entity == "contracts" and record["start_date"] and record["end_date"]:
        if record["start_date"] > record["end_date"]:
            raise PipelineError("Contract end_date precedes start_date")

    if entity in RELATIONSHIPS:
        _, foreign_key = RELATIONSHIPS[entity]
        related = parents.get(record[foreign_key])
        if related is None:
            raise PipelineError(f"Orphan {foreign_key} in {entity}")

        if related["source_system"] != record["source_system"]:
            raise PipelineError(f"Cross-source {foreign_key} in {entity}")


def validate_datasets(datasets: Datasets) -> None:
    for entity, id_field in ID_FIELDS.items():
        identifiers: set[str] = set()
        parent, _ = RELATIONSHIPS.get(entity, ("", ""))
        parents = {row[ID_FIELDS[parent]]: row for row in datasets.get(parent, [])}

        for record in datasets[entity]:
            validate_record(record, entity, identifiers, parents)
            identifiers.add(record[id_field])
