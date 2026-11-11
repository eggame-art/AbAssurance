from mapping.identifiers import make_id
from shared.errors import PipelineError, ensure
from shared.types import Datasets

ID_FIELDS = {"customers": "customer_id", "contracts": "contract_id", "claims": "claim_id", "payments": "payment_id"}


def validate_datasets(datasets: Datasets) -> None:
    for entity, id_field in ID_FIELDS.items():
        identifiers: set[str] = set()
        source_field = "source_reference" if (entity == "contracts") else "source_id"

        for record in datasets[entity]:
            identifier = record[id_field]

            ensure(identifier not in identifiers, f"Duplicate identifier in {entity}: {identifier}", PipelineError)
            ensure(
                identifier == make_id(record["source_system"], record[source_field]),
                f"Inconsistent source identifier in {entity}",
                PipelineError,
            )

            identifiers.add(identifier)

    for child, parent, foreign_key in (
        ("contracts", "customers", "customer_id"),
        ("claims", "contracts", "contract_id"),
        ("payments", "contracts", "contract_id"),
    ):
        parents = {row[ID_FIELDS[parent]]: row for row in datasets[parent]}

        for record in datasets[child]:
            related = parents.get(record[foreign_key])

            ensure(related is not None, f"Orphan {foreign_key} in {child}", PipelineError)
            ensure(
                related["source_system"] == record["source_system"],
                f"Cross-source {foreign_key} in {child}",
                PipelineError,
            )
