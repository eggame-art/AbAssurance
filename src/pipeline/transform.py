from shared.errors import PipelineError, error_context
from shared.types import Mapper, Record


def map_records(records: list[Record], source: str, entity: str, mapper: Mapper) -> list[Record]:
    result: list[Record] = []

    for position, record in enumerate(records, start=1):
        message = f"Mapping failed for {source}/{entity}, row {position}"

        with error_context(message, (KeyError, ValueError, TypeError, ArithmeticError), PipelineError):
            result.append(mapper(record, source))

    return result
