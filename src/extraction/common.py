from collections.abc import Callable
from contextlib import closing
from typing import Any

from shared.errors import ExtractionError
from shared.types import Record, TableMap, ValueConverter


def read_records(cursor: Any, convert: ValueConverter | None = None) -> list[Record]:
    if cursor.description is None:
        raise ExtractionError("Cursor does not contain a result set")

    columns = [column[0] for column in cursor.description]
    records: list[Record] = []
    for row in cursor:
        values = [convert(value) for value in row] if convert is not None else row
        records.append(dict(zip(columns, values, strict=True)))
    return records


def extract_records(
    entity: str,
    tables: TableMap,
    connector: Callable[[], Any],
    *,
    convert: ValueConverter | None = None,
    output_type_handler: Callable[..., Any] | None = None,
) -> list[Record]:
    if entity not in tables:
        raise ValueError(f"Unknown entity: {entity}")

    table, primary_key = tables[entity]
    with closing(connector()) as connection, closing(connection.cursor()) as cursor:
        if output_type_handler is not None:
            cursor.outputtypehandler = output_type_handler
        cursor.execute(f"SELECT * FROM {table} ORDER BY {primary_key}")
        return read_records(cursor, convert)
