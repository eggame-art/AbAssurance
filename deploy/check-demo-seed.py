import os
from contextlib import closing
from datetime import date

from abassurance.shared.environment import Environment
from abassurance.systems.data_integration.extraction.config import load_sqlserver_config
from abassurance.systems.data_integration.extraction.sqlserver import connect_sqlserver
from abassurance.systems.demo_seed import TABLES, demo_rows, write_rows


def main():
    env = Environment(os.environ)

    assert env.get("SOURCE_DATA_KIND") == "demo", "Demo data mode is required."

    rows = demo_rows("sqlserver", 20, date(2026, 11, 18))
    expected = {entity: len(records) for entity, records in rows.items()}

    config = load_sqlserver_config(env)

    with closing(connect_sqlserver(config=config)) as connection:
        inserted = write_rows(connection, "sqlserver", rows)
        assert inserted == expected

        repeated = write_rows(connection, "sqlserver", rows)
        assert repeated == dict.fromkeys(rows, 0)

        with closing(connection.cursor()) as cursor:
            for entity, (table, _, _, condition) in TABLES["sqlserver"].items():
                cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {condition}")

                row = cursor.fetchone()
                assert row is not None, f"Missing count for {entity}"
                count = row[0]

                assert count == expected[entity], entity

    print("SQL Server demo loading passed: related records inserted, repeat load unchanged.")


if __name__ == "__main__":
    main()
