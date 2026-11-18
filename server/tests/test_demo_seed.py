from copy import deepcopy
from datetime import date
from unittest.mock import Mock

import pytest
from abassurance.systems import demo_seed
from abassurance.systems.data_integration.extraction import oracle, sqlserver
from abassurance.systems.data_integration.pipeline.run import MAPPERS
from abassurance.systems.demo_seed import TABLES, demo_rows, main, write_rows

TEST_DATE = date(2026, 11, 18)


@pytest.mark.parametrize("source", TABLES)
def test_ten_thousand_fictitious_customers_have_valid_unique_related_records(source):
    rows = demo_rows(source, 10000, TEST_DATE)

    assert {entity: len(records) for entity, records in rows.items()} == {
        "clients": 10000,
        "contracts": 10000,
        "claims": 10000,
        "payments": 45000,
    }

    for entity, records in rows.items():
        key = TABLES[source][entity][1]

        assert len({str(record[key]) for record in records}) == len(records)

    customer_key = TABLES[source]["clients"][1]
    contract_key = TABLES[source]["contracts"][1]
    email_field = TABLES[source]["clients"][2]

    customers = {record[customer_key] for record in rows["clients"]}
    contracts = {record[contract_key] for record in rows["contracts"]}

    assert all(record[email_field].endswith("@example.test") for record in rows["clients"])
    assert all(record[customer_key] in customers for record in rows["contracts"])

    for entity in ("payments", "claims"):
        assert all(record[contract_key] in contracts for record in rows[entity])

    assert len({str(record) for record in rows["payments"]}) == 45000

    expected = {}

    for entity, records in rows.items():
        if entity == "clients":
            expected[entity] = records[:3]
            continue

        parent_ids = (
            {record[customer_key] for record in rows["clients"][:3]}
            if entity == "contracts"
            else {record[contract_key] for record in rows["contracts"][:3]}
        )

        guard_field = TABLES[source][entity][2]

        expected[entity] = [record for record in records if record[guard_field] in parent_ids]

    assert demo_rows(source, 3, TEST_DATE) == expected


@pytest.mark.parametrize(("source", "count"), [("invalid", 1), ("oracle", 0), ("sqlserver", 10001)])
def test_generator_rejects_invalid_or_excessive_requests(source, count):
    with pytest.raises(ValueError):
        demo_rows(source, count, date.today())


@pytest.mark.parametrize(("source", "system"), [("oracle", "abassurance"), ("sqlserver", "assureplus")])
def test_generated_source_rows_are_compatible_with_the_integration_mappers(source, system):
    rows = demo_rows(source, 100, TEST_DATE)

    mapped = {
        entity: [MAPPERS["customers" if entity == "clients" else entity](record, system) for record in records]
        for entity, records in rows.items()
    }

    customers = {record["customer_id"] for record in mapped["clients"]}
    contracts = {record["contract_id"] for record in mapped["contracts"]}

    assert all(record["customer_id"] in customers for record in mapped["contracts"])

    for entity in ("claims", "payments"):
        assert all(record["contract_id"] in contracts for record in mapped[entity])


class MemoryCursor:
    def __init__(self, source):
        self.source = source
        self.tables = {values[0]: {} for values in TABLES[source].values()}
        self.statements = []
        self.results = []

    def execute(self, statement):
        self.statements.append(statement)

        if statement.startswith("SELECT"):
            fields, table = statement.split(" FROM ")
            table = table.split(" WHERE ")[0]

            keys = fields.removeprefix("SELECT ").split(", ")

            self.results = [(str(row[keys[0]]).upper(), row[keys[1]]) for row in self.tables[table].values()]

        return self

    def __iter__(self):
        return iter(self.results)

    def executemany(self, statement, values):
        table, columns = statement.removeprefix("INSERT INTO ").split(" (", 1)
        columns = columns.split(")")[0].split(",")

        for value in values:
            row = dict(zip(columns, value, strict=True))
            self.tables[table][str(value[0])] = row

    def close(self):
        pass


@pytest.mark.parametrize("source", TABLES)
def test_repeat_load_skips_existing_rows_even_with_uppercase_sql_guids(source):
    cursor = MemoryCursor(source)
    connection = Mock(cursor=lambda: cursor)

    rows = demo_rows(source, 7, TEST_DATE)
    expected = {entity: len(records) for entity, records in rows.items()}

    assert write_rows(connection, source, rows, batch_size=2) == expected

    before = deepcopy(cursor.tables)

    assert write_rows(connection, source, rows) == dict.fromkeys(rows, 0)
    assert cursor.tables == before
    assert connection.commit.call_count == 2

    connection.rollback.assert_not_called()

    assert not any(statement.startswith(("DELETE", "UPDATE")) for statement in cursor.statements)


@pytest.mark.parametrize("source", TABLES)
def test_collisions_and_database_errors_roll_back_without_commit(source):
    cursor = MemoryCursor(source)
    connection = Mock(cursor=lambda: cursor)

    rows = demo_rows(source, 1, date.today())
    table, key, guard, _ = TABLES[source]["clients"]

    cursor.tables[table]["1000001"] = {key: 1000001, guard: "existing-private@example.test"}

    with pytest.raises(ValueError, match="collide"):
        write_rows(connection, source, rows)

    connection.commit.assert_not_called()
    connection.rollback.assert_called_once()

    cursor.tables[table].clear()
    cursor.executemany = Mock(side_effect=RuntimeError("database failed"))

    with pytest.raises(RuntimeError):
        write_rows(connection, source, rows)

    assert connection.rollback.call_count == 2
    connection.commit.assert_not_called()


def test_default_preview_does_not_connect_or_write(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["demo-seed", "--clients", "10000"])

    assert main() == 0

    output = capsys.readouterr().out

    assert "oracle preview" in output
    assert "sqlserver preview" in output
    assert "45000" in output


def test_writes_require_explicit_demo_classification(monkeypatch):
    monkeypatch.setenv("SOURCE_DATA_KIND", "real")
    monkeypatch.setattr("sys.argv", ["demo-seed", "--confirm-demo"])

    with pytest.raises(SystemExit) as error:
        main()

    assert error.value.code == 2


def test_confirmed_cli_loads_both_sources_and_closes_connections(monkeypatch, capsys):
    monkeypatch.setenv("SOURCE_DATA_KIND", "demo")
    monkeypatch.setattr("sys.argv", ["demo-seed", "--clients", "3", "--confirm-demo"])

    connections = [Mock(cursor=lambda source=source: MemoryCursor(source)) for source in TABLES]

    connect_oracle = Mock(return_value=connections[0])
    connect_sqlserver = Mock(return_value=connections[1])

    monkeypatch.setattr(demo_seed, "load_oracle_config", lambda _: "oracle-config")
    monkeypatch.setattr(demo_seed, "load_sqlserver_config", lambda _: "sql-config")
    monkeypatch.setattr(oracle, "connect_oracle", connect_oracle)
    monkeypatch.setattr(sqlserver, "connect_sqlserver", connect_sqlserver)

    assert main() == 0

    connect_oracle.assert_called_once_with("oracle-config")
    connect_sqlserver.assert_called_once_with(config="sql-config")

    for connection in connections:
        connection.commit.assert_called_once()
        connection.close.assert_called_once()

    assert "sqlserver inserted" in capsys.readouterr().out


def test_cli_failure_after_first_source_can_be_reported_without_private_details(monkeypatch, capsys):
    monkeypatch.setenv("SOURCE_DATA_KIND", "demo")
    monkeypatch.setattr("sys.argv", ["demo-seed", "--clients", "1", "--confirm-demo"])

    connection = Mock(cursor=lambda: MemoryCursor("oracle"))

    monkeypatch.setattr(demo_seed, "load_oracle_config", lambda _: None)
    monkeypatch.setattr(demo_seed, "load_sqlserver_config", lambda _: None)
    monkeypatch.setattr(oracle, "connect_oracle", lambda _: connection)
    monkeypatch.setattr(sqlserver, "connect_sqlserver", Mock(side_effect=RuntimeError("confidential password")))

    assert main() == 1

    connection.commit.assert_called_once()

    output = capsys.readouterr()

    assert "previously completed source may remain loaded" in output.err
    assert "confidential" not in output.err
