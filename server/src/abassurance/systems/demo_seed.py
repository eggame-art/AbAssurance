import argparse
import sys
from contextlib import closing
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from uuid import NAMESPACE_URL, uuid5

from abassurance.shared.environment import Environment
from abassurance.systems.data_integration.extraction.config import (
    PROJECT_ROOT,
    load_oracle_config,
    load_sqlserver_config,
)

PREFIX = {"oracle": "AB", "sqlserver": "AP"}

TABLES = {
    "oracle": {
        "clients": ("AB_CLIENT", "AB_CLIENT_ID", "AB_EMAIL", "AB_CLIENT_ID BETWEEN 1000001 AND 1010000"),
        "contracts": ("AB_CONTRAT", "AB_POLICY_NUMBER", "AB_CLIENT_ID", "AB_POLICY_NUMBER LIKE 'DEMO10K-AB-%'"),
        "claims": ("AB_SINISTRE", "AB_CLAIM_ID", "AB_POLICY_NUMBER", "AB_POLICY_NUMBER LIKE 'DEMO10K-AB-%'"),
        "payments": ("AB_PAIEMENT", "AB_PAYMENT_ID", "AB_POLICY_NUMBER", "AB_POLICY_NUMBER LIKE 'DEMO10K-AB-%'"),
    },
    "sqlserver": {
        "clients": ("dbo.AP_USERS", "AP_USER_ID", "AP_MAIL_ADDRESS", "AP_USER_ID BETWEEN 1000001 AND 1010000"),
        "contracts": ("dbo.AP_CONTRACTS", "AP_CONTRACT_REF", "AP_USER_ID", "AP_CONTRACT_REF LIKE 'DEMO10K-AP-%'"),
        "claims": ("dbo.AP_CLAIMS", "AP_SINISTRE_NUM", "AP_CONTRACT_REF", "AP_CONTRACT_REF LIKE 'DEMO10K-AP-%'"),
        "payments": ("dbo.AP_PAYMENTS", "AP_PAYMENT_REF", "AP_CONTRACT_REF", "AP_CONTRACT_REF LIKE 'DEMO10K-AP-%'"),
    },
}


def demo_rows(source: str, clients: int, as_of: date):
    if source not in TABLES or not 1 <= clients <= 10000:
        raise ValueError("Choose oracle/sqlserver and 1 to 10000 clients")

    rows = {entity: [] for entity in TABLES[source]}
    is_oracle = source == "oracle"

    for index in range(1, clients + 1):
        identifier = 1_000_000 + index
        contract = f"DEMO10K-{PREFIX[source]}-{index:05d}"
        email = f"demo-{PREFIX[source].lower()}-{index:05d}@example.test"

        start = as_of - timedelta(days=365 + index % 900)
        monthly = Decimal(70 + index % 111)
        birth_date = date(1980 + index % 25, 1 + index % 12, 1 + index % 28)
        product = ("AUTO", "HABITATION", "SANTE")[index % 3]

        if is_oracle:
            rows["clients"].append(
                {
                    "AB_CLIENT_ID": identifier,
                    "AB_NOM": f"FICTIF AB {index:05d}",
                    "AB_PRENOM": "Demo",
                    "AB_DATE_NAISSANCE": birth_date,
                    "AB_EMAIL": email,
                    "AB_DATE_CREATION": datetime.combine(start, time()),
                    "AB_STATUT_CLIENT": "ACTIVE",
                }
            )

            rows["contracts"].append(
                {
                    "AB_POLICY_NUMBER": contract,
                    "AB_CLIENT_ID": identifier,
                    "AB_TYPE_ASSURANCE": product,
                    "AB_DATE_DEBUT": start,
                    "AB_PRIME_ANNUELLE": monthly * 12,
                    "AB_STATUT_CONTRAT": "ACTIVE",
                    "AB_AGENCE_ID": 1 + index % 10,
                }
            )
        else:
            rows["clients"].append(
                {
                    "AP_USER_ID": identifier,
                    "AP_FULL_NAME": f"FICTIF AP {index:05d}",
                    "AP_BIRTH_DATE": birth_date.isoformat(),
                    "AP_MAIL_ADDRESS": email,
                    "AP_CREATED_AT": datetime.combine(start, time()),
                    "AP_CUSTOMER_STATUS": "ACTIVE",
                    "AP_LOYALTY_SCORE": index % 101,
                }
            )

            rows["contracts"].append(
                {
                    "AP_CONTRACT_REF": contract,
                    "AP_USER_ID": identifier,
                    "AP_PRODUCT_CODE": product,
                    "AP_START_DATE": datetime.combine(start, time()),
                    "AP_MONTHLY_PREMIUM": monthly,
                    "AP_CONTRACT_STATE": "ACTIVE",
                    "AP_BROKER_CODE": f"DEMO-{index % 10}",
                }
            )

        for claim_index in range(index % 3):
            incident_date = datetime.combine(
                as_of - timedelta(days=15 + (index * 7 + claim_index * 31) % 170), time(12)
            )

            if is_oracle:
                rows["claims"].append(
                    {
                        "AB_CLAIM_ID": 20_000_000 + index * 10 + claim_index,
                        "AB_POLICY_NUMBER": contract,
                        "AB_DATE_SINISTRE": incident_date,
                        "AB_MONTANT_ESTIME": Decimal(100 + index % 3000),
                        "AB_STATUT_SINISTRE": "CLOSED",
                        "AB_DESCRIPTION": "Sinistre fictif DEMO10K",
                    }
                )
            else:
                rows["claims"].append(
                    {
                        "AP_SINISTRE_NUM": 20_000_000 + index * 10 + claim_index,
                        "AP_CONTRACT_REF": contract,
                        "AP_INCIDENT_DATE": incident_date.strftime("%Y-%m-%d %H:%M:%S"),
                        "AP_ESTIMATED_AMOUNT": Decimal(100 + index % 3000),
                        "AP_CLAIM_STATUS": "CLOSED",
                        "AP_CLAIM_COMMENT": "Sinistre fictif DEMO10K",
                        "AP_FRAUD_SCORE": Decimal(index % 20),
                    }
                )

        for payment_index in range(2 + index % 6):
            payment_date = as_of - timedelta(days=5 + (index * 17) % 105 + payment_index * 28)

            if is_oracle:
                rows["payments"].append(
                    {
                        "AB_PAYMENT_ID": 10_000_000 + index * 10 + payment_index,
                        "AB_POLICY_NUMBER": contract,
                        "AB_DATE_PAIEMENT": payment_date,
                        "AB_MONTANT": monthly,
                        "AB_MODE_PAIEMENT": "CARD",
                    }
                )
            else:
                payment_id = uuid5(NAMESPACE_URL, f"abassurance-demo10k:{index}:{payment_index}")

                rows["payments"].append(
                    {
                        "AP_PAYMENT_REF": str(payment_id),
                        "AP_CONTRACT_REF": contract,
                        "AP_PAYMENT_DATETIME": datetime.combine(payment_date, time(12)),
                        "AP_AMOUNT_PAID": monthly,
                        "AP_PAYMENT_CHANNEL": "CARD",
                        "AP_TRANSACTION_STATUS": "COMPLETED",
                    }
                )

    return rows


def write_rows(connection, source, rows, batch_size=500):
    inserted = {}

    try:
        with closing(connection.cursor()) as cursor:
            if source == "sqlserver":
                cursor.execute("SET XACT_ABORT ON")
                cursor.execute("BEGIN TRANSACTION")
                cursor.execute(
                    "DECLARE @r int; EXEC @r=sp_getapplock @Resource='abassurance-demo10k', "
                    "@LockMode='Exclusive', @LockOwner='Transaction', @LockTimeout=0; "
                    "IF @r < 0 THROW 51000, 'Another demo load is running.', 1;"
                )
            else:
                cursor.execute("LOCK TABLE AB_CLIENT, AB_CONTRAT, AB_SINISTRE, AB_PAIEMENT IN EXCLUSIVE MODE NOWAIT")

            for entity, records in rows.items():
                table, key, guard, condition = TABLES[source][entity]

                cursor.execute(f"SELECT {key}, {guard} FROM {table} WHERE {condition}")

                existing = {str(row[0]).casefold(): str(row[1]) for row in cursor}

                columns = list(records[0]) if records else []
                pending = []

                for record in records:
                    previous = existing.get(str(record[key]).casefold())

                    if previous is not None:
                        if previous != str(record[guard]):
                            raise ValueError(
                                "Reserved demo identifiers collide with existing data; no overwrite allowed"
                            )

                        continue

                    pending.append(tuple(record[column] for column in columns))

                if pending:
                    placeholders = ",".join(
                        f":{index + 1}" if source == "oracle" else "?" for index in range(len(columns))
                    )

                    statement = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders})"

                    for offset in range(0, len(pending), batch_size):
                        cursor.executemany(statement, pending[offset : offset + batch_size])

                inserted[entity] = len(pending)

        connection.commit()

    except BaseException:
        connection.rollback()
        raise

    return inserted


def main():
    parser = argparse.ArgumentParser(
        description="Add fictitious clients and related records; never delete existing data"
    )

    parser.add_argument("--clients", type=int, default=10000)
    parser.add_argument("--source", choices=("both", "oracle", "sqlserver"), default="both")
    parser.add_argument("--as-of", type=date.fromisoformat, default=datetime.now(UTC).date())
    parser.add_argument("--confirm-demo", action="store_true")

    args = parser.parse_args()

    sources = ("oracle", "sqlserver") if args.source == "both" else (args.source,)
    env = Environment.from_file(PROJECT_ROOT / ".env")

    if args.confirm_demo and env.get("SOURCE_DATA_KIND") != "demo":
        parser.error("Loading is allowed only with SOURCE_DATA_KIND=demo")

    try:
        for source in sources:
            rows = demo_rows(source, args.clients, args.as_of)

            if not args.confirm_demo:
                counts = {entity: len(records) for entity, records in rows.items()}

                print(source, "preview:", counts)
                continue

            if source == "oracle":
                from abassurance.systems.data_integration.extraction.oracle import connect_oracle

                connection = connect_oracle(load_oracle_config(env))
            else:
                from abassurance.systems.data_integration.extraction.sqlserver import connect_sqlserver

                connection = connect_sqlserver(config=load_sqlserver_config(env))

            with closing(connection):
                inserted = write_rows(connection, source, rows)

            print(source, "inserted:", inserted)

        return 0

    except Exception:
        print(
            "Demo load failed. Current source rolled back; a previously completed source may remain loaded.",
            file=sys.stderr,
        )

        return 1


if __name__ == "__main__":
    raise SystemExit(main())
