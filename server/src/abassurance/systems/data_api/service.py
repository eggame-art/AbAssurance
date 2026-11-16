from contextlib import closing
from dataclasses import replace

from abassurance.shared.environment import Environment
from abassurance.shared.errors import ConfigurationError
from abassurance.shared.types import Record
from abassurance.systems.data_api.schemas import (
    ClaimsResponse,
    ClientsResponse,
    ContractsResponse,
    DataKind,
    DataSource,
    PaymentsResponse,
    SourceClaim,
    SourceClient,
    SourceContract,
    SourcePayment,
)
from abassurance.systems.data_integration.extraction.common import read_records
from abassurance.systems.data_integration.extraction.config import (
    PROJECT_ROOT,
    load_oracle_config,
    load_sqlserver_config,
)

READ_TIMEOUT = 5

ORACLE_CLIENTS_QUERY = """
    SELECT
        TO_CHAR(AB_CLIENT_ID, 'TM9') AS "source_id",
        AB_NOM AS "name",
        AB_PRENOM AS "first_name",
        AB_EMAIL AS "email",
        AB_TELEPHONE AS "phone",
        AB_STATUT_CLIENT AS "status"
    FROM AB_CLIENT
    ORDER BY AB_CLIENT_ID
"""

SQLSERVER_CLIENTS_QUERY = """
    SELECT
        CAST(AP_USER_ID AS VARCHAR(20)) AS source_id,
        AP_FULL_NAME AS name,
        CAST(NULL AS NVARCHAR(100)) AS first_name,
        AP_MAIL_ADDRESS AS email,
        AP_PHONE_NUMBER AS phone,
        AP_CUSTOMER_STATUS AS status
    FROM dbo.AP_USERS
    ORDER BY AP_USER_ID
"""

ORACLE_CONTRACTS_QUERY = """
    SELECT
        AB_POLICY_NUMBER AS "source_reference",
        TO_CHAR(AB_CLIENT_ID, 'TM9') AS "source_client_id",
        AB_TYPE_ASSURANCE AS "product_type",
        TO_CHAR(AB_DATE_DEBUT, 'YYYY-MM-DD"T"HH24:MI:SS') AS "start_date",
        TO_CHAR(AB_DATE_FIN, 'YYYY-MM-DD"T"HH24:MI:SS') AS "end_date",
        TO_CHAR(AB_PRIME_ANNUELLE, 'TM9', 'NLS_NUMERIC_CHARACTERS=''.,''') AS "premium",
        'annual' AS "premium_period",
        AB_STATUT_CONTRAT AS "status"
    FROM AB_CONTRAT
    ORDER BY AB_POLICY_NUMBER
"""

SQLSERVER_CONTRACTS_QUERY = """
    SELECT
        AP_CONTRACT_REF AS source_reference,
        CAST(AP_USER_ID AS VARCHAR(20)) AS source_client_id,
        AP_PRODUCT_CODE AS product_type,
        CONVERT(VARCHAR(33), AP_START_DATE, 126) AS start_date,
        CONVERT(VARCHAR(33), AP_END_DATE, 126) AS end_date,
        CAST(AP_MONTHLY_PREMIUM AS VARCHAR(30)) AS premium,
        'monthly' AS premium_period,
        AP_CONTRACT_STATE AS status
    FROM dbo.AP_CONTRACTS
    ORDER BY AP_CONTRACT_REF
"""

ORACLE_CLAIMS_QUERY = """
    SELECT
        TO_CHAR(AB_CLAIM_ID, 'TM9') AS "source_id",
        AB_POLICY_NUMBER AS "source_contract_reference",
        TO_CHAR(AB_DATE_SINISTRE, 'YYYY-MM-DD"T"HH24:MI:SS.FF6') AS "incident_date",
        TO_CHAR(AB_MONTANT_ESTIME, 'TM9', 'NLS_NUMERIC_CHARACTERS=''.,''') AS "estimated_amount",
        AB_STATUT_SINISTRE AS "status",
        AB_DESCRIPTION AS "description"
    FROM AB_SINISTRE
    ORDER BY AB_CLAIM_ID
"""

SQLSERVER_CLAIMS_QUERY = """
    SELECT
        CAST(AP_SINISTRE_NUM AS VARCHAR(20)) AS source_id,
        AP_CONTRACT_REF AS source_contract_reference,
        AP_INCIDENT_DATE AS incident_date,
        CAST(AP_ESTIMATED_AMOUNT AS VARCHAR(30)) AS estimated_amount,
        AP_CLAIM_STATUS AS status,
        AP_CLAIM_COMMENT AS description
    FROM dbo.AP_CLAIMS
    ORDER BY AP_SINISTRE_NUM
"""

ORACLE_PAYMENTS_QUERY = """
    SELECT
        TO_CHAR(AB_PAYMENT_ID, 'TM9') AS "source_id",
        AB_POLICY_NUMBER AS "source_contract_reference",
        TO_CHAR(AB_DATE_PAIEMENT, 'YYYY-MM-DD"T"HH24:MI:SS') AS "payment_date",
        TO_CHAR(AB_MONTANT, 'TM9', 'NLS_NUMERIC_CHARACTERS=''.,''') AS "amount",
        AB_MODE_PAIEMENT AS "method",
        CAST(NULL AS VARCHAR2(20)) AS "status"
    FROM AB_PAIEMENT
    ORDER BY AB_PAYMENT_ID
"""

SQLSERVER_PAYMENTS_QUERY = """
    SELECT
        CAST(AP_PAYMENT_REF AS VARCHAR(36)) AS source_id,
        AP_CONTRACT_REF AS source_contract_reference,
        CONVERT(VARCHAR(33), AP_PAYMENT_DATETIME, 126) AS payment_date,
        CAST(AP_AMOUNT_PAID AS VARCHAR(30)) AS amount,
        AP_PAYMENT_CHANNEL AS method,
        AP_TRANSACTION_STATUS AS status
    FROM dbo.AP_PAYMENTS
    ORDER BY AP_PAYMENT_REF
"""


def _read_source(source: DataSource, oracle_query: str, sqlserver_query: str) -> tuple[DataKind, list[Record]]:
    env = Environment.from_file(PROJECT_ROOT / ".env")
    data_kind = env.get("SOURCE_DATA_KIND", "unspecified")

    if data_kind != "demo" and data_kind != "unspecified":
        raise ConfigurationError("Invalid source data kind")

    oracle_connection = None
    convert = None
    if source == "oracle":
        from abassurance.systems.data_integration.extraction.oracle import connect_oracle, materialize_value

        oracle_connection = connect_oracle(replace(load_oracle_config(env), timeout=READ_TIMEOUT))
        connection = oracle_connection
        query = oracle_query
        convert = materialize_value
    else:
        from abassurance.systems.data_integration.extraction.sqlserver import connect_sqlserver

        connection = connect_sqlserver(config=replace(load_sqlserver_config(env), timeout=READ_TIMEOUT))
        query = sqlserver_query

    with closing(connection):
        if oracle_connection is not None:
            oracle_connection.call_timeout = READ_TIMEOUT * 1000
        with closing(connection.cursor()) as cursor:
            cursor.execute(query)
            records = read_records(cursor, convert)

    return data_kind, records


def get_clients(source: DataSource) -> ClientsResponse:
    data_kind, records = _read_source(source, ORACLE_CLIENTS_QUERY, SQLSERVER_CLIENTS_QUERY)
    clients = [SourceClient.model_validate(record) for record in records]

    return ClientsResponse(source=source, data_kind=data_kind, clients=clients)


def get_contracts(source: DataSource) -> ContractsResponse:
    data_kind, records = _read_source(source, ORACLE_CONTRACTS_QUERY, SQLSERVER_CONTRACTS_QUERY)
    contracts = [SourceContract.model_validate(record) for record in records]

    return ContractsResponse(source=source, data_kind=data_kind, contracts=contracts)


def get_claims(source: DataSource) -> ClaimsResponse:
    data_kind, records = _read_source(source, ORACLE_CLAIMS_QUERY, SQLSERVER_CLAIMS_QUERY)
    claims = [SourceClaim.model_validate(record) for record in records]

    return ClaimsResponse(source=source, data_kind=data_kind, claims=claims)


def get_payments(source: DataSource) -> PaymentsResponse:
    data_kind, records = _read_source(source, ORACLE_PAYMENTS_QUERY, SQLSERVER_PAYMENTS_QUERY)
    payments = [SourcePayment.model_validate(record) for record in records]

    return PaymentsResponse(source=source, data_kind=data_kind, payments=payments)
