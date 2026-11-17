
SET NOCOUNT ON;

USE master;

IF EXISTS (
    SELECT 1
    FROM sys.server_role_members
    WHERE member_principal_id = SUSER_ID(N'abassurance_reader')
)
BEGIN
    THROW 51000, 'Service login must not belong to a server role.', 1;
END;

IF EXISTS (
    SELECT 1
    FROM sys.server_permissions
    WHERE grantee_principal_id = SUSER_ID(N'abassurance_reader')
        AND state IN ('G', 'W')
        AND permission_name <> 'CONNECT SQL'
)
BEGIN
    THROW 51000, 'Unexpected server permissions on service login.', 1;
END;

IF EXISTS (
    SELECT 1
    FROM sys.databases
    WHERE owner_sid = SUSER_SID(N'abassurance_reader')
)
BEGIN
    THROW 51000, 'Service login must not own a database.', 1;
END;

IF SUSER_ID(N'abassurance_reader') IS NULL
BEGIN
    CREATE LOGIN [abassurance_reader]
    WITH PASSWORD = N'$(SQLSERVER_PASSWORD)',
        DEFAULT_DATABASE = [AssurePlus],
        CHECK_POLICY = ON,
        CHECK_EXPIRATION = OFF;
END;
ELSE
BEGIN
    ALTER LOGIN [abassurance_reader]
    WITH PASSWORD = N'$(SQLSERVER_PASSWORD)',
        DEFAULT_DATABASE = [AssurePlus],
        CHECK_POLICY = ON,
        CHECK_EXPIRATION = OFF;
END;

DENY VIEW ANY DATABASE TO [abassurance_reader];

GO

USE AssurePlus;

SET XACT_ABORT ON;

BEGIN TRANSACTION;

IF USER_ID(N'abassurance_reader') IS NULL
BEGIN
    CREATE USER [abassurance_reader]
    FOR LOGIN [abassurance_reader];
END;

IF NOT EXISTS (
    SELECT 1
    FROM sys.database_principals
    WHERE name = N'abassurance_reader'
        AND sid = SUSER_SID(N'abassurance_reader')
        AND type = 'S'
)
BEGIN
    THROW 51000, 'Service database user does not match its login.', 1;
END;

IF EXISTS (
    SELECT 1
    FROM sys.database_role_members
    WHERE member_principal_id = USER_ID(N'abassurance_reader')
)
BEGIN
    THROW 51000, 'Service user must not belong to a database role.', 1;
END;

IF EXISTS (
    SELECT 1
    FROM sys.schemas
    WHERE principal_id = USER_ID(N'abassurance_reader')
)
BEGIN
    THROW 51000, 'Service user must not own a schema.', 1;
END;

IF EXISTS (
    SELECT 1
    FROM sys.database_permissions
    WHERE grantee_principal_id = USER_ID(N'abassurance_reader')
        AND state IN ('G', 'W')
        AND NOT (
            permission_name = 'CONNECT'
            AND class = 0
        )
        AND NOT (
            permission_name = 'SELECT'
            AND class = 1
            AND minor_id = 0
            AND major_id IN (
                OBJECT_ID(N'dbo.AP_USERS'),
                OBJECT_ID(N'dbo.AP_CONTRACTS'),
                OBJECT_ID(N'dbo.AP_CLAIMS'),
                OBJECT_ID(N'dbo.AP_PAYMENTS')
            )
        )
)
BEGIN
    THROW 51000, 'Unexpected database permissions on service user.', 1;
END;

GRANT CONNECT TO [abassurance_reader];

GRANT SELECT ON OBJECT::dbo.AP_USERS TO [abassurance_reader];
GRANT SELECT ON OBJECT::dbo.AP_CONTRACTS TO [abassurance_reader];
GRANT SELECT ON OBJECT::dbo.AP_CLAIMS TO [abassurance_reader];
GRANT SELECT ON OBJECT::dbo.AP_PAYMENTS TO [abassurance_reader];

DENY INSERT, UPDATE, DELETE, ALTER
ON SCHEMA::dbo TO [abassurance_reader];

DENY CREATE TABLE, CREATE VIEW, CREATE PROCEDURE, CREATE FUNCTION
TO [abassurance_reader];

COMMIT TRANSACTION;

GO
