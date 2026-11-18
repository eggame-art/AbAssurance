#!/usr/bin/env bash
set -Eeuo pipefail

sqlcmd="${1:-/opt/mssql-tools18/bin/sqlcmd}"
query="IF DB_ID(N'AssurePlus') IS NOT NULL EXEC(N'USE [AssurePlus]; SELECT 1;'); ELSE SELECT 1;"

for ((attempt = 1; attempt <= 60; attempt++)); do
    if "$sqlcmd" -S sqlserver -U sa -C -b -d master -l 5 -t 5 -Q "$query" > /dev/null 2>&1; then
        echo 'SQL Server is ready; existing AssurePlus database is accessible.'
        exit 0
    fi

    if ((attempt < 60)); then
        echo "Waiting for SQL Server and AssurePlus ($attempt/60)..." >&2
        sleep 2
    fi
done

echo 'SQL Server readiness timed out; check SQL Server logs and database recovery status.' >&2
exit 1
