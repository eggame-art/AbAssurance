#!/usr/bin/env bash
set -Eeuo pipefail
: "${SQLSERVER_PASSWORD:?Service password is required}"
SQLCMDPASSWORD="$SQLSERVER_PASSWORD" /opt/mssql-tools18/bin/sqlcmd \
  -S sqlserver -U abassurance_reader -d AssurePlus -C -b -l 30 -i /security/check-access.sql
