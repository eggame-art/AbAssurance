set -Eeuo pipefail

/opt/mssql-tools18/bin/sqlcmd -S sqlserver -U sa -C -b -l 30 \
    -i /init/01-schema.sql
/opt/mssql-tools18/bin/sqlcmd -S sqlserver -U sa -C -b -l 30 \
    -d AssurePlus -i /init/02-seed.sql

echo 'AssurePlus : schema et donnees de demonstration initialises.'
