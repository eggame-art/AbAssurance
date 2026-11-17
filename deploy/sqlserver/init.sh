set -Eeuo pipefail

if [[ ! ${SQLSERVER_PASSWORD:-} =~ ^Aa1![0-9a-fA-F]{32,96}$ ]]; then
  echo 'SQLSERVER_PASSWORD must be Aa1! followed by 32 to 96 hexadecimal characters.' >&2
  exit 1
fi

bash /init/init.sh
/opt/mssql-tools18/bin/sqlcmd -S sqlserver -U sa -C -b -l 30 -i /security/service-account.sql
bash /security/check-access.sh
