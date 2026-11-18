set -Eeuo pipefail

source /security/read-password.sh

bash /init/init.sh
/opt/mssql-tools18/bin/sqlcmd -S sqlserver -U sa -C -b -l 30 -i /security/service-account.sql
bash /security/check-access.sh
