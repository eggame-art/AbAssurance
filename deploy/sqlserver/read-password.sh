# Sourced only by the administrative provisioning container.
if [[ -n ${SQLSERVER_PASSWORD_FILE:-} ]]; then
  SQLSERVER_PASSWORD="$(cat -- "$SQLSERVER_PASSWORD_FILE")"
fi
if [[ ! ${SQLSERVER_PASSWORD:-} =~ ^Aa1![0-9a-fA-F]{32,96}$ ]]; then
  echo 'SQLSERVER_PASSWORD must be Aa1! followed by 32 to 96 hexadecimal characters.' >&2
  exit 1
fi
export SQLSERVER_PASSWORD
