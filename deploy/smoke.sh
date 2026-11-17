#!/usr/bin/env bash
set -euo pipefail
image="abassurance-backend:${RELEASE_TAG:-local}"
name="abassurance-smoke-${RANDOM}"
web="$name-web"
cleanup() {
  docker rm -f "$web" "$name" >/dev/null 2>&1 || true
  docker network rm "$name" >/dev/null 2>&1 || true
}
trap cleanup EXIT
docker network create "$name"
docker run -d --name "$name" --network "$name" --network-alias backend \
  -e AUTH_DB_PATH=/tmp/auth.sqlite \
  -e INTEGRATION_STATE_DIR=/tmp/integrations "$image"
for attempt in {1..30}; do
  if docker exec "$name" python -c "from urllib.request import urlopen; urlopen('http://localhost:8000/openapi.json', timeout=2)"; then
    break
  fi
  sleep 2
done
docker run -d --name "$web" --network "$name" --network-alias frontend \
  -e SITE_ADDRESS=http://:80 "abassurance-frontend:${RELEASE_TAG:-local}"
for attempt in {1..30}; do
  if docker exec "$name" python -c "from urllib.request import urlopen; urlopen('http://frontend/api/openapi.json', timeout=2)"; then
    break
  fi
  sleep 2
done
docker exec -i "$name" python - <<'PY'
import json
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from abassurance.systems.auth.service import create_user

create_user('ci-admin', 'ci-only-test-password', 'admin')
assert b'<html' in urlopen('http://frontend/').read().lower()
base = 'http://frontend/api'
assert urlopen(base + '/openapi.json').status == 200
try:
    urlopen(base + '/pipeline/services')
except HTTPError as exc:
    assert exc.code == 401
else:
    raise AssertionError('Supervision is accessible without authentication')
request = Request(base + '/auth/login',
    data=json.dumps({'username': 'ci-admin', 'password': 'ci-only-test-password'}).encode(),
    headers={'Content-Type': 'application/json', 'X-Requested-With': 'AbAssurance'})
with urlopen(request) as response:
    assert json.load(response)['role'] == 'admin'
    assert 'HttpOnly' in response.headers['Set-Cookie']
PY
