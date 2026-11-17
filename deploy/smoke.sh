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
  -e API_ROOT_PATH=/api \
  -e INTEGRATION_STATE_DIR=/tmp/integrations "$image"
for attempt in {1..30}; do
  if docker exec "$name" python -c "from urllib.request import urlopen; urlopen('http://localhost:8000/auth/status', timeout=2)"; then
    break
  fi
  sleep 2
done
docker run -d --name "$web" --network "$name" --network-alias frontend \
  -e SITE_ADDRESS=http://:80 "abassurance-frontend:${RELEASE_TAG:-local}"
for attempt in {1..30}; do
  if docker exec "$name" python -c "from urllib.request import urlopen; urlopen('http://frontend/api/auth/status', timeout=2)"; then
    break
  fi
  sleep 2
done
docker exec -i "$name" python - <<'PY'
import json
from urllib.error import HTTPError
from http.cookiejar import CookieJar
from urllib.request import Request, urlopen, build_opener, HTTPCookieProcessor
from abassurance.systems.auth.service import create_user

create_user('ci-admin', 'ci-only-test-password', 'admin')
assert b'<html' in urlopen('http://frontend/').read().lower()
assert b'<html' in urlopen('http://frontend/pipeline').read().lower()
assert b'<html' in urlopen('http://frontend/donnees').read().lower()
base = 'http://frontend/api'
for path in ('/docs', '/openapi.json'):
    try:
        urlopen(base + path)
    except HTTPError as exc:
        assert exc.code == 401
    else:
        raise AssertionError('Documentation is accessible without authentication')
try:
    urlopen(base + '/pipeline/services')
except HTTPError as exc:
    assert exc.code == 401
else:
    raise AssertionError('Supervision is accessible without authentication')
request = Request(base + '/auth/login',
    data=json.dumps({'username': 'ci-admin', 'password': 'ci-only-test-password'}).encode(),
    headers={'Content-Type': 'application/json', 'X-Requested-With': 'AbAssurance'})
opener = build_opener(HTTPCookieProcessor(CookieJar()))
with opener.open(request) as response:
    assert json.load(response)['role'] == 'admin'
    assert 'HttpOnly' in response.headers['Set-Cookie']
assert '/api/openapi.json' in opener.open(base + '/docs').read().decode()
assert {'url': '/api'} in json.load(opener.open(base + '/openapi.json'))['servers']
PY
