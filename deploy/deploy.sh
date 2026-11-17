#!/usr/bin/env bash
set -euo pipefail
release="${1:?Expected commit SHA}"
[[ "$release" =~ ^[0-9a-f]{40}$ ]] || exit 1
root=/opt/abassurance
cd "$root/releases/$release"
test -f "$root/.env" || { echo 'Create /opt/abassurance/.env first' >&2; exit 1; }
if grep -qE 'CHANGE_ME|assurance\.example\.com' "$root/.env"; then
  echo 'Replace example credentials and domain in /opt/abassurance/.env first.' >&2
  exit 1
fi
ln -sfn "$root/.env" .env
exec 9>"$root/deploy.lock"
flock -n 9 || { echo 'Another deployment is running' >&2; exit 1; }
docker load -i images.tar.gz
export RELEASE_TAG="$release"
docker compose -f compose.production.yaml config --quiet
previous="$(readlink -f "$root/current" || true)"
if docker compose -f compose.production.yaml up -d --no-build --wait --wait-timeout 600; then
  ln -sfn "$PWD" "$root/current"
  echo "Deployment completed: $release"
  rm -f images.tar.gz source.tar.gz
else
  echo 'Deployment failed; attempting to restore the previous application images.' >&2
  if [[ -n "$previous" && -d "$previous" && "$previous" != "$PWD" ]]; then
    cd "$previous"
    export RELEASE_TAG="$(basename "$previous")"
    docker compose -f compose.production.yaml up -d --no-build --wait --wait-timeout 600
  fi
  exit 1
fi
