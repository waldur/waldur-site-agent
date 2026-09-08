#!/usr/bin/env bash
#
# End-to-end demo: populate an OpenLDAP directory from Waldur, then log into a
# host that resolves its users from that directory through SSSD.
#
#   ./ci/sssd-demo/run-demo.sh          bring everything up and print the proof
#   ./ci/sssd-demo/run-demo.sh --down   tear it all down again
#
# It reuses the E2E stack (Waldur + PostgreSQL + OpenLDAP) and the site agent's
# `account_source: waldur` mode, so what runs here is the shipped code path
# rather than a bespoke script.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

DEMO_DIR="ci/sssd-demo"
COMPOSE="docker compose -f ci/docker-compose.e2e.yml"
CLIENT="waldur-sssd-client"
IMAGE="waldur-sssd-login-demo:latest"
PROJECT_A="e2eb0000000000000000000000000001"
TOKEN="e2e0000000000000000000000000token001"
API="http://localhost:8080/api"
DEMO_PASSWORD='Dem0-Passw0rd!'

export DOCKER_REGISTRY_PREFIX="${DOCKER_REGISTRY_PREFIX:-}"
export WALDUR_MASTERMIND_IMAGE_TAG="${WALDUR_MASTERMIND_IMAGE_TAG:-latest}"
export COMPOSE_PROFILES=ldap

if [ "${1:-}" = "--down" ]; then
  docker rm -f "$CLIENT" >/dev/null 2>&1 || true
  $COMPOSE down
  echo "torn down"
  exit 0
fi

say() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }

say "starting Waldur + OpenLDAP"
$COMPOSE up waldur-db-migration
$COMPOSE up -d

say "waiting for the Waldur API"
for i in $(seq 1 90); do
  code=$(curl -s -o /dev/null -w "%{http_code}" "$API/" 2>/dev/null || echo 000)
  if [ "$code" = "401" ] || [ "$code" = "200" ]; then echo "ready (HTTP $code)"; break; fi
  if [ "$i" -eq 90 ]; then echo "API never came up (last $code)"; $COMPOSE logs waldur-api | tail -40; exit 1; fi
  sleep 5
done

say "waiting for OpenLDAP"
for i in $(seq 1 30); do
  if $COMPOSE exec -T waldur-ldap ldapsearch -x -H ldap://localhost \
       -b "dc=sofiatech,dc=bg" -D "cn=admin,dc=sofiatech,dc=bg" -w e2e-ldap-password >/dev/null 2>&1; then
    echo "ready"; break
  fi
  if [ "$i" -eq 30 ]; then echo "OpenLDAP never came up"; exit 1; fi
  sleep 3
done

say "loading the demo preset into Waldur"
API_CONTAINER=$($COMPOSE ps -q waldur-api)
PRESETS=$($COMPOSE exec -T waldur-api find /usr/src -path "*/demo_presets/presets" -type d | head -1 | tr -d '\r')
docker cp ci/site_agent_e2e.json "${API_CONTAINER}:${PRESETS}/site_agent_e2e.json"
$COMPOSE exec -T waldur-api waldur demo_presets load site_agent_e2e --no-cleanup >/dev/null
$COMPOSE exec -T waldur-api waldur shell -c "
from rest_framework.authtoken.models import Token
from django.contrib.auth import get_user_model
u = get_user_model().objects.get(username='e2e-staff')
Token.objects.filter(user=u).delete()
Token.objects.create(user=u, key='${TOKEN}')
" >/dev/null
echo "preset loaded, API token set"

say "populating the directory from Waldur (account_source: waldur)"
# The committed config addresses the CI-internal host "docker"; from a laptop the
# same services are on localhost.
sed -e 's#http://docker:8080/api/#http://localhost:8080/api/#g' \
    -e 's#ldap://docker:389#ldap://localhost:389#g' \
    ci/e2e-ci-config-ldap-inverted.yaml > "${DEMO_DIR}/.agent-config.local.yaml"
uv run python "${DEMO_DIR}/reconcile.py" "${DEMO_DIR}/.agent-config.local.yaml" "$PROJECT_A"

say "starting an SSSD client on the stack network"
docker build -q -t "$IMAGE" "$DEMO_DIR" >/dev/null
NET=$(docker inspect "$($COMPOSE ps -q waldur-ldap)" \
        --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}')
docker rm -f "$CLIENT" >/dev/null 2>&1 || true
docker run -d --name "$CLIENT" --network "$NET" "$IMAGE" >/dev/null
for i in $(seq 1 20); do
  docker exec "$CLIENT" getent passwd wauser4 >/dev/null 2>&1 && break
  if [ "$i" -eq 20 ]; then echo "SSSD never resolved wauser4"; docker logs "$CLIENT" | tail -20; exit 1; fi
  sleep 2
done
echo "SSSD is resolving directory users"

# Authentication needs a credential in the directory. The agent only writes one
# when `generate_vpn_password` is enabled, and that value is random by design, so
# the demo sets a known password here purely to exercise the PAM path.
$COMPOSE exec -T waldur-ldap ldappasswd -x -H ldap://localhost \
  -D "cn=admin,dc=sofiatech,dc=bg" -w e2e-ldap-password \
  -s "$DEMO_PASSWORD" "uid=wauser4,ou=People,dc=sofiatech,dc=bg" >/dev/null 2>&1

say "PROOF"
echo
echo "1. Waldur is the authority"
curl -s -H "Authorization: token $TOKEN" \
  "$API/marketplace-offering-users/?offering_uuid=e2ef0000000000000000000000000005" |
  python3 -c "import sys,json;[print(f\"   {u['username']:9} uid={u['uidnumber']} gid={u['primarygroup']} home={u['home_directory']} shell={u['login_shell']}\") for u in json.load(sys.stdin)]"

echo
echo "2. The directory, populated by the agent"
$COMPOSE exec -T waldur-ldap ldapsearch -x -LLL -H ldap://localhost \
  -b "ou=People,dc=sofiatech,dc=bg" -D "cn=admin,dc=sofiatech,dc=bg" -w e2e-ldap-password \
  "(uid=wauser*)" uid uidNumber gidNumber homeDirectory loginShell 2>/dev/null | sed 's/^/   /'

echo "3. NSS through SSSD"
echo "   \$ getent passwd wauser4"; docker exec "$CLIENT" getent passwd wauser4 | sed 's/^/   /'
echo "   \$ id wauser4";           docker exec "$CLIENT" id wauser4          | sed 's/^/   /'

echo
echo "4. Login: session, home directory and shell"
docker exec "$CLIENT" su - wauser4 -c \
  'echo "   user=$(id -un) uid=$(id -u) gid=$(id -g) HOME=$HOME SHELL=$SHELL cwd=$(pwd)"' 2>&1 | tail -1

echo
echo "5. PAM authentication through SSSD"
printf '   correct password -> '
docker exec -i "$CLIENT" sh -c "printf '%s\n' '$DEMO_PASSWORD' | sssctl user-checks wauser4 -a auth -s system-auth" 2>&1 |
  grep -o "pam_authenticate for user \[wauser4\]: .*"
printf '   wrong password   -> '
docker exec -i "$CLIENT" sh -c "printf 'WRONG\n' | sssctl user-checks wauser4 -a auth -s system-auth" 2>&1 |
  grep -o "pam_authenticate for user \[wauser4\]: .*"

say "done - tear down with: $0 --down"
