#!/usr/bin/env bash
# Idempotent end-to-end setup for the rancher-kc-crd plugin against
# the local mastermind dev stack.
#
# Creates / verifies:
#   - 4 OfferingRoles on the existing "Adamas HPC Cluster" offering:
#       Resource scope:         cluster_owner, clustercatalogs_view
#       ResourceProject scope:  ingress_manage, configmaps_manage
#   - 6 user×role assignments across 2 users (test-rancher-00,
#     test-rancher-01) at both scopes, with deliberate overlap on
#     cluster_owner and ingress_manage to exercise the operator's
#     multi-user-per-binding path
#
# Pre-requisites:
#   - dev stack running (./scripts/start-dev-stack.sh)
#   - existing demo data: HPC Demo Org customer, Adamas HPC Cluster
#     offering with backend_id pointing at a real Rancher cluster,
#     6 ResourceProjects already created
#
# Usage:
#   ./e2e-localdev-setup.sh
#   # then run the agent:
#   cd ../../../   # back to waldur-site-agent root
#   uv run waldur_site_agent --mode membership_sync \
#       --config-file plugins/rancher-kc-crd/examples/e2e-localdev-config.yaml

set -euo pipefail

API="http://localhost:10780/api"
TOKEN="${WALDUR_TOKEN:-}"
if [ -z "$TOKEN" ]; then
  TOKEN=$(curl -sk -X POST "$API-auth/password/" \
    -H 'Content-Type: application/json' \
    -d '{"username":"staff","password":"demo"}' \
    | python3 -c "import json,sys; print(json.load(sys.stdin)['token'])")
  echo "Acquired staff token via password auth: ${TOKEN:0:20}..."
fi
H_AUTH="Authorization: Token $TOKEN"

OFFERING_UUID="feeb119a7df24e67b413f171e8968fa5"
RESOURCE_UUID="8598e0db5823407ea6e03d69d2ad68e9"
RP_ILJATEST_UUID="da3ca6aa9bfc45ebbe3b53b4712e0fce"
USER_00_UUID="c24b20a3dc614e0bb30925aea6983e9b"   # test-rancher-00
USER_01_UUID="731df737538a41ba8293360eb2167907"   # test-rancher-01

# ------------------------------------------------------------------
# 1. Roles (idempotent: POST returns 400 if exists; we look up first)
# ------------------------------------------------------------------

ensure_role() {
  local name="$1" content_type="$2"
  local existing
  existing=$(curl -sk -H "$H_AUTH" "$API/marketplace-offering-roles/?offering_uuid=$OFFERING_UUID" \
    | python3 -c "
import json,sys
for r in json.load(sys.stdin):
  if r['name']=='$name' and r['content_type']=='$content_type':
    print(r['uuid']); break")
  if [ -n "$existing" ]; then
    echo "  ✓ role '$name' ($content_type) already exists: $existing"
    return 0
  fi
  local created
  created=$(curl -sk -X POST -H "$H_AUTH" -H 'Content-Type: application/json' \
    -d "{\"name\":\"$name\",\"content_type_input\":\"$content_type\",\"offering\":\"$OFFERING_UUID\",\"is_active\":true}" \
    "$API/marketplace-offering-roles/" \
    | python3 -c "
import json,sys
r=json.load(sys.stdin)
print(r.get('uuid','ERROR: '+str(r)))")
  echo "  ✓ created role '$name' ($content_type): $created"
}

echo "[1/2] Ensuring offering roles..."
ensure_role cluster_owner          resource
ensure_role clustercatalogs_view   resource
ensure_role ingress_manage         resource_project
ensure_role configmaps_manage      resource_project

# Re-fetch role UUIDs (in case they were just created)
role_uuid() {
  local name="$1" content_type="$2"
  curl -sk -H "$H_AUTH" "$API/marketplace-offering-roles/?offering_uuid=$OFFERING_UUID" \
    | python3 -c "
import json,sys
for r in json.load(sys.stdin):
  if r['name']=='$name' and r['content_type']=='$content_type':
    print(r['uuid']); break"
}

ROLE_CLUSTER_OWNER=$(role_uuid cluster_owner resource)
ROLE_CLUSTERCATALOGS_VIEW=$(role_uuid clustercatalogs_view resource)
ROLE_INGRESS_MANAGE=$(role_uuid ingress_manage resource_project)
ROLE_CONFIGMAPS_MANAGE=$(role_uuid configmaps_manage resource_project)

# ------------------------------------------------------------------
# 2. User × role assignments (idempotent: server returns "already has role" on dup)
# ------------------------------------------------------------------

assign_resource() {
  local user="$1" role="$2" label="$3"
  local resp
  resp=$(curl -sk -X POST -H "$H_AUTH" -H 'Content-Type: application/json' \
    -d "{\"user\":\"$user\",\"role\":\"$role\"}" \
    "$API/marketplace-provider-resources/$RESOURCE_UUID/add_user/")
  if echo "$resp" | grep -q "already"; then
    echo "  = $label (already assigned)"
  else
    echo "  ✓ $label"
  fi
}
assign_rp() {
  local rp="$1" user="$2" role="$3" label="$4"
  local resp
  resp=$(curl -sk -X POST -H "$H_AUTH" -H 'Content-Type: application/json' \
    -d "{\"user\":\"$user\",\"role\":\"$role\"}" \
    "$API/marketplace-provider-resource-projects/$rp/add_user/")
  if echo "$resp" | grep -q "already"; then
    echo "  = $label (already assigned)"
  else
    echo "  ✓ $label"
  fi
}

echo
echo "[2/2] Assigning users to roles..."
echo "  Resource scope (= cluster-wide CRTBs):"
assign_resource $USER_00_UUID $ROLE_CLUSTER_OWNER         "test-rancher-00 -> cluster_owner"
assign_resource $USER_00_UUID $ROLE_CLUSTERCATALOGS_VIEW  "test-rancher-00 -> clustercatalogs_view"
assign_resource $USER_01_UUID $ROLE_CLUSTER_OWNER         "test-rancher-01 -> cluster_owner (overlaps with -00)"

echo "  ResourceProject scope on iljatest (= per-project PRTBs):"
assign_rp $RP_ILJATEST_UUID $USER_00_UUID $ROLE_INGRESS_MANAGE     "test-rancher-00 -> ingress_manage"
assign_rp $RP_ILJATEST_UUID $USER_00_UUID $ROLE_CONFIGMAPS_MANAGE  "test-rancher-00 -> configmaps_manage"
assign_rp $RP_ILJATEST_UUID $USER_01_UUID $ROLE_INGRESS_MANAGE     "test-rancher-01 -> ingress_manage (overlaps with -00)"

# ------------------------------------------------------------------
# 3. Verify
# ------------------------------------------------------------------

echo
echo "Final state:"
echo
echo "Resource-scope users:"
curl -sk -H "$H_AUTH" "$API/marketplace-provider-resources/$RESOURCE_UUID/list_users/" \
  | python3 -c "
import json,sys
for u in json.load(sys.stdin):
  print(f\"  {u['user_username']:25} {u['role_name']!r}\")"

echo
echo "iljatest RP users:"
curl -sk -H "$H_AUTH" "$API/marketplace-provider-resource-projects/$RP_ILJATEST_UUID/list_users/" \
  | python3 -c "
import json,sys
for u in json.load(sys.stdin):
  print(f\"  {u['user_username']:25} {u['role_name']!r}\")"

echo
echo "✓ Setup complete. Run the agent:"
echo "  cd $(dirname "$0")/../../../"
echo "  uv run waldur_site_agent --mode membership_sync \\"
echo "      --config-file plugins/rancher-kc-crd/examples/e2e-localdev-config.yaml"
