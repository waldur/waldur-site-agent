# Manual Testing Guide

A step-by-step walkthrough for setting up and testing the Rancher +
Keycloak membership sync (this plugin, paired with the
[`rancher-keycloak-operator`][operator-github]) entirely through the
Waldur web UI, without needing a fully working provisioning backend.

This is aimed at whoever needs to validate the integration end to end —
support engineers, QA, or anyone setting up a demo — not at developers
of the plugin itself. See `README.md` for configuration reference and
architecture details.

[operator-github]: https://github.com/waldur/rancher-keycloak-operator

## What you'll need before starting

- The `rancher-keycloak-operator` already installed and running against
  your target Rancher and Keycloak instances (see the operator's own
  setup docs).
- A Rancher cluster you can point the test at. It needs to be genuinely
  **Active** in Rancher — an imported cluster (pointing at a real,
  reachable Kubernetes API server) is the easiest way to get that
  without provisioning real infrastructure. A "Custom" node-driven
  cluster will sit in "Updating" forever unless you actually attach a
  node to it, so avoid that path for a quick test.
- Staff/provider access in Waldur to create an offering.
- The people you want to test with already having accounts in
  Keycloak (see the username note in Step 4 — this trips people up).

## Step 1 — Create the offering

1. Create a new provider offering of type **"Waldur Site Agent"**.
2. On the offering's **Edit -> Integration -> Operations -> Resource lifecycle** tab, turn on **"Enable resource
   projects"**. This unlocks the sub-project feature this plugin syncs
   membership for.
3. On the **Edit -> Integration -> Resource display options** tab, turn on **"Enable display of
   order actions for service provider"**. This gives you a **"Mark as
   done"** button on orders later, so you don't need a live
   provisioning backend to move an order past "Executing".
4. Go to the **Roles** tab and add at least one role:
   - One role with scope **"Resource project"** (e.g. `project_member`) —
     this is required before you can add anyone to a resource project.
     If you skip this, adding a member to a resource project will tell
     you no roles are set up yet.
   - One role with scope **"Resource"** (e.g. `project_admin`) if you
     want to test resource-wide access.

   **Name these roles exactly like the keys in `role_map` in the
   site-agent config for this offering** (Step 5) — e.g. `project_member`,
   `project_admin`. A role that exists in Waldur but has no matching
   `role_map` key is skipped entirely: members with that role get no
   Keycloak group and no Rancher binding at all, with only a log
   warning to explain why.

## Step 2 — Get a resource into place

1. Place an order for the offering as a regular user, then approve it
   as provider, same as any other order.
2. The order will sit in **"Executing"** — normal, since there's no
   live backend to complete it. Open the order's action menu and click
   **"Mark as done"**. The resource moves straight to **OK**.
3. On the resource, set its **backend ID** to the ID of the Rancher
   cluster you're testing against (the `c-xxxxx` value from Rancher).
   This is what tells the plugin which cluster to manage projects in.
4. If you place a limit/quota-change order on this resource later,
   it'll get stuck in **"Updating"** the same way — same fix, find the
   pending order and mark it done.

## Step 3 — Create resource projects and add members

1. On the resource, create one or more **resource projects** (these
   map to Rancher projects inside the cluster).
2. Add a member to a resource project using the resource-project-scope
   role you created in Step 1.

   **Important:** the member must also have a role in the parent
   Waldur **project's team** — being added to the resource project
   alone isn't enough. Add them to the project first (any role), then
   add them to the resource project.

## Step 4 — Keycloak username: the one thing to get right

The plugin matches people to Keycloak by their actual Waldur account
**username** — the same one they log in with — not any
offering-specific display name or generated identifier. If you're used
to other agent-backed offerings that generate a separate per-offering
account name for each user, this is different: that generated name is
never sent to Keycloak.

Before syncing, make sure everyone you added in Step 3 already has an
account in Keycloak under their real Waldur username. If they don't,
they'll be silently skipped — the resource project, Rancher project,
and Keycloak group all get created and look healthy, but that
particular person just won't show up as a group member. They'll pick
up membership automatically the next time the sync runs after their
Keycloak account exists.

## Step 5 — Run the sync

Configure a site-agent offering entry pointing at this offering's
UUID, using this plugin for membership sync only (leave provisioning
and reporting backends unset — this plugin doesn't do either). A
minimal config looks like this:

```yaml
offerings:
  - name: "<a name for this offering entry>"
    waldur_offering_uuid: "<offering-uuid>"
    waldur_api_url: "<https://your-waldur-instance/>"
    waldur_api_token: "<a Waldur API token, not a session token>"
    backend_type: "rancher-kc-crd"
    membership_sync_backend: "rancher-kc-crd"
    backend_settings:
      waldur_api_url: "<same as waldur_api_url above>"
      waldur_api_token: "<same as waldur_api_token above>"

      kubeconfig_path: "<path to a local kubeconfig for the operator's cluster>"
      namespace: "waldur-system"
      context: "<kubeconfig context to use>"
      role_map: { project_member: "project-member", project_admin: "project-owner" }
```

`role_map` values are the Rancher role template IDs the roles bind
to — keys are the Waldur role names from Step 1. See
`examples/rancher-kc-crd-config.yaml` for the full reference example
with every available setting.

The `waldur_api_url`/`waldur_api_token` duplication under
`backend_settings` is intentional, not a copy-paste mistake — leave
both copies in. This plugin reads ResourceProjects and UserRoles
straight from Waldur itself, which most other backends never need to
do, so it keeps its own separate credentials for that.

Run the agent against that config from the `waldur-site-agent` repo
root:

```bash
uv run waldur_site_agent --mode membership_sync --config-file <path-to-your-config>.yaml
```

(Drop `uv run` if you're running from an already-activated
virtualenv.) It keeps running in the foreground and re-syncs on an
interval, so leave it running while you test and stop it with
`Ctrl+C` when you're done. Each pass picks up your resource projects
and members and pushes them through to the operator.

## Step 6 — Verify

- In your Kubernetes cluster, list the sync objects the plugin creates
  in the operator's namespace and check their status — a healthy one
  reports ready on every condition (project created, groups created,
  bindings created, members synced).
- In Rancher, confirm the project exists and has the expected role
  bindings.
- In Keycloak, confirm the group exists under the expected parent
  group, and that your test users are members.
- **End-to-end check:** log in to Rancher as one of the added members
  (via Keycloak SSO) and confirm they can see the cluster and the
  resource project you created for them. This is the real proof the
  whole chain worked — CR, Keycloak group, and Rancher binding all
  have to be correct for the project to actually show up.
- If a user is missing, re-check Step 4 first — it's the most common
  cause by far.

## Quick troubleshooting

| Symptom | Likely cause |
|---|---|
| "No roles have been set up for project members yet" | Missing resource-project-scope role — see Step 1. |
| Order stuck in "Executing" | No live backend — use "Mark as done" (Step 2, needs the Step 1 toggle). |
| One user missing from the Keycloak group, rest looks synced | Username mismatch — see Step 4. |
| Rancher cluster stuck in "Updating" | Likely a "Custom" cluster with no node attached — import instead. |
