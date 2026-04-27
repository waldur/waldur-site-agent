# SLURM Storage Quotas

The SLURM plugin can apply two independent kinds of filesystem quotas during
normal agent operation:

- **Per-user home directory quota** (`homedir_quota`) — runs in
  `membership_sync` mode when a new user is added to a SLURM account and a
  homedir is created. Sets a *user* quota on the user's home directory via
  CephFS xattrs, XFS user quotas, or Lustre user quotas.
- **Per-project directory + Lustre group/project quota**
  (`project_directory`) — runs in `order_process` mode during resource
  creation (`_pre_create_resource`). Creates a shared project directory,
  applies ownership / permissions / ACLs, and optionally sets a Lustre
  *project* quota keyed on the LDAP group GID.

The two are independent — you can enable either, both, or neither. Failures
in either are logged but do not abort agent operation.

## Subsystem A — Per-user home directory quota

This lives in the **core** module
[`waldur_site_agent/backend/quota.py`](../waldur_site_agent/backend/quota.py)
and is invoked from
[`BaseBackend.create_user_homedirs`](../waldur_site_agent/backend/backends.py)
whenever the agent creates a new user homedir. It is wired into the SLURM
backend through the standard
`enable_user_homedir_account_creation` /
`default_homedir_umask` settings.

The SLURM backend triggers `create_user_homedirs` from three call sites:

- `post_create_resource` — during `order_process`, after a resource is
  created, for the offering users in the user context.
- `add_users_to_resource` — during `membership_sync`, for users newly added
  to a resource.
- `process_existing_users` — during `membership_sync`, to ensure homedirs
  exist for already-known users.

It is also called by the standalone `create_homedirs_for_offering_users`
utility (`waldur_site_agent/common/utils.py`).

### Homedir quota configuration

```yaml
backend_settings:
  enable_user_homedir_account_creation: true
  default_homedir_umask: "0077"

  # Optional override. When set, the agent applies the quota to
  # {homedir_base_path}/{username}. When unset, the path is taken from
  # the system passwd database (pwd.getpwnam(username).pw_dir).
  homedir_base_path: "/cephfs/home"

  homedir_quota:
    provider: "ceph_xattr"   # one of: ceph_xattr | xfs | lustre
    # ...provider-specific fields below
```

The schema is `HomedirQuotaConfig` in `backend/quota.py`. It uses
`extra="forbid"`, so unknown keys are rejected. An invalid configuration is
logged and treated as "no quota" — homedir creation still proceeds.

### Provider: `ceph_xattr`

Sets quotas via extended attributes on the homedir.

```yaml
homedir_quota:
  provider: "ceph_xattr"
  max_bytes: "1099511627776"   # 1 TiB in bytes; string. Optional.
  max_files: 100000             # integer. Optional.
```

Commands executed (per attribute):

```bash
setfattr -n ceph.quota.max_bytes -v 1099511627776 <homedir>
getfattr --only-values -n ceph.quota.max_bytes <homedir>   # verification
```

The verify step compares the read-back value against the configured one and
logs a warning on mismatch.

### Provider: `xfs`

Sets XFS user quotas via `xfs_quota`. Block limits accept human-readable
suffixes (`g`, `t`, …).

```yaml
homedir_quota:
  provider: "xfs"
  mount_point: "/home"          # required
  block_softlimit: "900g"
  block_hardlimit: "1t"
  inode_softlimit: 90000
  inode_hardlimit: 100000
```

Command executed:

```bash
xfs_quota -x -c "limit -u bsoft=900g bhard=1t isoft=90000 ihard=100000 alice" /home
xfs_quota -x -c "quota -u -N -b -h alice" /home   # verification (logged)
```

If `mount_point` is missing or no limits are set, the quota step is
skipped with a log message. The homedir itself is still created.

### Provider: `lustre`

Sets Lustre user quotas via `lfs setquota`. Block limits are expressed in
**kilobytes**.

```yaml
homedir_quota:
  provider: "lustre"
  mount_point: "/home"
  block_softlimit: "943718400"    # ~900 GiB in KiB
  block_hardlimit: "1048576000"   # ~1 TiB in KiB
  inode_softlimit: 90000
  inode_hardlimit: 100000
```

Command executed:

```bash
lfs setquota -u alice -b 943718400 -B 1048576000 -i 90000 -I 100000 /home
lfs quota -u alice /home   # verification (logged)
```

### When the quota is applied

`BaseBackend.create_user_homedirs` iterates over the set of usernames it is
given. For each one:

1. `client.create_linux_user_homedir(username, umask)` is called.
2. If `homedir_quota` is configured, the quota is then applied on the resolved
   homedir path.
3. A failure for one user is logged but does not stop processing of the
   remaining users.
4. If homedir creation itself fails for a user, the quota step is skipped for
   that user.

## Subsystem B — Project directory + Lustre group/project quota

SLURM-plugin specific. Schemas: `ProjectDirectoryConfig` and
`LustreQuotaConfig` in
[`plugins/slurm/waldur_site_agent_slurm/schemas.py`](../plugins/slurm/waldur_site_agent_slurm/schemas.py).
Implementation: `_setup_project_directory` /
`_set_lustre_quota` in
[`plugins/slurm/waldur_site_agent_slurm/backend.py`](../plugins/slurm/waldur_site_agent_slurm/backend.py).

### Project directory configuration

```yaml
backend_settings:
  project_directory:
    enabled: true
    base_path: "/valhalla/projects"   # default
    owner: "nobody"                    # uid/owner for chown
    permissions: "770"                 # octal, applied via chmod
    set_gid: true                      # chmod g+s
    set_acl: true                      # setfacl -R -m group:<g>:rwx,d:group:<g>:rwx

    # Optional Lustre group/project quota (see below for prerequisites)
    lustre_quota:
      mount_point: "/valhalla"
      block_softlimit: 5368709120      # kilobytes
      block_hardlimit: 6442450944
      inode_softlimit: 5000000
      inode_hardlimit: 7000000
```

### What the agent does on resource creation

When `project_directory.enabled: true`, `_pre_create_resource` calls
`_setup_project_directory(resource_backend_id)` after the SLURM account
hierarchy is created. It executes (in order):

```bash
mkdir -p   <base_path>/<resource_backend_id>
chmod 770  <base_path>/<resource_backend_id>
chmod g+s  <base_path>/<resource_backend_id>     # if set_gid
chown <owner>:<group_name> <base_path>/<resource_backend_id>
setfacl -R -m group:<group_name>:rwx,d:group:<group_name>:rwx <path>   # if set_acl
```

`<group_name>` defaults to the resource backend ID. It can be overridden
through two extra keys that are read by the backend but not (yet) part of
the schema fields — they pass through because the schema has
`extra="allow"`:

- `group_owner_source`: when set to `"project_id"` (the default), the group
  name is the SLURM account name. Any other value falls back to:
- `group_name`: explicit override of the group used in `chown` / `setfacl`.

### Lustre group/project quota: prerequisites

The Lustre quota step inside `_setup_project_directory` runs **only if all
three** of the following are true:

1. `lustre_quota` is configured.
2. The SLURM backend has an LDAP client configured (`backend_settings.ldap`).
3. `LdapClient.get_group_gid(group_name)` returns a non-`None` GID.

If any of these is missing the directory is still created but the Lustre
quota is silently skipped. No warning is emitted today — if you intend to
use Lustre project quotas, make sure the offering has `ldap:` configured
and that the project group exists in LDAP before the resource is created.

If the prerequisites are met, the agent runs:

```bash
lfs setquota -p <gid> [-b <block_softlimit>] [-B <block_hardlimit>] \
                      [-i <inode_softlimit>] [-I <inode_hardlimit>] <mount_point>
lfs project -p <gid> -r -s <project_path>
```

Each `-b/-B/-i/-I` flag is included only if the corresponding limit is set
in the configuration. `mount_point` defaults to `/valhalla`.

Note that this is a **project quota** (`-p`), not a group or user quota. The
GID coming from LDAP is reused as the Lustre project ID, and the directory
tree is tagged with that project ID via `lfs project -r -s`.

Unlike subsystem A, no verification step is run. Failures from
`lfs setquota` are logged but do not abort resource creation.

## Examples

A complete reference example showing both subsystems is in
[`examples/waldur-site-agent-config.yaml.example`](../examples/waldur-site-agent-config.yaml.example):

- The `Example SLURM Offering` shows `homedir_quota` placement with the
  `ceph_xattr` provider (commented out).
- The `Discoverer CPU` offering shows `homedir_base_path`, the `lustre` and
  `xfs` `homedir_quota` provider examples (commented out), and a full
  uncommented `project_directory` block with `lustre_quota`.

## Caveats and known inconsistencies

- **`HomedirQuotaConfig.block_softlimit` / `block_hardlimit` are typed as
  `Optional[str]`** (to allow XFS suffixes like `"900g"`), while
  `LustreQuotaConfig.block_softlimit` / `block_hardlimit` are typed as
  `Optional[int]`. Use integers (kilobytes) for `project_directory.lustre_quota`
  even if you use string-with-suffix for `homedir_quota` on Lustre.
- **`group_owner_source` and `group_name`** are accepted by the runtime but
  are not declared fields of `ProjectDirectoryConfig` (they pass through via
  `extra="allow"`). They are documented here for completeness.
- **Lustre project quota silently no-ops without LDAP.** This is a current
  implementation choice — the quota requires the GID and the GID lookup goes
  through the LDAP client.
- **No verification** is run after `lfs setquota -p` (the homedir-quota path
  *does* verify). Operators should spot-check with `lfs quota -p <gid>
  <mount>` after onboarding the first project.

## Operator troubleshooting

Verify a user's home quota:

```bash
# Ceph
getfattr --only-values -n ceph.quota.max_bytes /cephfs/home/alice
# XFS
xfs_quota -x -c 'quota -u -N -b -h alice' /home
# Lustre
lfs quota -u alice /home
```

Verify a project directory's Lustre quota / project ID:

```bash
lfs quota   -p <gid>     /valhalla
lfs project -d /valhalla/projects/<resource_backend_id>
```

The agent logs each command it issues and each verification result. Search
the structured logs for `homedir`, `Lustre quota`, or
`Created project directory` to trace activity.
