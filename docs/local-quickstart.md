# Local quick start (no Kubernetes)

Run Cellbox on one Linux machine with the `local-runsc` provider: a single `cellbox-server` service starts gVisor sandboxes directly, with no cluster and no containerd. Use it to try Cellbox, for development, or on a small single host. For the Kubernetes deployment, with the warm pool and the fast resume path, see [node installation](node-installation.md) and [deployment](deployment.md).

## What you get

The same REST API and Go SDK as the Kubernetes deployment, backed by local gVisor sandboxes:

- create Boxes from templates and revisions, run commands, read and write files, proxy HTTP to ports in a Box;
- suspend a Box to a checkpoint on this machine and resume it with process memory intact;
- persistent HOME volumes.

What it does not have: the warm pool (a resume restores a checkpoint without a prepared slot, so the roughly 200 ms figure in [Resume on Kubernetes](resume-on-kubernetes.md) does not apply), multiple nodes, and archives unless you also configure object storage. Resume timing on this provider has not been measured.

## Requirements

- Linux x86_64 (Ubuntu or Debian with systemd for the installer), and root.
- Outbound access to the registry that holds your base image. If `runsc` is not installed, the installer also downloads gVisor from `storage.googleapis.com`.
- The installer installs these packages with `apt-get` if they are missing: `skopeo`, `umoci`, `iproute2`, `iptables`, `curl`, `jq`, `bzip2`, `procps`.
- No `/dev/kvm` is needed; the default platform is `systrap`. Use `--platform kvm` only on a host with `/dev/kvm`.
- This provider uses upstream gVisor. The Cellbox gVisor build described in [node installation](node-installation.md) is only needed for the Kubernetes warm path.

The sandbox network uses addresses from `10.231.0.0/16`. Avoid a host network that overlaps it.

## Install

From a source checkout, the installer builds a bundle, copies it to the target host over SSH and installs it there:

```sh
bash scripts/install-local-runsc.sh --ssh my-host
```

To install on the machine you are logged in to, build the bundle, unpack it, and run the installer as root:

```sh
make local-package
tar -xzf dist/cellbox-local-runsc-linux-amd64.tar.gz -C /tmp
sudo bash /tmp/cellbox-local-runsc/install-local-runsc.sh
```

Options:

| Option | Meaning |
| --- | --- |
| `--image REPOSITORY@sha256:DIGEST` | Base image for the first profile. It must be an immutable digest reference and reachable from the host. Used only when no configuration exists yet |
| `--platform systrap\|kvm` | gVisor platform, default `systrap` |
| `--port PORT` | Loopback port of the API, default `8090`. Used only for a new configuration |
| `--skip-smoke` | Skip the smoke test at the end |

The default image in the sample configuration is a public Alpine image from Docker Hub. If your host cannot reach Docker Hub, pass an image from a registry it can reach.

### What the installer does

1. Checks the bundle's checksums and the host (Linux x86_64, root, systemd).
2. Installs missing packages and, if `runsc` is absent, downloads the latest upstream gVisor into `/opt/cellbox-local/runtime`.
3. Creates `/etc/cellbox-local/config.json` (mode `0600`) from [`config/local-runsc.sample.json`](../config/local-runsc.sample.json). An existing file is kept.
4. Installs the binaries into `/opt/cellbox-local/releases/<hash>/` and points `/opt/cellbox-local/current` at them. Data lives in `/var/lib/cellbox-local`.
5. Enables IPv4 forwarding (`/etc/sysctl.d/90-cellbox-local.conf`) and writes and starts `cellbox-local.service`.
6. Runs a smoke test unless skipped: it builds the first revision if needed, creates a Box, runs a command, suspends and resumes it, checks that state survived, and cleans up.

The API listens on `127.0.0.1` only, and the installer requires a loopback address.

A successful run ends like this:

```text
uid=11000(cellbox-agent) gid=11000(cellbox-agent)
cellbox-gvisor-ok
Checkpoint/resume: restored tmpfs token, generation 2

Installed: cellbox-local.service
API: http://127.0.0.1:8090
```

The `--ssh` form of this flow was run, as an upgrade of an existing installation, on a 2-core, 1.6 GB Ubuntu 24.04 host that was also running other services, without KVM. The form that installs on the local machine runs the same script on the unpacked bundle but was not run separately.

## Use it

The API has no authentication and listens on loopback. To reach it from your own machine, forward the port:

```sh
ssh -N -L 8090:127.0.0.1:8090 my-host
export CELLBOX_API_URL=http://127.0.0.1:8090
```

Requests use the client namespace from the configuration (`clientId`, `local` in the sample). Send it as `X-Cellbox-Client-ID: local`. Then follow the [REST quick start](rest-quickstart.md): create a template, build a revision with `profileId` set to `local-runsc` and `baseImage` set to the profile's image, create a Box, run a command, then suspend and resume it. Unless you used `--skip-smoke`, the installer already created a template named `local-runsc` with a stable revision, so you can create a Box from it right away:

```sh
curl -sS -X POST "$CELLBOX_API_URL/v1/boxes" \
  -H 'Content-Type: application/json' -H 'X-Cellbox-Client-ID: local' \
  -d '{"templateId":"<id of the local-runsc template>","ownerKey":"try-1"}'
```

Check the service with `systemctl status cellbox-local` and `journalctl -u cellbox-local`.

## Upgrade and roll back

Run the installer again with the new version. It keeps the configuration and the data, adds a new release directory, restarts the service and repeats the smoke test. The API is unavailable while the service restarts. I did not check how sandboxes that are running at that moment behave across a restart (the host used for the test had none), so list the Boxes first and suspend or finish them if the host is in use.

To roll back, point `current` at an older release and restart:

```sh
ln -sfn /opt/cellbox-local/releases/<previous-hash> /opt/cellbox-local/current
systemctl restart cellbox-local
```

Before upgrading, copy `/etc/cellbox-local` and the unit file `/etc/systemd/system/cellbox-local.service`.

## Remove

There is no uninstall script. The installer created: the service `cellbox-local.service`, `/opt/cellbox-local`, `/etc/cellbox-local`, `/var/lib/cellbox-local` (all Box data and volumes) and `/etc/sysctl.d/90-cellbox-local.conf`. Stop and disable the service before deleting them, and copy anything you need out of `/var/lib/cellbox-local` first.
