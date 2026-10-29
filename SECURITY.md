# Security policy

Cellbox runs untrusted code in sandboxes, so we want to hear about weaknesses in it and treat them seriously.

## Reporting a vulnerability

**Do not open a public issue for a security problem.**

Report it privately through GitHub: open the repository's **Security** tab and choose **Report a vulnerability** ([direct link](https://github.com/jingyugao/cell-box/security/advisories/new)). Include:

- what you found and which component it affects (API server, guest agent, node wrapper, Helm chart, SDK);
- the Cellbox version or commit, the gVisor build (`runsc --version`) and the Kubernetes setup;
- steps or a proof of concept to reproduce it;
- the impact you expect, for example a sandbox escape, access to another Box's data, or a bypass of an identity check.

You will get an acknowledgement and a first assessment from a maintainer. Please give us a reasonable time to fix the issue before you disclose it publicly, and tell us if you are on a deadline.

## Supported versions

Cellbox is alpha (`v1alpha1`). Only the latest release and the `master` branch receive fixes.

## Scope and trust boundaries

These are things to know when you assess a report or deploy Cellbox. The README lists the same points under Limitations.

- **The sandbox boundary is gVisor.** Cellbox adds its own checks (Box ownership, execution identity, tickets), but isolation of the code running inside a Box depends on the gVisor build you install. Cellbox uses a patched build; see [node installation](docs/node-installation.md).
- **The API has no built-in authentication.** `X-Cellbox-Client-ID` selects a data namespace and is not a credential. Anyone who can reach the API can act as any client. Deploy it behind an authenticating gateway and restrict network access to it. Reports that only state that the API is unauthenticated are expected behavior, but a way to cross client namespaces despite a correct gateway is in scope.
- **The server is privileged.** `cellbox-server` runs privileged with `hostPID` and host mounts on the one node it manages. Treat that node as part of the trusted computing base.
- **Guest port 40000.** The guest agent's control API listens on port 40000 inside the sandbox network namespace. It is meant to be reached only by `cellbox-server`. Network policy on the node should keep other workloads away from Pod IPs.

## Out of scope

- Denial of service from a Box using the CPU, memory or disk limits it was given.
- Findings that need access to the node's root account or to the Kubernetes API with administrator rights.
- Vulnerabilities in gVisor itself, Kubernetes or containerd. Report those to the respective projects; tell us as well if Cellbox's configuration makes them worse.
