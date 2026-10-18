"""Live acceptance tests. Mutations use REST; kubectl supplies runtime evidence.

Only the explicitly selected server deployment may be restarted. No diagnostic
Pods are created. Requests with side effects are submitted once.
"""

import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import shlex
import socket
import subprocess
import sys
import tarfile
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
import uuid


class Suite:
    def __init__(self, args):
        self.args = args
        self.run_id = "e2e-" + uuid.uuid4().hex[:12]
        self.client = self.run_id
        self.other = self.run_id + "-other"
        self.evidence = Path(args.evidence_dir or "tmp/validation/" + self.run_id)
        self.evidence.mkdir(parents=True, exist_ok=True)
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        self.forward = None
        self.forward_log = None
        self.base = ""
        self.boxes = []
        self.archives = []
        self.revisions = []
        self.template = None
        self.started = time.monotonic()
        self.report = {
            "runId": self.run_id, "clientId": self.client, "status": "running",
            "context": args.context, "namespace": args.namespace,
            "sourceImage": args.source_image, "checks": [], "timings": {},
            "skipped": [],
            "resources": {"boxes": [], "archives": [], "revisions": self.revisions},
        }

    def save(self, name, value):
        (self.evidence / (name + ".json")).write_text(json.dumps(value, indent=2) + "\n")

    def check(self, condition, message):
        if not condition:
            raise AssertionError(message)

    def passed(self, name, **details):
        entry = {"name": name, "elapsedSeconds": round(time.monotonic() - self.started, 3), **details}
        self.report["checks"].append(entry)
        self.save("report", self.report)
        print("PASS: " + name, flush=True)

    def k(self, *args, json_output=True):
        command = ["kubectl", "--context", self.args.context, "-n", self.args.namespace, *args]
        if json_output:
            command += ["-o", "json"]
        result = subprocess.run(command, capture_output=True, text=True, timeout=300)
        if result.returncode:
            raise RuntimeError("kubectl " + " ".join(args) + ": " + result.stderr[-4000:])
        return json.loads(result.stdout) if json_output else result.stdout

    def stop_forward(self):
        if self.forward is not None:
            self.forward.terminate()
            try:
                self.forward.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.forward.kill()
                self.forward.wait(timeout=5)
            self.forward = None
        if self.forward_log is not None:
            self.forward_log.close()
            self.forward_log = None

    def start_forward(self):
        self.stop_forward()
        if not self.base:
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", 0))
                port = sock.getsockname()[1]
            self.base = "http://127.0.0.1:" + str(port)
        port = urllib.parse.urlsplit(self.base).port
        self.forward_log = (self.evidence / "port-forward.log").open("ab")
        self.forward = subprocess.Popen([
            "kubectl", "--context", self.args.context, "-n", self.args.namespace,
            "port-forward", "--address", "127.0.0.1", "svc/cellbox-api", f"{port}:8090",
        ], stdout=self.forward_log, stderr=self.forward_log)

    def request(self, method, path, data=None, *, status=200, code=None, client=None, headers=None, timeout=35, raw=False):
        request_headers = {"X-Cellbox-Client-ID": client or self.client, **(headers or {})}
        token = os.environ.get("CELLBOX_CLIENT_TOKEN")
        if token:
            request_headers["Authorization"] = "Bearer " + token
        if isinstance(data, bytes):
            request_headers["Content-Type"] = "application/octet-stream"
        elif data is not None:
            request_headers["Content-Type"] = "application/json"
            data = json.dumps(data).encode()
        request = urllib.request.Request(self.base + path, data=data, method=method, headers=request_headers)
        try:
            response = self.opener.open(request, timeout=timeout)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            payload = response.read()
            content_type = response.headers.get("Content-Type", "").lower()
            value = json.loads(payload) if not raw and payload and "application/json" in content_type else payload
            self.check(response.code == status, f"{method} {path}: HTTP {response.code}, expected {status}: {value!r}")
            if code:
                self.check(isinstance(value, dict) and value.get("error", {}).get("code") == code,
                           f"{method} {path}: expected error {code}: {value!r}")
            return value

    def eventually(self, name, read, accept, seconds=180):
        deadline = time.monotonic() + seconds
        last = None
        while time.monotonic() < deadline:
            try:
                last = read()
                if accept(last):
                    return last
            except (OSError, RuntimeError, AssertionError) as error:
                last = str(error)
            time.sleep(0.5)
        raise AssertionError(f"Timed out waiting for {name}: {last!r}")

    def wait(self, op, expected="succeeded"):
        started = time.monotonic()
        deadline = started + 900
        while op["status"] in ("queued", "running", "cancelling"):
            self.check(time.monotonic() < deadline, "Operation deadline: " + json.dumps(op))
            op = self.request("GET", "/v1/operations/" + op["id"] + "?waitMs=1000")
        self.save("operation-" + op["id"], op)
        if expected is not None:
            self.check(op["status"] == expected, "Unexpected operation outcome: " + json.dumps(op))
        return op

    def submit(self, path, data=None, **kwargs):
        return self.request("POST", path, data, status=202, **kwargs)

    def box(self, box_id):
        return self.request("GET", "/v1/boxes/" + box_id)

    def create(self, *, revision=None, template=None, archive=None, accept_image_change=False, headers=None):
        data = {"ownerKey": self.run_id}
        if revision:
            data["revisionId"] = revision
        if template:
            data["templateId"] = template
        path = "/v1/boxes"
        if archive:
            path += ":restore"
            data["archiveId"] = archive
            data["acceptImageChange"] = accept_image_change
        op = self.submit(path, data, headers=headers)
        self.boxes.append(op["targetId"])
        self.report["resources"]["boxes"].append(op["targetId"])
        self.save("report", self.report)
        self.wait(op)
        return self.box(op["targetId"]), op

    def action(self, box_id, action, generation=None):
        data = {"expectedGeneration": generation} if generation else None
        return self.wait(self.submit(f"/v1/boxes/{box_id}:{action}", data))

    def execute(self, box_id, command, *, generation=None, headers=None):
        generation = generation or self.box(box_id)["generation"]
        op = self.wait(self.submit(f"/v1/boxes/{box_id}/execs", {
            "argv": ["/bin/sh", "-ec", command], "expectedGeneration": generation, "timeoutMs": 20000,
        }, headers=headers))
        execution = self.request("GET", "/v1/execs/" + op["result"]["execId"])
        self.check(execution["state"] == "exited" and execution["result"]["exitCode"] == 0,
                   "Command failed: " + json.dumps(execution))
        return execution, op

    def file(self, box_id, name, data=None, **kwargs):
        path = f"/v1/boxes/{box_id}/files?" + urllib.parse.urlencode({"path": name})
        return self.request("GET" if data is None else "PUT", path, data, status=200 if data is None else 204, raw=True, **kwargs)

    def app(self, box_id, endpoint):
        return self.request("GET", f"/v1/boxes/{box_id}/services/8000/{endpoint}", timeout=10)

    def ready(self, box_id):
        return self.eventually("counter startup", lambda: self.app(box_id, "state"), lambda x: isinstance(x, dict))

    def runtime(self, box_id):
        rows = self.k("get", "cellboxes", "-l", "cellbox.local/box-id=" + box_id)["items"]
        self.check(len(rows) == 1, "Expected one sandbox CR: " + box_id)
        cr = rows[0]
        result = {"cr": cr, "pod": None, "service": self.k("get", "service", cr["metadata"]["name"])}
        if cr.get("status", {}).get("podName"):
            result["pod"] = self.k("get", "pod", cr["status"]["podName"])
        return result

    def warm(self, revision, ready=True):
        def accept(value):
            pool = value.get("pool")
            if value.get("stale") or value.get("syncStatus") != "applied":
                return False
            return bool(pool and pool["readySlots"] >= 1) if ready else not pool
        value = self.eventually("warm pool ready" if ready else "warm demand withdrawal", lambda: self.request(
            "GET", f"/v1/template-revisions/{revision}/warm-pool"), accept, 240)
        self.save("warm-" + revision + ("-ready" if ready else "-disabled"), value)
        return value

    def patch_revision(self, revision, **changes):
        current = self.request("GET", "/v1/template-revisions/" + revision)
        return self.request("PATCH", "/v1/template-revisions/" + revision,
                            {"expectedGeneration": current["generation"], **changes})

    def stable(self, revision):
        current = self.request("GET", "/v1/templates/" + self.template["id"])
        self.template = self.request("PATCH", "/v1/templates/" + current["id"], {
            "expectedGeneration": current["generation"], "stableRevisionId": revision,
        })

    def build(self, tool_version):
        script = '#!/bin/sh\nprintf "%s\\n" "' + tool_version + '"\n'
        command = "printf '%s' " + shlex.quote(script) + " > /usr/local/bin/cellbox-e2e-tool && chmod 755 /usr/local/bin/cellbox-e2e-tool"
        run = ("docker run --env " + shlex.quote("COUNTER_MARKER_PATH=" + self.workspace + "/marker")
               + " --env COUNTER_MEMORY_MIB=32 --workdir " + shlex.quote(self.workspace)
               + " " + shlex.quote(self.args.source_image) + " /server")
        op = self.wait(self.submit(f"/v1/templates/{self.template['id']}/revisions", {
            "userVersion": "1.01", "profileId": self.args.profile_id, "baseImage": self.args.source_image,
            "runCommand": run, "buildCommand": command,
        }))
        revision = self.request("GET", "/v1/template-revisions/" + op["result"]["revisionId"])
        self.revisions.append(revision["id"])
        self.check("@sha256:" in revision["image"] and "@sha256:" in revision["resolvedBaseImage"], "Images must be pinned")
        self.check(revision["warmSlots"] == 1, "Revision did not inherit template warm slots")
        logs = self.request("GET", f"/v1/operations/{op['id']}/logs?offset=0&limit=65536")
        self.check(logs["eof"] and "Published image:" in logs["text"], "Missing completed build logs")
        self.save("revision-" + revision["id"], revision)
        self.save("build-logs-" + op["id"], logs)
        return revision

    def same_state(self, before, after):
        for key in ("count", "started", "instance", "memory_bytes", "memory_sha256", "marker", "pid"):
            self.check(before[key] == after[key], f"Checkpoint did not preserve {key}: {before[key]!r} != {after[key]!r}")

    def restart_server(self):
        self.check(self.args.restart_server, "--restart-server is required for the restart acceptance check")
        # Refuse to restart a server that has any sandbox not owned by this run.
        rows = self.k("get", "cellboxes")["items"]
        owned = set(self.boxes)
        foreign = [r["metadata"]["name"] for r in rows if r["metadata"].get("labels", {}).get("cellbox.local/box-id") not in owned]
        self.check(not foreign, "Server restart would affect preexisting sandboxes: " + str(foreign))
        before = self.k("get", "pods", "-l", "app=cellbox-server")
        self.save("server-before-restart", before)
        self.k("rollout", "restart", "deployment/" + self.args.deployment, json_output=False)
        self.k("rollout", "status", "deployment/" + self.args.deployment, "--timeout=240s", json_output=False)
        self.start_forward()
        self.eventually("HTTP after server restart", lambda: self.request("GET", "/healthz"), lambda x: x.get("status") == "ok", 90)
        after = self.k("get", "pods", "-l", "app=cellbox-server")
        self.save("server-after-restart", after)
        old_uids = {p["metadata"]["uid"] for p in before["items"]}
        self.check(all(p["metadata"]["uid"] not in old_uids for p in after["items"]), "Server Pod was not replaced")

    def run(self):
        deployment = self.k("get", "deployment", self.args.deployment)
        containers = deployment["spec"]["template"]["spec"]["containers"]
        self.check(self.args.deployment == "cellbox-server" and any(c["name"] == "server" for c in containers), "Expected unified cellbox-server")
        self.save("deployment", deployment)
        self.k("rollout", "status", "deployment/" + self.args.deployment, "--timeout=240s", json_output=False)
        self.start_forward()
        profiles = self.eventually("Cellbox API", lambda: self.request("GET", "/v1/profiles"), lambda x: isinstance(x, list), 60)
        profile = next(p for p in profiles if p["id"] == self.args.profile_id)
        self.check(profile["provider"] == "resumable-k8s-pod", "Full suite requires Kubernetes checkpoint support")
        self.check(profile["capabilities"]["archives"] == "stopped-workspace", "Profile must advertise stopped workspace archives")
        self.workspace = profile["workspace"]
        self.check(not self.k("get", "cellboxes")["items"], "Full suite requires an isolated server without existing sandboxes")
        self.passed("isolated unified server and required capabilities")

        for method, path in [("GET", "/v1/images"), ("POST", "/v1/images:import"), ("POST", "/v1/routes"),
                             ("POST", "/v1/boxes/unused:upgrade"), ("POST", "/v1/boxes/unused:rebuild"),
                             ("POST", "/v1/boxes/unused:ensure-running"), ("POST", "/v1/boxes/unused:reconcile"),
                             ("POST", "/v1/boxes/unused/leases")]:
            self.request(method, path, {} if method == "POST" else None, status=404)
        self.passed("removed public APIs return 404")

        self.template = self.request("POST", "/v1/templates", {"name": self.run_id, "description": "full acceptance test", "defaultWarmSlots": 1}, status=201)
        first = self.build("tool-v1")
        self.check(first["version"] == "1.01-r1", "Unexpected first revision version")
        self.stable(first["id"])
        self.warm(first["id"])
        source, create_op = self.create(template=self.template["id"], headers={"Idempotency-Key": "ignored-create"})
        source_id = source["id"]
        self.check(source["revisionId"] == first["id"], "Stable was not resolved")
        duplicate, duplicate_op = self.create(template=self.template["id"], headers={"Idempotency-Key": "ignored-create"})
        self.check(source_id != duplicate["id"] and create_op["id"] != duplicate_op["id"], "Create was deduplicated")
        self.action(duplicate["id"], "destroy")
        self.boxes.remove(duplicate["id"])
        self.passed("template build, immutable revision, stable resolution and independent repeated create")

        state = self.ready(source_id)
        self.check(state["memory_bytes"] == 32 << 20, "Counter did not allocate expected memory")
        for expected in range(1, 11):
            self.check(self.app(source_id, "increment") == f"{expected}\n".encode(), "Counter increment mismatch")
        state = self.app(source_id, "state")
        self.save("state-before-suspend", state)
        proof = (json.dumps(state, sort_keys=True) + "\n").encode()
        self.file(source_id, "proof.json", proof)
        self.check(self.file(source_id, "proof.json") == proof, "File read/write mismatch")
        entries = self.request("GET", f"/v1/boxes/{source_id}/files?path=.&list=1")
        self.check(any(e["name"] == "proof.json" for e in entries), "Workspace listing omitted proof")
        self.check(self.request("GET", f"/v1/boxes/{source_id}/files?path=proof.json", status=206, headers={"Range": "bytes=0-9"}, raw=True) == proof[:10], "Range mismatch")
        tool, exec_op = self.execute(source_id, "/usr/local/bin/cellbox-e2e-tool")
        self.check(tool["result"]["stdout"] == "tool-v1\n", "buildCommand custom tool missing")
        for _ in range(2):
            self.execute(source_id, "printf 'once\\n' >> repeated-exec.txt", headers={"Idempotency-Key": "ignored-exec"})
        self.check(self.file(source_id, "repeated-exec.txt") == b"once\nonce\n", "Exec was deduplicated")
        for path in ["/v1/templates/" + self.template["id"], "/v1/template-revisions/" + first["id"],
                     "/v1/boxes/" + source_id, "/v1/operations/" + exec_op["id"], "/v1/execs/" + tool["id"]]:
            self.request("GET", path, status=404, client=self.other)
        self.request("GET", f"/v1/boxes/{source_id}/files?path=..%2Fetc%2Fpasswd", status=400)
        self.request("POST", f"/v1/boxes/{source_id}/archives", status=409, code="CONFLICT")
        self.passed("custom tool, exec, raw/range/list files, HTTP proxy, tenant isolation and archive source guard")

        runtime_before = self.runtime(source_id)
        self.save("runtime-before-warm-resume", runtime_before)
        generation_before = self.box(source_id)["generation"]
        self.action(source_id, "suspend", generation_before)
        self.check(self.box(source_id)["phase"] == "suspended", "Suspend did not settle")
        self.request("GET", f"/v1/boxes/{source_id}/files?path=proof.json", status=409, code="CONFLICT")
        self.warm(first["id"])
        warm_candidates = self.k("get", "pods", "-l", "cellbox.local/warm-pool=true")
        self.save("warm-candidates-before-resume", warm_candidates)
        warm_uids = {p["metadata"]["uid"] for p in warm_candidates["items"] if not p["metadata"].get("ownerReferences")}
        started = time.monotonic()
        self.action(source_id, "resume", generation_before)
        self.report["timings"]["warmResumeSeconds"] = round(time.monotonic() - started, 4)
        self.same_state(state, self.ready(source_id))
        runtime_after = self.runtime(source_id)
        self.save("runtime-after-warm-resume", runtime_after)
        self.check(runtime_after["pod"]["metadata"]["uid"] in warm_uids, "Resume did not adopt an existing warm Pod")
        self.check(runtime_after["pod"]["metadata"]["labels"].get("cellbox.local/warm-pool") == "true", "Warm adoption label missing")
        self.check(runtime_before["pod"]["metadata"]["uid"] != runtime_after["pod"]["metadata"]["uid"], "Suspend/resume did not replace Pod")
        self.check(runtime_before["service"]["metadata"]["uid"] == runtime_after["service"]["metadata"]["uid"], "Service was replaced")
        self.check(runtime_before["service"]["spec"]["clusterIP"] == runtime_after["service"]["spec"]["clusterIP"], "Service IP changed")
        self.request("POST", f"/v1/boxes/{source_id}/execs", {"argv": ["/bin/true"], "expectedGeneration": generation_before}, status=409, code="STALE_GENERATION")
        self.passed("warm Pod was actually adopted; PID, process memory and service identity preserved")

        self.patch_revision(first["id"], warmSlots=0)
        self.warm(first["id"], ready=False)
        self.action(source_id, "suspend")
        started = time.monotonic()
        self.action(source_id, "resume")
        self.report["timings"]["coldResumeSeconds"] = round(time.monotonic() - started, 4)
        self.same_state(state, self.ready(source_id))
        cold_runtime = self.runtime(source_id)
        self.save("runtime-after-cold-resume", cold_runtime)
        self.check(cold_runtime["pod"]["metadata"].get("labels", {}).get("cellbox.local/warm-pool") != "true", "Cold resume unexpectedly used a warm Pod")
        self.patch_revision(first["id"], warmSlots=1)
        self.passed("warm demand withdrawal and ordinary checkpoint resume")

        if self.args.restart_server:
            before_restart = self.runtime(source_id)
            # UNKNOWN_OUTCOME does not promise that the guest command stopped.
            # This fixture has a bounded runtime; let that bound elapse before
            # asking to suspend it later, without resubmitting the command.
            exec_deadline = time.monotonic() + 47
            op = self.submit(f"/v1/boxes/{source_id}/execs", {"argv": ["/bin/sh", "-ec", "printf 'once\\n' >> restart-proof.txt; sleep 30"], "expectedGeneration": self.box(source_id)["generation"], "timeoutMs": 45000})
            self.eventually("long command side effect", lambda: self.file(source_id, "restart-proof.txt"), lambda x: x == b"once\n")
            pending_build = self.submit(f"/v1/templates/{self.template['id']}/revisions", {
                "userVersion": "interrupted", "profileId": self.args.profile_id,
                "baseImage": self.args.source_image, "buildCommand": "printf 'E2E_BUILD_STARTED\\n'; sleep 120", "warmSlots": 0,
            })
            self.eventually("build worker started", lambda: self.request("GET", "/v1/operations/" + pending_build["id"]),
                            lambda x: x["status"] == "running" and x.get("stage") == "building")
            self.restart_server()
            interrupted = self.wait(self.request("GET", "/v1/operations/" + op["id"]), expected="failed")
            self.check(interrupted.get("error", {}).get("code") == "UNKNOWN_OUTCOME", "Interrupted command must have unknown outcome")
            execution = self.request("GET", "/v1/execs/" + op["result"]["execId"])
            self.check(execution["state"] == "unknown", "Interrupted exec must not appear exited")
            interrupted_build = self.wait(self.request("GET", "/v1/operations/" + pending_build["id"]), expected="failed")
            self.check(interrupted_build.get("error", {}).get("code") == "UNKNOWN_OUTCOME", "Interrupted build must have unknown outcome")
            self.check(len(self.request("GET", f"/v1/templates/{self.template['id']}/revisions")) == 1, "Interrupted build published or replayed a revision")
            self.check(self.file(source_id, "restart-proof.txt") == b"once\n", "Command replayed during restart")
            while time.monotonic() < exec_deadline:
                self.check(self.file(source_id, "restart-proof.txt") == b"once\n", "Command replayed after restart")
                time.sleep(min(0.5, max(0, exec_deadline - time.monotonic())))
            self.same_state(state, self.ready(source_id))
            self.check(self.runtime(source_id)["pod"]["metadata"]["uid"] == before_restart["pod"]["metadata"]["uid"], "API restart replaced sandbox Pod")
            persisted = self.request("GET", "/v1/templates/" + self.template["id"])
            self.check(persisted["stableRevisionId"] == first["id"], "Stable pointer lost across restart")
            self.check(self.request("GET", "/v1/template-revisions/" + first["id"])["image"] == first["image"], "Revision lost across restart")
            self.warm(first["id"])
            self.passed("server restart preserves SQLite and sandbox; interrupted exec/build have UNKNOWN_OUTCOME without replay")
        else:
            self.report["skipped"].append("server restart / UNKNOWN_OUTCOME")

        second = self.build("tool-v2")
        self.check(second["version"] == "1.01-r2" and second["image"] != first["image"], "Tool update did not produce a new immutable revision")
        self.check(second["resolvedBaseImage"] == first["resolvedBaseImage"], "Source image changed during test")
        self.stable(second["id"])
        current, _ = self.create(template=self.template["id"])
        current_id = current["id"]
        self.check(current["revisionId"] == second["id"] and self.box(source_id)["revisionId"] == first["id"], "Stable switch changed an existing sandbox")
        self.check(self.execute(current_id, "/usr/local/bin/cellbox-e2e-tool")[0]["result"]["stdout"] == "tool-v2\n", "New revision tool mismatch")
        self.check(self.execute(source_id, "/usr/local/bin/cellbox-e2e-tool")[0]["result"]["stdout"] == "tool-v1\n", "Old revision tool changed")
        first_pool, second_pool = self.warm(first["id"]), self.warm(second["id"])
        self.check(first_pool["pool"]["poolId"] != second_pool["pool"]["poolId"], "Different images shared a pool")
        usage = self.request("GET", f"/v1/templates/{self.template['id']}/runtime")
        self.save("template-runtime", usage)
        for revision in (first, second):
            observed = next(v for v in usage["revisions"] if v["revisionId"] == revision["id"])
            self.check(not observed["stale"] and observed["sandboxCount"] >= 1, "Runtime usage missing an active revision")
        pinned, _ = self.create(revision=first["id"])
        self.check(pinned["revisionId"] == first["id"], "Explicit revision did not override stable selection")
        self.action(pinned["id"], "destroy")
        self.boxes.remove(pinned["id"])
        self.action(current_id, "destroy")
        self.boxes.remove(current_id)
        self.passed("tool update creates 1.01-r2; stable switch, pinning, pool isolation and live usage")

        cancelled_build = self.submit(f"/v1/templates/{self.template['id']}/revisions", {
            "userVersion": "cancelled", "profileId": self.args.profile_id,
            "baseImage": self.args.source_image, "buildCommand": "sleep 120", "warmSlots": 0,
        })
        self.eventually("cancellable build started", lambda: self.request("GET", "/v1/operations/" + cancelled_build["id"]),
                        lambda x: x["status"] == "running")
        self.wait(self.request("POST", "/v1/operations/" + cancelled_build["id"] + ":cancel"), expected="cancelled")
        self.check(len(self.request("GET", f"/v1/templates/{self.template['id']}/revisions")) == 2, "Cancelled build published a revision")
        listed = self.request("GET", "/v1/operations?" + urllib.parse.urlencode({"templateId": self.template["id"]}))
        self.check(any(x["id"] == cancelled_build["id"] for x in listed), "Build operation listing omitted cancelled build")
        self.passed("build cancellation and operation listing do not publish a revision")

        self.action(source_id, "suspend")
        archive_op = self.wait(self.submit(f"/v1/boxes/{source_id}/archives"))
        archive_id = archive_op["result"]["archiveId"]
        self.archives.append(archive_id)
        self.report["resources"]["archives"].append(archive_id)
        self.check(self.box(source_id)["phase"] == "suspended", "Capture changed source lifecycle")
        archive = self.request("GET", "/v1/archives/" + archive_id)
        payload = self.request("GET", f"/v1/archives/{archive_id}/content")
        self.check(len(payload) == archive["size"] and hashlib.sha256(payload).hexdigest() == archive["sha256"], "Archive checksum mismatch")
        with tarfile.open(fileobj=io.BytesIO(payload), mode="r:gz") as tar:
            proof_member = next(m for m in tar.getmembers() if m.name.rstrip("/") == "proof.json")
            self.check(tar.extractfile(proof_member).read() == proof, "Archive did not contain workspace proof")
        self.request("GET", "/v1/archives/" + archive_id, status=404, client=self.other)
        self.request("POST", "/v1/boxes:restore", {"revisionId": second["id"], "archiveId": archive_id, "ownerKey": self.run_id}, status=409, code="ARCHIVE_INCOMPATIBLE")
        self.request("POST", "/v1/boxes:restore", {"revisionId": first["id"], "archiveId": archive_id, "ownerKey": self.run_id, "boxId": source_id}, status=400)
        self.action(source_id, "destroy")
        self.boxes.remove(source_id)
        restored, _ = self.create(revision=first["id"], archive=archive_id)
        self.check(restored["id"] != source_id and restored["phase"] == "staged", "Restore must create a new staged sandbox")
        self.check(self.file(restored["id"], "proof.json") == proof and self.file(restored["id"], "marker") == state["instance"].encode(), "Staged restore ran workload or lost files")
        self.action(restored["id"], "activate")
        restored_state = self.ready(restored["id"])
        self.check(restored_state["count"] == 0 and restored_state["instance"] != state["instance"], "Archive incorrectly restored process memory")
        self.check(self.file(restored["id"], "proof.json") == proof, "Activation lost workspace files")
        self.action(restored["id"], "destroy")
        self.boxes.remove(restored["id"])
        migrated, _ = self.create(revision=second["id"], archive=archive_id, accept_image_change=True)
        self.check(migrated["phase"] == "staged" and self.file(migrated["id"], "proof.json") == proof, "Explicit cross-image restore failed")
        self.check(self.execute(migrated["id"], "/usr/local/bin/cellbox-e2e-tool")[0]["result"]["stdout"] == "tool-v2\n", "Cross-image restore did not use new tools")
        self.action(migrated["id"], "activate")
        self.check(self.ready(migrated["id"])["count"] == 0, "Migrated counter did not start fresh")
        self.action(migrated["id"], "destroy")
        self.boxes.remove(migrated["id"])
        self.passed("S3 archive bytes verified; new staged restore, activation and explicit tool-version migration")

        if self.args.sdk:
            root = Path(__file__).resolve().parents[2]
            environment = {**os.environ, "CELLBOX_API_URL": self.base, "CELLBOX_CLIENT_ID": self.client, "CELLBOX_REVISION_ID": second["id"]}
            environment.pop("CELLBOX_TEMPLATE_ID", None)
            result = subprocess.run(["go", "run", "./examples/lifecycle"], cwd=root / "sdk/go", env=environment, capture_output=True, text=True, timeout=360)
            (self.evidence / "sdk-lifecycle.log").write_text(result.stdout + result.stderr)
            self.check(result.returncode == 0 and "command ran after lifecycle transition" in result.stdout, "SDK lifecycle failed: " + result.stdout + result.stderr)
            self.check(not self.request("GET", "/v1/boxes"), "SDK failed to destroy its sandbox")
            self.passed("Go SDK create/files/suspend/resume/refreshed handle/exec/destroy")
        else:
            self.report["skipped"].append("Go SDK lifecycle")

    def cleanup(self):
        errors = []
        # Runtime evidence first, while any failed sandbox still exists.
        for resource in ("cellboxes", "pods", "services"):
            try:
                self.save("diagnostic-" + resource, self.k("get", resource))
            except Exception as error:
                errors.append(str(error))
        try:
            for box in self.request("GET", "/v1/boxes"):
                if box["id"] not in self.boxes:
                    self.boxes.append(box["id"])
        except Exception as error:
            errors.append("list run sandboxes for cleanup: " + str(error))
        for box_id in list(reversed(self.boxes)):
            try:
                box = self.box(box_id)
                if box["phase"] != "deleted":
                    if box.get("operationId"):
                        self.wait(self.request("GET", "/v1/operations/" + box["operationId"]), expected=None)
                    self.action(box_id, "destroy")
                self.boxes.remove(box_id)
            except Exception as error:
                errors.append(f"destroy {box_id}: {error}")
        for archive_id in list(self.archives):
            try:
                self.request("DELETE", "/v1/archives/" + archive_id, status=204)
                self.archives.remove(archive_id)
            except Exception as error:
                errors.append(f"delete archive {archive_id}: {error}")
        if self.template:
            try:
                self.stable("")
                for revision in self.revisions:
                    self.patch_revision(revision, warmSlots=0, status="retired")
                self.report["preservedMetadata"] = {"templateId": self.template["id"], "revisionIds": list(self.revisions), "reason": "Built revisions have no delete API; retired with zero warm demand. Registry images are retained."}
            except Exception as error:
                errors.append("retire revisions: " + str(error))
        try:
            remaining = self.request("GET", "/v1/boxes")
            self.check(not remaining, "Run still owns live sandboxes: " + str(remaining))
            self.check(not self.request("GET", "/v1/archives"), "Run still owns archives")
            if self.revisions:
                self.warm(self.revisions[0], ready=False)
            self.eventually("idle warm Pods removed", lambda: self.k("get", "pods", "-l", "cellbox.local/warm-pool=true")["items"],
                            lambda rows: not [p for p in rows if not p["metadata"].get("ownerReferences")], 60)
            for resource in ("cellboxes", "pods", "services"):
                self.save("final-" + resource, self.k("get", resource))
            self.passed("cleanup: no live sandbox/archive or warm demand; built metadata explicitly retained")
        except Exception as error:
            errors.append(str(error))
        try:
            logs = self.k("logs", "deployment/" + self.args.deployment, "-c", "server", "--tail=200", json_output=False)
            (self.evidence / "server.log").write_text(logs)
        except Exception as error:
            errors.append("collect server log: " + str(error))
        self.report["cleanupErrors"] = errors
        self.stop_forward()
        return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", default="k3s")
    parser.add_argument("--namespace", required=True, help="Dedicated test namespace; never the production/CoCell namespace")
    parser.add_argument("--deployment", default="cellbox-server")
    parser.add_argument("--profile-id", required=True)
    parser.add_argument("--source-image", required=True, help="Immutable test/e2e/Dockerfile image including /server and /bin/sh")
    parser.add_argument("--evidence-dir", default="")
    parser.add_argument("--restart-server", action="store_true", help="Restart only this dedicated server to verify durable state and no replay")
    parser.add_argument("--sdk", action="store_true", help="Also run the Go SDK lifecycle example against the built revision")
    args = parser.parse_args()
    if args.namespace in ("cell-box", "co-cell", "default", "kube-system"):
        parser.error("Use a dedicated namespace, not cell-box/co-cell")
    if "@sha256:" not in args.source_image:
        parser.error("--source-image must use repository@sha256:digest")
    suite = Suite(args)
    error = None
    try:
        suite.run()
    except Exception as exception:
        error = str(exception)
        suite.report["error"] = error
        (suite.evidence / "failure.txt").write_text(traceback.format_exc())
        print("FAIL: " + error, file=sys.stderr, flush=True)
    finally:
        cleanup_errors = suite.cleanup()
        suite.report["status"] = "failed" if error or cleanup_errors else "passed"
        suite.report["durationSeconds"] = round(time.monotonic() - suite.started, 3)
        suite.save("report", suite.report)
    print("Evidence: " + str(suite.evidence), flush=True)
    return 1 if suite.report["status"] == "failed" else 0


if __name__ == "__main__":
    sys.exit(main())
