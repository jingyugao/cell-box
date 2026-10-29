"""Resume readiness acceptance flow, split around the server deployment.

The prepare phase creates and suspends a Box with the old Guest. The verify
phase runs after deployment and exercises old-checkpoint compatibility,
explicit ports, a warm restore, and health-check failure handling. Cleanup only
uses resource IDs saved by these phases.
"""

import argparse
import datetime
import ipaddress
import json
import os
from pathlib import Path
import shlex
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request


CLIENT_ID = "waitports-e2e-20261029"
BASE_IMAGE = "192.168.133.1:5001/mybox@sha256:e2aa919642df6fd323da2a5e8136f7b8061ea9bdcfee03f91f9089fa5b52388d"
PROFILE_ID = "cocell-k8s-resumable"
MANAGEMENT_PORT = 40000
APP_PORT = 8000
EXTRA_PORT = 8011


class ResumePorts:
    def __init__(self, args):
        self.args = args
        self.state_path = Path(args.state)
        self.state_preexisting = self.state_path.exists()
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        self.state = {
            "clientId": CLIENT_ID,
            "api": args.api,
            "runToken": str(time.time_ns()),
            "context": args.context,
            "namespace": args.namespace,
            "createdAt": utc_now(),
            "resources": {"boxes": [], "volumes": [], "templates": [], "revisions": [], "operations": []},
            "evidence": {},
        }

    def save(self):
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.state_path.with_suffix(self.state_path.suffix + ".tmp")
        temporary.write_text(json.dumps(self.state, indent=2, sort_keys=True) + "\n")
        temporary.replace(self.state_path)

    def load(self):
        self.state = json.loads(self.state_path.read_text())
        if self.state.get("clientId") != CLIENT_ID:
            raise RuntimeError("state file belongs to a different client")
        if self.state.get("api") != self.args.api:
            raise RuntimeError("state file API endpoint does not match arguments")
        if self.state.get("context") != self.args.context or self.state.get("namespace") != self.args.namespace:
            raise RuntimeError("state file Kubernetes target does not match arguments")
        self.state.setdefault("resources", {})
        for key in ("boxes", "volumes", "templates", "revisions", "operations"):
            self.state["resources"].setdefault(key, [])
        self.state.setdefault("evidence", {})

    def request(self, method, path, data=None, *, expected=200, raw=False, timeout=30):
        headers = {"X-Cellbox-Client-ID": CLIENT_ID}
        token = os.environ.get("CELLBOX_CLIENT_TOKEN")
        if token:
            headers["Authorization"] = "Bearer " + token
        if data is not None:
            data = json.dumps(data, separators=(",", ":")).encode()
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(self.args.api.rstrip("/") + path, data=data, method=method, headers=headers)
        try:
            response = self.opener.open(request, timeout=timeout)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            body = response.read()
            if response.code != expected:
                detail = body.decode(errors="replace")[:2000]
                raise RuntimeError(f"{method} {path}: HTTP {response.code}, expected {expected}: {detail}")
            if raw:
                return body
            if not body:
                return None
            if "json" not in response.headers.get("Content-Type", "").lower():
                return body
            return json.loads(body)

    def api_path(self, resource):
        return urllib.parse.quote(str(resource), safe="")

    def submit(self, path, body=None):
        operation = self.request("POST", path, body, expected=202)
        if not operation or not operation.get("id"):
            raise RuntimeError(f"operation response has no ID: {operation!r}")
        saved = {"id": operation["id"], "targetId": operation.get("targetId"), "kind": operation.get("kind"), "status": operation.get("status")}
        self.state["resources"]["operations"].append(saved)
        self.save()
        return operation

    def wait(self, operation, timeout=900):
        deadline = time.monotonic() + timeout
        while operation.get("status") in ("queued", "running", "cancelling"):
            if time.monotonic() >= deadline:
                raise TimeoutError("operation deadline: " + json.dumps(operation))
            operation = self.request("GET", "/v1/operations/" + self.api_path(operation["id"]) + "?waitMs=1000")
        for saved in self.state["resources"]["operations"]:
            if saved["id"] == operation["id"]:
                saved.update({"status": operation.get("status"), "result": operation.get("result"), "error": operation.get("error"), "completedAt": utc_now()})
                break
        self.save()
        if operation.get("status") != "succeeded":
            raise RuntimeError("operation did not succeed: " + json.dumps(operation))
        return operation

    def box(self, box_id):
        return self.request("GET", "/v1/boxes/" + self.api_path(box_id))

    def create_box(self, revision_id, label):
        op = self.submit("/v1/boxes", {"ownerKey": CLIENT_ID, "revisionId": revision_id})
        box_id = op.get("targetId")
        if not box_id:
            raise RuntimeError("create operation did not return a Box targetId")
        record = {"id": box_id, "label": label, "revisionId": revision_id, "createOperationId": op["id"]}
        if not any(item["id"] == box_id for item in self.state["resources"]["boxes"]):
            self.state["resources"]["boxes"].append(record)
        self.save()
        self.wait(op)
        current = self.box(box_id)
        self.capture_box_resources(record, current)
        self.save()
        return current

    def capture_box_resources(self, record, box):
        record["ownerKey"] = box.get("ownerKey")
        record["homeVolumeId"] = box.get("homeVolumeId", "")
        record["phase"] = box.get("phase")
        ids = set()
        if box.get("homeVolumeId"):
            ids.add(box["homeVolumeId"])
        for mount in box.get("mounts", []):
            if mount.get("volumeId"):
                ids.add(mount["volumeId"])
        for volume_id in ids:
            if not any(v.get("id") == volume_id for v in self.state["resources"]["volumes"]):
                self.state["resources"]["volumes"].append({"id": volume_id, "boxId": box["id"]})

    def action(self, box_id, action, wait_ports=None):
        body = {"waitPorts": wait_ports} if wait_ports is not None else None
        op = self.submit(f"/v1/boxes/{self.api_path(box_id)}:{action}", body)
        return self.wait(op)

    def service(self, box_id, port, path="", *, raw=False):
        return self.request("GET", f"/v1/boxes/{self.api_path(box_id)}/services/{port}/{path}", raw=raw, timeout=12)

    def service_request(self, box_id, port, path, method, data=None, *, raw=False):
        return self.request(method, f"/v1/boxes/{self.api_path(box_id)}/services/{port}/{path}", data,
                            raw=raw, timeout=12)

    @staticmethod
    def same_instance(before, after):
        return before.get("instance") == after.get("instance") and before.get("pid") == after.get("pid")

    def initial_state(self, box_id, seconds=60):
        deadline = time.monotonic() + seconds
        last = None
        while time.monotonic() < deadline:
            try:
                return self.service(box_id, APP_PORT, "state")
            except (OSError, RuntimeError) as error:
                last = str(error)
            time.sleep(0.5)
        raise TimeoutError("counter did not start: " + repr(last))

    def create_template(self, label):
        template = self.request("POST", "/v1/templates", {
            "name": CLIENT_ID + "-" + label + "-" + self.state["runToken"],
            "description": "Resume wait-port acceptance test",
            "defaultWarmSlots": 0,
        }, expected=201)
        record = {"id": template["id"], "label": label, "name": template.get("name")}
        self.state["resources"]["templates"].append(record)
        self.save()
        return template

    def create_revision(self, template_id, label):
        app = '''import json, os, threading, time, uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

STATE = {"instance": uuid.uuid4().hex, "count": 0, "started": time.time(), "pid": os.getpid()}
extra = None

class ExtraHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = b"ok\\n"
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *_):
        pass

class Handler(BaseHTTPRequestHandler):
    def reply(self, code, value):
        body = json.dumps(value).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def do_GET(self):
        if self.path != "/state":
            self.reply(404, {"error": "not found"})
            return
        STATE["count"] += 1
        self.reply(200, STATE)
    def do_POST(self):
        global extra
        if self.path == "/open-extra":
            if extra is None:
                extra = ThreadingHTTPServer(("0.0.0.0", 8011), ExtraHandler)
                threading.Thread(target=extra.serve_forever, daemon=True).start()
            self.reply(200, {"listening": 8011})
        elif self.path == "/close-extra":
            if extra is not None:
                extra.shutdown()
                extra.server_close()
                extra = None
            self.reply(200, {"closed": 8011})
        else:
            self.reply(404, {"error": "not found"})
    def log_message(self, *_):
        pass

ThreadingHTTPServer(("0.0.0.0", 8000), Handler).serve_forever()
'''
        build = "mkdir -p /opt/cellbox && cat > /opt/cellbox/port-test.py <<'CELLBOX_E2E_PY'\n" + app + "CELLBOX_E2E_PY\nchmod 0644 /opt/cellbox/port-test.py"
        run = ("docker run --entrypoint /usr/local/bin/python3 --workdir /home/agent/workspace "
               + BASE_IMAGE + " /opt/cellbox/port-test.py")
        op = self.submit("/v1/templates/" + self.api_path(template_id) + "/revisions", {
            "userVersion": "1.01",
            "profileId": PROFILE_ID,
            "baseImage": BASE_IMAGE,
            "runCommand": run,
            "buildCommand": build,
            "warmSlots": 0,
        })
        operation = self.wait(op)
        revision_id = (operation.get("result") or {}).get("revisionId")
        if not revision_id:
            raise RuntimeError("revision build operation did not return revisionId")
        revision = self.request("GET", "/v1/template-revisions/" + self.api_path(revision_id))
        record = {"id": revision_id, "templateId": template_id, "label": label, "version": revision.get("version"), "image": revision.get("image")}
        self.state["resources"]["revisions"].append(record)
        self.save()
        if "@sha256:" not in revision.get("image", ""):
            raise RuntimeError("built revision image is not immutable: " + json.dumps(revision))
        return revision

    def suspend_box(self, box_id):
        self.action(box_id, "suspend")
        box = self.box(box_id)
        if box.get("phase") != "suspended":
            raise RuntimeError("Box did not suspend: " + json.dumps(box))
        for record in self.state["resources"]["boxes"]:
            if record["id"] == box_id:
                self.capture_box_resources(record, box)
                record["phase"] = box["phase"]
        self.save()
        return box

    def get_operation(self, operation_id):
        return self.request("GET", "/v1/operations/" + self.api_path(operation_id))

    def kube(self, *args):
        command = ["kubectl", "--context", self.args.context, "-n", self.args.namespace, *args, "-o", "json"]
        result = subprocess.run(command, capture_output=True, text=True, timeout=30)
        if result.returncode:
            raise RuntimeError("kubectl " + " ".join(args) + ": " + result.stderr[-2000:])
        return json.loads(result.stdout)

    def runtime(self, box_id):
        objects = self.kube("get", "cellboxes", "-l", "cellbox.local/box-id=" + box_id)
        rows = objects.get("items", [])
        if len(rows) != 1:
            raise RuntimeError(f"expected one Cellbox CR for {box_id}, got {len(rows)}")
        cr = rows[0]
        status = cr.get("status", {})
        execution = status.get("execution") or {}
        pod_name = status.get("podName") or execution.get("podName")
        pod = None
        if pod_name:
            pod = self.kube("get", "pod", pod_name)
        return {"cr": cr, "pod": pod}

    def current_pod_ip(self, box_id, image, previous_uid, seconds=120):
        deadline = time.monotonic() + seconds
        last = None
        while time.monotonic() < deadline:
            try:
                objects = self.kube("get", "cellboxes", "-l", "cellbox.local/box-id=" + box_id)
                cr_rows = objects.get("items", [])
                if len(cr_rows) != 1:
                    raise RuntimeError(f"expected one Cellbox CR for {box_id}, got {len(cr_rows)}")
                cr = cr_rows[0]
                execution = (cr.get("status", {}).get("execution") or {})
                pod = None
                pod_name = cr.get("status", {}).get("podName")
                if pod_name:
                    try:
                        pod = self.kube("get", "pod", pod_name)
                    except RuntimeError:
                        pass
                runtime = {"cr": cr, "pod": pod}
                execution_uid = execution.get("podUID")
                if (pod and execution.get("podIP") and execution_uid and
                        execution_uid == pod.get("metadata", {}).get("uid") and execution_uid != previous_uid):
                    ipaddress.ip_address(execution["podIP"])
                    return runtime, execution["podIP"]

                # A warm restore keeps its lease outside CR status until the
                # operation completes. Select only the unique exact-image warm
                # Pod, and later verify its Guest proxy serves this Box instance.
                rows = self.kube("get", "pods", "-l", "cellbox.local/warm-pool=true").get("items", [])
                candidates = [p for p in rows
                              if p.get("metadata", {}).get("uid") != previous_uid
                              and any(c.get("image") == image for c in p.get("spec", {}).get("containers", []))]
                if len(candidates) > 1:
                    raise RuntimeError("multiple warm Pods match the exact revision image; refusing ambiguous Guest access")
                if len(candidates) == 1:
                    candidate = candidates[0]
                    pod_ip = candidate.get("status", {}).get("podIP")
                    if candidate.get("status", {}).get("phase") == "Running" and pod_ip:
                        ipaddress.ip_address(pod_ip)
                        runtime["warmCandidate"] = candidate
                        return runtime, pod_ip
                last = {"phase": cr.get("status", {}).get("phase"), "execution": execution,
                        "candidateCount": len(candidates), "pod": pod}
            except (OSError, RuntimeError, ValueError) as error:
                last = str(error)
            time.sleep(0.5)
        raise TimeoutError("timed out finding restored Pod IP: " + repr(last))

    def ssh_curl(self, url, *, post_json=None, timeout=15):
        command = ["curl", "--fail", "--silent", "--show-error", "--max-time", str(max(2, timeout - 2))]
        if post_json is not None:
            command += ["-X", "POST", "-H", "Content-Type: application/json", "--data-binary", json.dumps(post_json, separators=(",", ":"))]
        command += [url]
        result = subprocess.run(["ssh", "-o", "BatchMode=yes", self.args.ssh, shlex.join(command)], capture_output=True, text=True, timeout=timeout)
        if result.returncode:
            raise RuntimeError(f"SSH curl failed ({result.returncode}): {result.stderr[-1000:]}")
        return result.stdout

    def wait_guest_control(self, box_id, pod_ip, seconds=120):
        deadline = time.monotonic() + seconds
        url = f"http://{pod_ip}:{MANAGEMENT_PORT}/healthz"
        last = None
        while time.monotonic() < deadline:
            try:
                return self.ssh_curl(url, timeout=12)
            except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
                last = str(error)
            time.sleep(0.5)
        raise TimeoutError(f"Guest control did not become reachable for {box_id}: {last}")

    def wait_guest_instance(self, pod_ip, expected_state, seconds=120):
        url = f"http://{pod_ip}:{MANAGEMENT_PORT}/proxy/{APP_PORT}/state"
        deadline = time.monotonic() + seconds
        last = None
        while time.monotonic() < deadline:
            try:
                state = json.loads(self.ssh_curl(url, timeout=12))
                if not self.same_instance(expected_state, state):
                    raise RuntimeError("exact-image Guest serves a different counter instance or PID; refusing to execute inside it")
                return state
            except (OSError, ValueError, subprocess.TimeoutExpired) as error:
                last = str(error)
            except RuntimeError as error:
                if "different counter instance or PID" in str(error):
                    raise
                last = str(error)
            time.sleep(0.5)
        raise TimeoutError("restored Guest proxy did not expose the saved Box instance: " + repr(last))

    def wait_warm_ready(self, revision_id, seconds=240):
        deadline = time.monotonic() + seconds
        path = "/v1/template-revisions/" + self.api_path(revision_id) + "/warm-pool"
        last = None
        while time.monotonic() < deadline:
            last = self.request("GET", path)
            pool = last.get("pool") or {}
            if not last.get("stale") and last.get("syncStatus") == "applied" and pool.get("readySlots", 0) >= 1:
                self.state["evidence"]["warmReady"] = {"revision": revision_id, "pool": last, "observedAt": utc_now()}
                self.save()
                return last
            time.sleep(1)
        raise TimeoutError("warm pool did not become ready: " + json.dumps(last))

    def patch_revision(self, revision_id, **changes):
        current = self.request("GET", "/v1/template-revisions/" + self.api_path(revision_id))
        result = self.request("PATCH", "/v1/template-revisions/" + self.api_path(revision_id), {
            "expectedGeneration": current["generation"], **changes,
        })
        self.save()
        return result

    def prepare_legacy(self):
        if self.state_path.exists():
            raise RuntimeError("state file already exists; refuse to overwrite resources")
        template = self.create_template("legacy")
        revision = self.create_revision(template["id"], "legacy")
        box = self.create_box(revision["id"], "legacy")
        state = self.initial_state(box["id"])
        self.state["evidence"]["legacyInitialState"] = state
        self.state["evidence"]["legacyPreparedAt"] = utc_now()
        self.save()
        self.suspend_box(box["id"])
        self.state["phase"] = "prepared-legacy"
        self.state["preparedAt"] = utc_now()
        self.save()
        print(json.dumps({"phase": self.state["phase"], "boxId": box["id"], "templateId": template["id"], "revisionId": revision["id"], "instance": state.get("instance")}, indent=2))

    def verify(self):
        self.load()
        legacy = self.find_box("legacy")
        initial = self.state["evidence"]["legacyInitialState"]
        started = time.monotonic()
        op = self.action(legacy["id"], "resume")
        first_state_started = time.monotonic()
        first_state = self.service(legacy["id"], APP_PORT, "state")
        first_request_seconds = time.monotonic() - first_state_started
        resumed = self.box(legacy["id"])
        if resumed.get("phase") != "running":
            raise RuntimeError("legacy default resume did not reach running: " + json.dumps(resumed))
        legacy_result = {"operationId": op["id"], "resumeSeconds": round(first_state_started - started, 3), "firstRequestSeconds": round(first_request_seconds, 3), "firstState": first_state}
        if not self.same_instance(initial, first_state):
            raise RuntimeError("legacy resume changed the counter instance or PID: " + json.dumps({"before": initial, "after": first_state}))
        self.state["evidence"]["legacyResume"] = legacy_result
        self.save()
        self.suspend_box(legacy["id"])

        template = self.create_template("current")
        revision = self.create_revision(template["id"], "current")
        box = self.create_box(revision["id"], "current")
        initial_new = self.initial_state(box["id"])
        self.state["evidence"]["currentInitialState"] = initial_new
        self.save()
        self.suspend_box(box["id"])
        self.action(box["id"], "resume", [MANAGEMENT_PORT, APP_PORT])
        first_after_resume = self.service(box["id"], APP_PORT, "state")
        if not self.same_instance(initial_new, first_after_resume):
            raise RuntimeError("custom-port resume changed the counter instance or PID")
        self.state["evidence"]["customResume"] = {"ports": [MANAGEMENT_PORT, APP_PORT], "state": first_after_resume, "observedAt": utc_now()}
        self.save()
        self.verify_warm()

    def verify_warm(self):
        self.load()
        if "customResume" not in self.state["evidence"] or "delayedResume" in self.state["evidence"]:
            raise RuntimeError("warm verification requires completed custom resume and no previous delayed resume")
        record = self.find_box("current")
        box = self.box(record["id"])
        revision = self.request("GET", "/v1/template-revisions/" + self.api_path(record["revisionId"]))
        initial_new = self.state["evidence"]["currentInitialState"]
        before_warm = self.state["evidence"].get("podBeforeWarmResume")
        if before_warm is None:
            before_warm = self.runtime(box["id"])
            self.state["evidence"]["podBeforeWarmResume"] = before_warm
            self.save()
        if box.get("phase") == "running":
            self.suspend_box(box["id"])
        elif box.get("phase") != "suspended":
            raise RuntimeError("warm verification requires a running or suspended Box")
        self.patch_revision(revision["id"], warmSlots=1)
        pool = self.wait_warm_ready(revision["id"])
        warm_pods = self.kube("get", "pods", "-l", "cellbox.local/warm-pool=true").get("items", [])
        self.state["evidence"]["warmPoolBeforeResume"] = {
            "status": pool,
            "pods": [{"name": p["metadata"]["name"], "uid": p["metadata"].get("uid"), "nodeName": p.get("spec", {}).get("nodeName")} for p in warm_pods],
        }
        self.save()

        ports = [MANAGEMENT_PORT, APP_PORT, EXTRA_PORT]
        resume_started = time.monotonic()
        pending_op = self.submit(f"/v1/boxes/{self.api_path(box['id'])}:resume", {"waitPorts": ports})
        self.state["evidence"]["delayedResume"] = {"operationId": pending_op["id"], "ports": ports, "startedAt": utc_now()}
        self.save()
        old_pod_uid = (before_warm.get("pod") or {}).get("metadata", {}).get("uid")
        runtime, pod_ip = self.current_pod_ip(box["id"], revision["image"], old_pod_uid)
        self.wait_guest_control(box["id"], pod_ip)
        observed_state = self.wait_guest_instance(pod_ip, initial_new)
        observed_op = self.get_operation(pending_op["id"])
        if observed_op.get("status") not in ("queued", "running"):
            raise RuntimeError("resume should remain pending while port 8011 is absent: " + json.dumps(observed_op))
        self.state["evidence"]["delayedResume"].update({
            "pendingObservedAt": utc_now(), "pendingStatus": observed_op.get("status"),
            "runtimeAtPending": runtime, "podIP": pod_ip, "verifiedGuestState": observed_state,
        })
        self.save()

        exec_result = self.ssh_curl(f"http://{pod_ip}:{MANAGEMENT_PORT}/proxy/{APP_PORT}/open-extra",
                                    post_json={}, timeout=15)
        self.state["evidence"]["startDelayedListener"] = {"response": exec_result, "sentAt": utc_now()}
        self.save()
        self.wait(pending_op)
        waited = time.monotonic() - resume_started
        body = self.service(box["id"], EXTRA_PORT, "", raw=True)
        if body.strip() != b"ok":
            raise RuntimeError("public service 8011 returned unexpected content: " + repr(body[:500]))
        final_state = self.service(box["id"], APP_PORT, "state")
        if not self.same_instance(initial_new, final_state):
            raise RuntimeError("warm resume changed the counter instance or PID")
        after_warm = self.runtime(box["id"])
        old_pod = (before_warm.get("pod") or {}).get("metadata", {}).get("uid")
        new_pod = (after_warm.get("pod") or {}).get("metadata", {}).get("uid")
        warm_evidence = {
            "resumeOperationId": pending_op["id"], "resumeSeconds": round(waited, 3), "podIP": pod_ip,
            "oldPodUID": old_pod, "newPodUID": new_pod,
            "podLabels": ((after_warm.get("pod") or {}).get("metadata", {}).get("labels", {})),
            "crExecution": after_warm["cr"].get("status", {}).get("execution"),
            "port8011Body": body.decode(errors="replace"), "instance": final_state.get("instance"),
        }
        if not old_pod or not new_pod:
            raise RuntimeError("warm-resume Pod UID evidence is missing")
        if old_pod == new_pod:
            raise RuntimeError("warm resume unexpectedly reused the suspended Pod")
        self.state["evidence"]["warmResume"] = warm_evidence
        self.state["phase"] = "verified-resume"
        self.state["verifiedAt"] = utc_now()
        self.save()

        health_started = time.monotonic()
        self.state["evidence"]["closeDelayedListener"] = {"sentAt": utc_now()}
        self.save()
        self.service_request(box["id"], APP_PORT, "close-extra", "POST", {})
        failed_box, failed_runtime = self.wait_health_failure(box["id"], 25)
        pod = failed_runtime.get("pod")
        if not pod:
            raise RuntimeError("failed Cellbox has no retained Pod")
        serving = pod.get("metadata", {}).get("labels", {}).get("cellbox.local/serving")
        if serving != "false":
            raise RuntimeError("failed Pod should be retained with serving=false: " + repr(serving))
        self.state["evidence"]["healthFailure"] = {
            "box": failed_box, "cr": failed_runtime["cr"], "pod": pod,
            "observedAt": utc_now(), "serving": serving,
            "closeRequestToFailedSeconds": round(time.monotonic() - health_started, 3),
        }
        self.state["phase"] = "verified"
        self.state["verifiedAt"] = utc_now()
        self.save()
        print(json.dumps({"phase": self.state["phase"], "legacy": self.state["evidence"]["legacyResume"], "warmResume": warm_evidence, "healthFailure": {"boxPhase": failed_box.get("phase"), "crPhase": failed_runtime["cr"].get("status", {}).get("phase"), "serving": serving}}, indent=2))

    def wait_health_failure(self, box_id, seconds):
        deadline = time.monotonic() + seconds
        last = None
        while time.monotonic() < deadline:
            try:
                box = self.box(box_id)
                runtime = self.runtime(box_id)
                cr_phase = runtime["cr"].get("status", {}).get("phase")
                pod = runtime.get("pod") or {}
                serving = pod.get("metadata", {}).get("labels", {}).get("cellbox.local/serving")
                if box.get("phase") == "failed" and cr_phase == "Failed" and pod and serving == "false":
                    return box, runtime
                last = {"boxPhase": box.get("phase"), "crPhase": cr_phase, "podName": pod.get("metadata", {}).get("name"), "serving": serving}
            except (OSError, RuntimeError, AssertionError) as error:
                last = str(error)
            time.sleep(0.5)
        raise TimeoutError("independent health check did not fail the Box and CR within 25s: " + repr(last))

    def find_box(self, label):
        matches = [b for b in self.state["resources"]["boxes"] if b.get("label") == label]
        if len(matches) != 1:
            raise RuntimeError(f"expected one saved {label} Box, got {len(matches)}")
        return matches[0]

    def cleanup(self):
        self.load()
        errors = []
        for saved in reversed(self.state["resources"]["boxes"]):
            box_id = saved["id"]
            try:
                box = self.box(box_id)
                if box.get("ownerKey") != CLIENT_ID:
                    raise RuntimeError("refusing to destroy Box with unexpected ownerKey")
                self.capture_box_resources(saved, box)
                if box.get("phase") != "deleted":
                    self.action(box_id, "destroy")
                saved["phase"] = "deleted"
                self.save()
            except Exception as error:
                errors.append(f"destroy Box {box_id}: {error}")
                self.state["cleanupErrors"] = errors[:]
                self.save()
        for volume in list(self.state["resources"]["volumes"]):
            volume_id = volume["id"]
            try:
                current = self.request("GET", "/v1/volumes/" + self.api_path(volume_id))
                if current.get("ownerKey") != CLIENT_ID:
                    raise RuntimeError("refusing to delete volume with unexpected ownerKey")
                if current.get("phase") != "deleted":
                    op = self.request("DELETE", "/v1/volumes/" + self.api_path(volume_id), expected=202)
                    self.state["resources"]["operations"].append({"id": op["id"], "targetId": volume_id, "kind": "volume-delete", "status": op.get("status")})
                    self.save()
                    self.wait(op)
                self.state["resources"]["volumes"].remove(volume)
                self.save()
            except Exception as error:
                errors.append(f"delete volume {volume_id}: {error}")
                self.state["cleanupErrors"] = errors[:]
                self.save()
        for template in self.state["resources"]["templates"]:
            try:
                current = self.request("GET", "/v1/templates/" + self.api_path(template["id"]))
                self.request("PATCH", "/v1/templates/" + self.api_path(template["id"]), {
                    "expectedGeneration": current["generation"], "stableRevisionId": "",
                })
                template["stableRevisionCleared"] = True
                self.save()
            except Exception as error:
                errors.append(f"clear stable revision for template {template['id']}: {error}")
                self.state["cleanupErrors"] = errors[:]
                self.save()
        for revision in self.state["resources"]["revisions"]:
            try:
                self.patch_revision(revision["id"], warmSlots=0, status="retired")
                revision["retired"] = True
                self.save()
            except Exception as error:
                errors.append(f"retire revision {revision['id']}: {error}")
                self.state["cleanupErrors"] = errors[:]
                self.save()
        self.state["cleanupErrors"] = errors
        self.state["phase"] = "cleanup-complete" if not errors else "cleanup-incomplete"
        self.state["cleanedAt"] = utc_now()
        self.save()
        if errors:
            raise RuntimeError("cleanup incomplete: " + "; ".join(errors))
        print(json.dumps({"phase": self.state["phase"], "boxes": self.state["resources"]["boxes"], "volumes": self.state["resources"]["volumes"], "revisionsRetired": [r["id"] for r in self.state["resources"]["revisions"]], "templatesPreserved": [t["id"] for t in self.state["resources"]["templates"]]}, indent=2))


def utc_now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", required=True, choices=("prepare-legacy", "verify", "verify-warm", "cleanup"))
    parser.add_argument("--api", default="http://127.0.0.1:18093")
    parser.add_argument("--state", required=True, help="persistent JSON file shared across phases")
    parser.add_argument("--context", default="k3s")
    parser.add_argument("--namespace", default="cell-box")
    parser.add_argument("--ssh", default="ubuntu")
    return parser.parse_args()


def main():
    suite = ResumePorts(parse_args())
    try:
        if suite.args.phase == "prepare-legacy":
            suite.prepare_legacy()
        elif suite.args.phase == "verify":
            suite.verify()
        elif suite.args.phase == "verify-warm":
            suite.verify_warm()
        else:
            suite.cleanup()
    except BaseException as error:
        if suite.args.phase == "prepare-legacy" and suite.state_preexisting:
            raise
        if suite.state_path.exists():
            try:
                suite.load()
            except Exception:
                pass
        suite.state["lastError"] = {"phase": suite.args.phase, "message": str(error), "at": utc_now()}
        suite.save()
        raise


if __name__ == "__main__":
    main()
