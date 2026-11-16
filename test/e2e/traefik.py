"""Exercise request-triggered resume through a real Kubernetes Traefik Ingress."""

import argparse
import concurrent.futures
import http.client
import json
from pathlib import Path
import shlex
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


class Validation:
    def __init__(self, args):
        self.args = args
        token = uuid.uuid4().hex[:10]
        self.name = "traefik-e2e-" + token
        self.client = self.name
        self.state = {"context": args.context, "namespace": args.namespace, "clientId": self.client,
                      "boxId": self.name, "host": self.name + ".cellbox.test", "cycles": []}
        self.evidence = Path(args.evidence)
        self.evidence.mkdir(parents=True, exist_ok=True)
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        self.save()

    def save(self):
        (self.evidence / "report.json").write_text(json.dumps(self.state, indent=2) + "\n")

    def api(self, method, path, body=None, expected=200):
        headers = {"X-Cellbox-Client-ID": self.client}
        if body is not None:
            body = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(self.args.api.rstrip("/") + path, data=body, method=method, headers=headers)
        try:
            response = self.opener.open(request, timeout=40)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            data = response.read()
            if response.status != expected:
                raise RuntimeError(f"{method} {path}: HTTP {response.status}, expected {expected}: {data.decode()[:2000]}")
            return json.loads(data) if data else None

    def wait(self, operation):
        print("operation", operation["kind"], operation["id"], flush=True)
        deadline = time.monotonic() + 600
        while operation["status"] in ("queued", "running", "cancelling"):
            if time.monotonic() > deadline:
                raise TimeoutError("operation still pending: " + operation["id"])
            operation = self.api("GET", "/v1/operations/" + operation["id"] + "?waitMs=1000")
        if operation["status"] != "succeeded":
            raise RuntimeError(json.dumps(operation))
        return operation

    def kube(self, *arguments, resource=None):
        command = ["kubectl", "--context", self.args.context, "-n", self.args.namespace, *arguments]
        result = subprocess.run(command, input=json.dumps(resource) if resource else None,
                                text=True, capture_output=True, timeout=45, check=True)
        return result.stdout

    def runtime(self):
        resource = json.loads(self.kube("get", "cellboxes", "cellbox-" + self.name, "-o", "json"))
        service = json.loads(self.kube("get", "service", "cellbox-" + self.name, "-o", "json"))
        return {"phase": resource["status"]["phase"], "podUID": resource["status"].get("podUID"),
                "cycle": resource["status"]["cycle"], "snapshot": resource["status"].get("snapshot"),
                "resumePath": resource["status"].get("resumePath"),
                "serviceUID": service["metadata"]["uid"], "clusterIP": service["spec"]["clusterIP"]}

    def box(self):
        return self.api("GET", "/v1/boxes/" + self.name)

    def app(self, path, method="GET"):
        target = urllib.parse.urlsplit(self.args.ingress)
        connection_type = http.client.HTTPSConnection if target.scheme == "https" else http.client.HTTPConnection
        connection = connection_type(target.hostname, target.port, timeout=150)
        started = time.monotonic()
        try:
            connection.request(method, path, body=b"validation" if method == "POST" else None,
                               headers={"Host": self.state["host"]})
            response = connection.getresponse()
            body = response.read()
            elapsed = (time.monotonic() - started) * 1000
            if response.status != 200:
                raise RuntimeError(f"Ingress {method} {path}: HTTP {response.status}: {body.decode()[:2000]}")
            return body, elapsed
        finally:
            connection.close()

    def wait_idle(self):
        deadline = time.monotonic() + 150
        previous = None
        while time.monotonic() < deadline:
            box = self.box()
            if box["phase"] != previous:
                print("box phase", box["phase"], flush=True)
                previous = box["phase"]
            if box["phase"] == "suspended" and not box.get("operationId"):
                runtime = self.runtime()
                assert not runtime["podUID"] and runtime["snapshot"], runtime
                endpoints = json.loads(self.kube("get", "endpointslices", "-l",
                    "kubernetes.io/service-name=cellbox-" + self.name, "-o", "json"))
                assert not any(e.get("conditions", {}).get("ready") for s in endpoints["items"] for e in (s.get("endpoints") or [])), endpoints
                return box, runtime
            if box["phase"] == "failed":
                raise RuntimeError(json.dumps(box))
            time.sleep(1)
        raise TimeoutError("Box did not automatically checkpoint: " + json.dumps(self.box()))

    def run(self):
        template = self.api("POST", "/v1/templates", {"name": self.name, "description": "Traefik HTTP checkpoint/resume validation"}, 201)
        self.state["templateId"] = template["id"]
        self.save()
        command = "docker run --rm -e COUNTER_MEMORY_MIB=8 -e COUNTER_MARKER_PATH=/home/agent/workspace/marker -w /home/agent/workspace " + shlex.quote(self.args.image)
        build = self.wait(self.api("POST", "/v1/templates/" + template["id"] + "/revisions",
            {"userVersion": "v1", "baseImage": self.args.image, "profileId": self.args.profile, "runCommand": command}, 202))
        self.state["revisionId"] = build["result"]["revisionId"]
        self.save()
        self.wait(self.api("POST", "/v1/boxes", {"id": self.name, "ownerKey": self.client,
            "revisionId": self.state["revisionId"], "httpService": {"port": 8000, "idleTimeoutSeconds": 10}}, 202))
        self.state["homeVolumeId"] = self.box().get("homeVolumeId")
        self.save()
        middleware = {"apiVersion": "traefik.io/v1alpha1", "kind": "Middleware", "metadata": {"name": self.name, "namespace": self.args.namespace},
            "spec": {"plugin": {"cellbox": {"cellboxURL": f"http://cellbox-api.{self.args.namespace}:8090", "clientID": self.client,
                "boxID": self.name, "wakeTimeout": "120s", "controlTimeout": "5s"}}}}
        ingress = {"apiVersion": "networking.k8s.io/v1", "kind": "Ingress", "metadata": {"name": self.name, "namespace": self.args.namespace,
            "annotations": {"traefik.ingress.kubernetes.io/router.middlewares": f"{self.args.namespace}-{self.name}@kubernetescrd"}},
            "spec": {"ingressClassName": "traefik", "rules": [{"host": self.state["host"], "http": {"paths": [{"path": "/", "pathType": "Prefix",
                "backend": {"service": {"name": "cellbox-" + self.name, "port": {"number": 8000}}}}]}}]}}
        print(self.kube("apply", "-f", "-", resource={"apiVersion": "v1", "kind": "List", "items": [middleware, ingress]}), flush=True)
        # Initial route publication may lag; these GET probes have no mutation.
        for attempt in range(40):
            try:
                initial = json.loads(self.app("/state")[0])
                break
            except (OSError, RuntimeError):
                if attempt == 39:
                    raise
                time.sleep(1)
        identity = {key: initial[key] for key in ("instance", "started", "pid", "memory_sha256", "memory_bytes")}
        self.state["initial"] = initial
        count = int(self.app("/increment", "POST")[0])
        assert count == 1
        for cycle in range(3):
            suspended, before = self.wait_idle()
            workers = 8 if cycle == 0 else 1
            # A business POST is sent once. Any lost response fails the run.
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
                replies = list(pool.map(lambda _: self.app("/increment", "POST"), range(workers)))
            assert sorted(int(body) for body, _ in replies) == list(range(count + 1, count + workers + 1)), replies
            count += workers
            current = json.loads(self.app("/state")[0])
            after = self.runtime()
            assert {key: current[key] for key in identity} == identity, (initial, current)
            assert current["count"] == count, current
            assert self.box()["generation"] == suspended["generation"] + 1
            assert after["cycle"] == before["cycle"] + 1 and after["podUID"]
            assert (after["serviceUID"], after["clusterIP"]) == (before["serviceUID"], before["clusterIP"])
            self.state["cycles"].append({"cycle": cycle + 1, "before": before, "after": after, "state": current,
                                          "firstRequestMs": [round(elapsed, 2) for _, elapsed in replies]})
            self.save()
            print(f"PASS cycle {cycle + 1}: {workers} POST(s), count={count}, memory and process identity preserved, latencies_ms=" +
                  str(self.state["cycles"][-1]["firstRequestMs"]), flush=True)
        # Admission accounting must also block a manual suspend while active.
        path = "/v1/boxes/" + self.name + "/http-requests/held-validation"
        self.api("PUT", path, expected=204)
        try:
            time.sleep(12)
            assert self.box()["phase"] == "running"
            self.api("POST", "/v1/boxes/" + self.name + ":suspend", expected=409)
        finally:
            self.api("DELETE", path, expected=204)
            self.api("DELETE", path, expected=204)
        self.wait_idle()
        self.state["activeRequestFence"] = {"heldSeconds": 12, "manualSuspendStatus": 409, "idempotentRelease": True}
        self.state["status"] = "passed"
        self.save()
        print("PASS active-request suspension fence and idempotent release", flush=True)

    def cleanup(self):
        for kind in ("ingress", "middleware"):
            self.kube("delete", kind, self.name, "--ignore-not-found=true", "--wait=true", "--timeout=30s")
        try:
            box = self.box()
        except RuntimeError as error:
            if "HTTP 404" not in str(error):
                raise
            return
        # Every validation request has ended before cleanup; release only this
        # run's known isolated Box requests if a prior control response was lost.
        status = self.api("GET", "/v1/boxes/" + self.name + "/http-service")
        for request in status["requests"]:
            self.api("DELETE", "/v1/boxes/" + self.name + "/http-requests/" + request["id"], expected=204)
        if box["phase"] != "deleted":
            self.wait(self.api("POST", "/v1/boxes/" + self.name + ":destroy", expected=202))
        volume = box.get("homeVolumeId")
        if volume:
            self.wait(self.api("DELETE", "/v1/volumes/" + volume, expected=202))
        self.state["cleanup"] = "box, volume, Ingress and middleware removed; immutable template revision retained"
        self.save()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api", required=True)
    parser.add_argument("--ingress", required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--profile", default="oss-check")
    parser.add_argument("--context", default="k3s")
    parser.add_argument("--namespace", default="cell-box")
    parser.add_argument("--evidence", required=True)
    parser.add_argument("--keep", action="store_true")
    args = parser.parse_args()
    validation = Validation(args)
    try:
        validation.run()
    except Exception as error:
        validation.state["status"] = "failed"
        validation.state["error"] = str(error)
        validation.save()
        raise
    finally:
        if not args.keep:
            validation.cleanup()


if __name__ == "__main__":
    main()
