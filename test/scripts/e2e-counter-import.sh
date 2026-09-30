#!/bin/bash
# Import test/counter's ordinary image and verify it through the public REST API.
# Keeps its source sandbox suspended and restored sandbox running for inspection.
set -euo pipefail
[[ $# == 3 ]] || { echo "Usage: CELLBOX_CLIENT_TOKEN=... $0 API_URL PROFILE_ID SOURCE_IMAGE" >&2; exit 2; }
: "${CELLBOX_CLIENT_TOKEN:?Set CELLBOX_CLIENT_TOKEN}"
uv run python - "$@" <<'PY'
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

base, profile, source = sys.argv[1:]
base = base.rstrip('/')
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
evidence = Path(os.environ.get('EVIDENCE_DIR', 'tmp/validation/counter-import'))
evidence.mkdir(parents=True, exist_ok=True)
report = {'source': source, 'profileId': profile}

def save(name, value):
    (evidence / (name + '.json')).write_text(json.dumps(value, indent=2) + '\n')

def call(method, path, data=None, token=None, key=None):
    headers = {'Authorization': 'Bearer ' + (token or os.environ['CELLBOX_CLIENT_TOKEN'])}
    headers['Idempotency-Key'] = key or str(uuid.uuid4())
    if isinstance(data, bytes):
        headers['Content-Type'] = 'application/octet-stream'
    elif data is not None:
        headers['Content-Type'] = 'application/json'
        data = json.dumps(data).encode()
    request = urllib.request.Request(base + path, method=method, data=data, headers=headers)
    try:
        with opener.open(request, timeout=60) as response:
            payload = response.read()
            if 'application/json' in response.headers.get('Content-Type', ''):
                return json.loads(payload)
            return payload
    except urllib.error.HTTPError as error:
        raise RuntimeError(f'{method} {path}: HTTP {error.code}: {error.read().decode()}') from None

def wait(op):
    print(f"Waiting: {op['kind']} {op['id']}", flush=True)
    deadline = time.monotonic() + 900
    while op['status'] in ('queued', 'running'):
        if time.monotonic() >= deadline:
            raise RuntimeError('Operation timed out: ' + op['id'])
        time.sleep(1)
        op = call('GET', '/v1/operations/' + op['id'])
    if op['status'] != 'succeeded':
        raise RuntimeError('Operation failed: ' + json.dumps(op))
    return op

def create(path, body, name):
    op = call('POST', path, body)
    report[name] = op['targetId']
    save('report', report)
    return wait(op)['targetId']

def route_for(box):
    route = call('POST', '/v1/routes', {'boxId': box, 'port': 8000})
    grant = call('POST', f"/v1/routes/{route['id']}/grants", {'subject': 'counter-import-e2e', 'ttlSeconds': 3600})
    return route['id'], grant['token']

def app(route, token, endpoint):
    return call('GET', f'/s/{route}/{endpoint}', token=token)

def ready(route, token):
    last = None
    for _ in range(60):
        try:
            assert app(route, token, 'healthz') == b'ok\n'
            return app(route, token, 'state')
        except RuntimeError as error:
            last = error
            time.sleep(1)
    raise RuntimeError('Counter did not become ready: ' + str(last))

def runtime(box):
    context = os.environ.get('CELLBOX_K8S_CONTEXT')
    if not context:
        return None
    namespace = os.environ.get('CELLBOX_K8S_NAMESPACE', 'cell-box')
    def k(*args):
        return json.loads(subprocess.check_output(['kubectl', '--context', context, '-n', namespace, *args, '-o', 'json']))
    crs = k('get', 'cellboxes', '-l', 'cellbox.local/box-id=' + box)['items']
    assert len(crs) == 1
    cr = crs[0]
    pod = k('get', 'pod', cr['status']['podName'])
    svc = k('get', 'service', cr['metadata']['name'])
    return {'podUID': pod['metadata']['uid'], 'serviceUID': svc['metadata']['uid'],
            'clusterIP': svc['spec']['clusterIP'], 'restartCounts': [x['restartCount'] for x in pod['status']['containerStatuses']]}

try:
    configured = next(p for p in call('GET', '/v1/profiles') if p['id'] == profile)
    workspace = configured['workspace']
    run = 'docker run --rm -e ' + shlex.quote('COUNTER_MARKER_PATH=' + workspace + '/marker')
    run += ' -w ' + shlex.quote(workspace) + ' -p 127.0.0.1:18000:8000 ' + shlex.quote(source)
    body = {'url': source, 'runCommand': run}
    key = str(uuid.uuid4())
    op = call('POST', '/v1/images:import', body, key=key)
    assert call('POST', '/v1/images:import', body, key=key)['id'] == op['id']
    imported_id = wait(op)['result']['importedImageId']
    imported = call('GET', '/v1/images/' + imported_id)
    report['importedImageId'] = imported_id
    report['image'] = imported['image']
    save('imported', imported)
    assert '@sha256:' in imported['resolvedSource'] and '@sha256:' in imported['image']
    expected_digest = os.environ.get('COUNTER_SOURCE_DIGEST')
    if expected_digest:
        assert imported['resolvedSource'].endswith('@' + expected_digest)
    assert imported['command'] == ['/server'] and imported['workingDir'] == workspace
    assert imported['env']['COUNTER_MARKER_PATH'] == workspace + '/marker' and imported['ports'] == [8000]
    print('PASS: registry image imported, entrypoint retained, guest injected, idempotency verified', flush=True)

    box = create('/v1/boxes', {'profileId': profile, 'ownerKey': 'counter-import-' + str(uuid.uuid4()), 'importedImageId': imported_id}, 'boxId')
    route, grant = route_for(box)
    report['sourceRouteId'] = route
    ready(route, grant)
    try:
        app(route, 'invalid-grant', 'state')
    except RuntimeError as error:
        assert 'HTTP 401' in str(error)
    else:
        raise AssertionError('Unauthenticated counter request succeeded')
    for i in range(1, 11):
        assert app(route, grant, 'increment') == f'{i}\n'.encode()
    before = app(route, grant, 'state')
    save('state-before-suspend', before)
    assert before['count'] == 10 and before['memory_bytes'] == 32 << 20
    assert call('GET', f'/v1/boxes/{box}/files?path=marker') == before['instance'].encode()
    proof = json.dumps(before, sort_keys=True).encode()
    call('PUT', f'/v1/boxes/{box}/files?path=counter-proof.json', proof)
    b = call('GET', '/v1/boxes/' + box)
    assert b['capabilities']['protectedTools'] is False
    execution = wait(call('POST', f'/v1/boxes/{box}/execs', {'argv': ['/server', 'get', 'http://127.0.0.1:8000/state'], 'expectedGeneration': b['generation']}))
    result = call('GET', '/v1/execs/' + execution['result']['execId'])['result']
    assert result['exitCode'] == 0 and json.loads(result['stdout'])['instance'] == before['instance']
    print('PASS: counter reached 10; HTTP authorization, exec and workspace files verified', flush=True)

    runtime_before = runtime(box)
    assert b['capabilities']['suspend'] == 'same-node-checkpoint'
    wait(call('POST', f'/v1/boxes/{box}:suspend'))
    assert call('GET', '/v1/boxes/' + box)['phase'] == 'suspended'
    wait(call('POST', f'/v1/boxes/{box}:resume'))
    after = ready(route, grant)
    save('state-after-resume', after)
    for field in ('count', 'started', 'instance', 'memory_bytes', 'memory_sha256', 'marker'):
        assert before[field] == after[field], field
    runtime_after = runtime(box)
    if runtime_before is not None:
        save('runtime-before', runtime_before)
        save('runtime-after', runtime_after)
        assert runtime_before['podUID'] != runtime_after['podUID']
        assert runtime_before['serviceUID'] == runtime_after['serviceUID']
        assert runtime_before['clusterIP'] == runtime_after['clusterIP']
        assert all(x == 0 for x in runtime_after['restartCounts'])
    assert app(route, grant, 'increment') == b'11\n'
    print('PASS: suspend/resume preserved count, process identity and 32 MiB of memory; counter continues at 11', flush=True)

    archive = wait(call('POST', f'/v1/boxes/{box}/archives'))['result']['archiveId']
    report['archiveId'] = archive
    # Keep the source checkpoint, freeing running resources for archive restore.
    wait(call('POST', f'/v1/boxes/{box}:suspend'))
    restored = create('/v1/boxes:restore', {'profileId': profile, 'ownerKey': 'counter-restore-' + str(uuid.uuid4()), 'archiveId': archive}, 'restoredBoxId')
    restored_box = call('GET', '/v1/boxes/' + restored)
    assert restored_box['phase'] == 'staged' and restored_box['importedImageId'] == imported_id
    assert call('GET', f'/v1/boxes/{restored}/files?path=marker') == before['instance'].encode()
    assert call('GET', f'/v1/boxes/{restored}/files?path=counter-proof.json') == proof
    wait(call('POST', f'/v1/boxes/{restored}:activate'))
    restored_route, restored_grant = route_for(restored)
    report['routeId'] = restored_route
    restored_state = ready(restored_route, restored_grant)
    assert restored_state['count'] == 0 and restored_state['instance'] != before['instance']
    assert call('GET', f'/v1/boxes/{restored}/files?path=counter-proof.json') == proof
    assert app(restored_route, restored_grant, 'increment') == b'1\n'
    save('restored-state', app(restored_route, restored_grant, 'state'))
    print('PASS: archive restored workspace and imported image; activation starts a fresh counter', flush=True)
    report['status'] = 'passed'
    print(json.dumps(report, indent=2), flush=True)
finally:
    save('report', report)
PY
