#!/bin/bash
# Import test/counter's ordinary image and verify it through the public REST API.
# Saves evidence under EVIDENCE_DIR and removes all test resources on exit.
set -euo pipefail
[[ $# == 3 ]] || { echo "Usage: $0 API_URL PROFILE_ID SOURCE_IMAGE" >&2; exit 2; }
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
boxes, archives = [], []
template_id = revision_id = None

def save(name, value):
    (evidence / (name + '.json')).write_text(json.dumps(value, indent=2) + '\n')

def call(method, path, data=None):
    headers = {}
    if os.environ.get('CELLBOX_CLIENT_ID'):
        headers['X-Cellbox-Client-ID'] = os.environ['CELLBOX_CLIENT_ID']
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
    boxes.append(op['targetId'])
    save('report', report)
    return wait(op)['targetId']

def app(box, endpoint):
    return call('GET', f'/v1/boxes/{box}/services/8000/{endpoint}')

def ready(box):
    last = None
    for _ in range(60):
        try:
            assert app(box, 'healthz') == b'ok\n'
            return app(box, 'state')
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
    crs = k('get', 'cellboxes', '-l', 'cellbox.jingyugao.github.io/box-id=' + box)['items']
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
    template = call('POST', '/v1/templates', {'name': 'counter-import-' + str(uuid.uuid4()), 'description': 'counter import and revision e2e'})
    template_id = template['id']
    build_request = {'userVersion': 'v1', 'baseImage': source, 'profileId': profile, 'runCommand': run}
    op = call('POST', f'/v1/templates/{template_id}/revisions', build_request)
    build = wait(op)
    revision_id = build['result']['revisionId']
    revision = call('GET', '/v1/template-revisions/' + revision_id)
    report['revisionId'] = revision_id
    report['image'] = revision['image']
    save('revision', revision)
    assert '@sha256:' in revision['resolvedBaseImage'] and '@sha256:' in revision['image']
    expected_digest = os.environ.get('COUNTER_SOURCE_DIGEST')
    if expected_digest:
        assert revision['resolvedBaseImage'].endswith('@' + expected_digest)
    assert revision['baseImage'] == source and revision['runCommand'] == run and revision['profileId'] == profile
    print('PASS: registry base resolved, immutable template revision built', flush=True)

    box = create('/v1/boxes', {'revisionId': revision_id, 'ownerKey': 'counter-import-' + str(uuid.uuid4())}, 'boxId')
    ready(box)
    for i in range(1, 11):
        assert app(box, 'increment') == f'{i}\n'.encode()
    before = app(box, 'state')
    save('state-before-suspend', before)
    assert before['count'] == 10 and before['memory_bytes'] == 32 << 20
    assert call('GET', f'/v1/boxes/{box}/files?path=marker') == before['instance'].encode()
    proof = json.dumps(before, sort_keys=True).encode()
    call('PUT', f'/v1/boxes/{box}/files?path=counter-proof.json', proof)
    b = call('GET', '/v1/boxes/' + box)
    execution = wait(call('POST', f'/v1/boxes/{box}/execs', {'argv': ['/server', 'get', 'http://127.0.0.1:8000/state'], 'expectedGeneration': b['generation']}))
    result = call('GET', '/v1/execs/' + execution['result']['execId'])['result']
    assert result['exitCode'] == 0 and json.loads(result['stdout'])['instance'] == before['instance']
    print('PASS: counter reached 10; anonymous HTTP access, exec and workspace files verified', flush=True)

    runtime_before = runtime(box)
    assert b['capabilities']['suspend'] == 'same-node-checkpoint'
    wait(call('POST', f'/v1/boxes/{box}:suspend'))
    assert call('GET', '/v1/boxes/' + box)['phase'] == 'suspended'
    wait(call('POST', f'/v1/boxes/{box}:resume'))
    after = ready(box)
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
    assert app(box, 'increment') == b'11\n'
    print('PASS: suspend/resume preserved count, process identity and 32 MiB of memory; counter continues at 11', flush=True)

    wait(call('POST', f'/v1/boxes/{box}:suspend'))
    archive = wait(call('POST', f'/v1/boxes/{box}/archives'))['result']['archiveId']
    archives.append(archive)
    report['archiveId'] = archive
    # Capture leaves the source suspended; restore creates a separate staged sandbox.
    restored = create('/v1/boxes:restore', {'revisionId': revision_id, 'ownerKey': 'counter-restore-' + str(uuid.uuid4()), 'archiveId': archive}, 'restoredBoxId')
    restored_box = call('GET', '/v1/boxes/' + restored)
    assert restored_box['phase'] == 'staged' and restored_box['revisionId'] == revision_id
    assert call('GET', f'/v1/boxes/{restored}/files?path=marker') == before['instance'].encode()
    assert call('GET', f'/v1/boxes/{restored}/files?path=counter-proof.json') == proof
    wait(call('POST', f'/v1/boxes/{restored}:activate'))
    restored_state = ready(restored)
    assert restored_state['count'] == 0 and restored_state['instance'] != before['instance']
    assert call('GET', f'/v1/boxes/{restored}/files?path=counter-proof.json') == proof
    assert app(restored, 'increment') == b'1\n'
    save('restored-state', app(restored, 'state'))
    print('PASS: archive restored workspace and imported image; activation starts a fresh counter', flush=True)
    report['status'] = 'passed'
    print(json.dumps(report, indent=2), flush=True)
finally:
    save('report', report)
    failed = False
    for box_id in reversed(boxes):
        try:
            wait(call('POST', f'/v1/boxes/{box_id}:destroy'))
        except Exception as error:
            failed = True
            print(f'Could not destroy test sandbox {box_id}: {error}', file=sys.stderr)
    for archive in archives:
        try:
            call('DELETE', '/v1/archives/' + archive)
        except Exception as error:
            failed = True
            print(f'Could not delete test archive {archive}: {error}', file=sys.stderr)
    if template_id and not revision_id:
        try:
            call('DELETE', '/v1/templates/' + template_id)
        except Exception as error:
            failed = True
            print(f'Could not delete empty test template {template_id}: {error}', file=sys.stderr)
    if failed:
        raise RuntimeError('Test resource cleanup incomplete; inspect the IDs above')
if template_id:
    print(f'Preserved template {template_id} and revision {revision_id}; phase one does not delete built revisions', flush=True)
PY
