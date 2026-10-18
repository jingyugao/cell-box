#!/bin/bash
# Against an API with BuildKit enabled. SOURCE_IMAGE must contain sh and sleep.
set -euo pipefail
[[ $# == 3 ]] || { echo "Usage: $0 API_URL PROFILE_ID SOURCE_IMAGE" >&2; exit 2; }
uv run python - "$@" <<'PY'
import json
import os
import shlex
import sys
import time
import urllib.error
import urllib.request
import uuid

base, profile, source = sys.argv[1:]
base = base.rstrip('/')
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
boxes, archives = [], []
template_id = revision_id = None

def call(method, path, data=None):
    headers = {}
    if os.environ.get('CELLBOX_CLIENT_TOKEN'):
        headers['Authorization'] = 'Bearer ' + os.environ['CELLBOX_CLIENT_TOKEN']
    if os.environ.get('CELLBOX_CLIENT_ID'):
        headers['X-Cellbox-Client-ID'] = os.environ['CELLBOX_CLIENT_ID']
    if data is not None:
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
    deadline = time.monotonic() + 900
    while op['status'] in ('queued', 'running'):
        if time.monotonic() >= deadline:
            raise RuntimeError('Operation timed out: ' + op['id'])
        time.sleep(1)
        op = call('GET', '/v1/operations/' + op['id'])
    if op['status'] != 'succeeded':
        raise RuntimeError('Operation failed: ' + json.dumps(op))
    return op

def create(path, body):
    op = call('POST', path, body)
    boxes.append(op['targetId'])
    return wait(op)['targetId']

try:
    configured = next(p for p in call('GET', '/v1/profiles') if p['id'] == profile)
    workspace = configured['workspace']
    workload = 'printf "%s" "$IMPORT_MESSAGE" > imported-proof.txt; while true; do sleep 1; done'
    run = 'docker run --rm -e IMPORT_MESSAGE=image-import-ok -w ' + shlex.quote(workspace) + ' -p 127.0.0.1:18080:8080 '
    run += shlex.quote(source) + ' sh -c ' + shlex.quote(workload)
    build_command = 'mkdir -p /opt/product && printf "%s" "platform-build-ok" > /opt/product/cellbox-build-proof.txt'
    template = call('POST', '/v1/templates', {'name': 'image-import-e2e-' + str(uuid.uuid4()), 'description': 'e2e image import and revision build'})
    template_id = template['id']
    build = call('POST', f'/v1/templates/{template_id}/revisions', {
        'userVersion': 'v1', 'baseImage': source, 'profileId': profile,
        'runCommand': run, 'buildCommand': build_command,
    })
    build = wait(build)
    revision_id = build['result']['revisionId']
    revision = call('GET', '/v1/template-revisions/' + revision_id)
    assert '@sha256:' in revision['resolvedBaseImage'] and '@sha256:' in revision['image']
    assert revision['baseImage'] == source and revision['buildCommand'] == build_command
    assert revision['version'] == 'v1-r1' and revision['profileId'] == profile
    logs = call('GET', f"/v1/operations/{build['id']}/logs?offset=0&limit=65536")
    assert 'Published image:' in logs['text'] and logs['eof']
    print('PASS: source tag resolved, BuildKit published immutable revision, build logs retained', flush=True)

    box_id = create('/v1/boxes', {'revisionId': revision_id, 'ownerKey': 'template-archive-e2e-' + str(uuid.uuid4())})
    box = call('GET', '/v1/boxes/' + box_id)
    if box['capabilities']['archives'] != 'stopped-workspace' or box['capabilities']['suspend'] != 'same-node-checkpoint':
        raise RuntimeError('archive E2E requires a persistent-HOME Kubernetes profile with checkpoint support')
    for _ in range(30):
        try:
            if call('GET', f'/v1/boxes/{box_id}/files?path=imported-proof.txt') == b'image-import-ok':
                break
        except RuntimeError:
            pass
        time.sleep(1)
    else:
        raise RuntimeError('Imported startup command did not create its proof file')
    box = call('GET', '/v1/boxes/' + box_id)
    assert box['image'] == revision['image'] and box['revisionId'] == revision_id
    execution = wait(call('POST', f'/v1/boxes/{box_id}/execs', {'argv': ['/bin/cat', workspace + '/imported-proof.txt'], 'expectedGeneration': box['generation']}))
    result = call('GET', '/v1/execs/' + execution['result']['execId'])['result']
    assert result['exitCode'] == 0 and result['stdout'] == 'image-import-ok'
    built_proof = wait(call('POST', f'/v1/boxes/{box_id}/execs', {'argv': ['/bin/cat', '/opt/product/cellbox-build-proof.txt'], 'expectedGeneration': box['generation']}))
    result = call('GET', '/v1/execs/' + built_proof['result']['execId'])['result']
    assert result['exitCode'] == 0 and result['stdout'] == 'platform-build-ok'
    print('PASS: buildCommand packaged platform content into the imported image', flush=True)
    print('PASS: imported command runs; exec and file APIs work', flush=True)

    if box['capabilities']['suspend'] == 'same-node-checkpoint':
        wait(call('POST', f'/v1/boxes/{box_id}:suspend'))
        assert call('GET', '/v1/boxes/' + box_id)['phase'] == 'suspended'
        wait(call('POST', f'/v1/boxes/{box_id}:resume'))
        assert call('GET', f'/v1/boxes/{box_id}/files?path=imported-proof.txt') == b'image-import-ok'
        print('PASS: imported sandbox suspends and resumes', flush=True)

    wait(call('POST', f'/v1/boxes/{box_id}:suspend'))
    archive = wait(call('POST', f'/v1/boxes/{box_id}/archives'))['result']['archiveId']
    archives.append(archive)
    # Free the source's resources so restoration also works on a small test node.
    wait(call('POST', f'/v1/boxes/{box_id}:destroy'))
    boxes.remove(box_id)
    restored_id = create('/v1/boxes:restore', {'revisionId': revision_id, 'ownerKey': 'image-import-restore-' + str(uuid.uuid4()), 'archiveId': archive})
    restored = call('GET', '/v1/boxes/' + restored_id)
    assert restored['phase'] == 'staged' and restored['image'] == revision['image']
    assert restored['revisionId'] == revision_id
    assert call('GET', f'/v1/boxes/{restored_id}/files?path=imported-proof.txt') == b'image-import-ok'
    wait(call('POST', f'/v1/boxes/{restored_id}:activate'))
    restored = call('GET', '/v1/boxes/' + restored_id)
    built_proof = wait(call('POST', f'/v1/boxes/{restored_id}/execs', {'argv': ['/bin/cat', '/opt/product/cellbox-build-proof.txt'], 'expectedGeneration': restored['generation']}))
    result = call('GET', '/v1/execs/' + built_proof['result']['execId'])['result']
    assert result['exitCode'] == 0 and result['stdout'] == 'platform-build-ok'
    print('PASS: archive restore reuses imported image and activation succeeds', flush=True)
finally:
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
print('PASS: template build and archive E2E completed; test sandboxes and archives removed')
PY
