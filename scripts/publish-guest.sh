#!/bin/sh
# Publish an operator-approved Guest via the same read-only shared mount as Harness releases.
set -eu
if [ "$#" -ne 3 ]; then
  echo "usage: publish-guest.sh CELLBOX_CONTAINER_AGENT ED25519_PRIVATE_PEM SHARED_ROOT" >&2
  exit 2
fi
umask 022
root=$3/guest
mkdir -p "$root/versions"
temp=$(mktemp -d "$root/.publish-XXXXXXXX")
trap 'rm -rf "$temp"' EXIT
cp "$1" "$temp/binary"
digest=$(sha256sum "$temp/binary" | cut -d ' ' -f 1)
printf 'cellbox-worker-v1:%s' "$digest" > "$temp/message"
openssl pkeyutl -sign -rawin -inkey "$2" -in "$temp/message" -out "$temp/signature"
signature=$(openssl base64 -A -in "$temp/signature")
printf '{"sha256":"%s","signature":"%s"}\n' "$digest" "$signature" > "$temp/current"
chmod 0444 "$temp/binary" "$temp/current"
mv -f "$temp/binary" "$root/versions/$digest"
mv -f "$temp/current" "$root/current.json"
printf 'Guest %s\nCELLBOX_GUEST_UPDATE_PUBLIC_KEY=' "$digest"
openssl pkey -in "$2" -pubout -outform DER | openssl base64 -A
printf '\n'
