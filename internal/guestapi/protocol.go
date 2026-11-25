package guestapi

import (
	"bytes"
	"crypto/ed25519"
	"crypto/x509"
	"debug/elf"
	"encoding/base64"
	"errors"
)

const ProtocolVersion = "2"

// Only the operator profile may supply these settings. Imported image values are discarded.
const GuestUpdateKeyEnv = "CELLBOX_GUEST_UPDATE_PUBLIC_KEY"
const ReleaseToolsEnv = "CELLBOX_RELEASE_TOOLS"
const WorkloadReleasesEnv = "CELLBOX_WORKLOAD_RELEASES"

// GuestUpdateKey decodes the operator's base64 DER SubjectPublicKeyInfo.
func GuestUpdateKey(encoded string) (ed25519.PublicKey, error) {
	der, err := base64.StdEncoding.DecodeString(encoded)
	if err == nil {
		parsed, err := x509.ParsePKIXPublicKey(der)
		if key, ok := parsed.(ed25519.PublicKey); err == nil && ok {
			return key, nil
		}
	}
	return nil, errors.New("invalid Ed25519 Guest update public key")
}

// ProtocolMarker identifies Guest binaries that can report listening ports.
const ProtocolMarker = "CELLBOX_GUEST_PROTOCOL=2;WAIT_PORTS_LISTENING"

// BinaryProtocolVersion inspects the ELF read-only data without executing the
// binary. An empty version means that the binary uses the legacy protocol.
func BinaryProtocolVersion(path string) (string, error) {
	f, err := elf.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	section := f.Section(".rodata")
	if section == nil {
		return "", nil
	}
	data, err := section.Data()
	if err != nil {
		return "", err
	}
	if bytes.Contains(data, []byte(ProtocolMarker)) {
		return ProtocolVersion, nil
	}
	return "", nil
}
