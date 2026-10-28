package guestapi

import (
	"bytes"
	"debug/elf"
)

const ProtocolVersion = "2"

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
