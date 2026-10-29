package guestapi

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestBinaryProtocolVersion(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("guest binaries use Linux ELF")
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	version, err := BinaryProtocolVersion(executable)
	if err != nil {
		t.Fatal(err)
	}
	if version != ProtocolVersion {
		t.Fatalf("test binary protocol = %q, want %q", version, ProtocolVersion)
	}

	image, err := os.ReadFile(executable)
	if err != nil {
		t.Fatal(err)
	}
	marker := []byte(ProtocolMarker)
	if !bytes.Contains(image, marker) {
		t.Fatal("test executable does not contain protocol marker fixture")
	}
	legacyImage := bytes.ReplaceAll(image, marker, bytes.Repeat([]byte{'x'}, len(marker)))
	legacyPath := filepath.Join(t.TempDir(), "legacy-guest")
	if err := os.WriteFile(legacyPath, legacyImage, 0700); err != nil {
		t.Fatal(err)
	}
	version, err = BinaryProtocolVersion(legacyPath)
	if err != nil {
		t.Fatal(err)
	}
	if version != "" {
		t.Fatalf("legacy ELF protocol = %q, want empty", version)
	}
}
