package main

import (
	"io"
	"os"
	"testing"

	"github.com/jingyugao/cell-box/internal/guestapi"
)

func TestProtocolVersionCommand(t *testing.T) {
	oldArgs, oldStdout := os.Args, os.Stdout
	t.Cleanup(func() {
		os.Args, os.Stdout = oldArgs, oldStdout
	})
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Args = []string{"cellbox-container-agent", "protocol-version"}
	os.Stdout = writer
	if err := mainErr(); err != nil {
		_ = reader.Close()
		_ = writer.Close()
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
	if string(output) != guestapi.ProtocolMarker+"\n" {
		t.Fatalf("protocol-version output = %q, want marker %q", output, guestapi.ProtocolMarker)
	}
}
