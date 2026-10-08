package node

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	api "cellbox.local/cellbox/api/v1alpha1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestDiagnosticTailPreservesLastBytesWithoutBlockingWriters(t *testing.T) {
	tail := &diagnosticTail{limit: 8}
	for _, data := range []string{"12345", "67890", "abcdefghijkl"} {
		n, err := tail.Write([]byte(data))
		if n != len(data) || err != nil {
			t.Fatal("diagnostic output blocked")
		}
	}
	if !bytes.Equal(tail.data, []byte("efghijkl")) {
		t.Fatalf("tail=%q", tail.data)
	}
}

func TestFailureEvidenceSurvivesRepeatedCleanupAndMissingPod(t *testing.T) {
	b := &Backend{Base: t.TempDir()}
	w := &api.ResumablePod{ObjectMeta: meta.ObjectMeta{UID: "owner"}, Status: api.Status{Cycle: 1, Message: "startup failed"}}
	if err := b.CaptureFailure(context.Background(), w, nil); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(b.Base, "diagnostics", "owner", "unassigned-cycle-1", "failure.json")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	w.Status.Message = "cleanup retry"
	if err := b.CaptureFailure(context.Background(), w, nil); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("first failure evidence overwritten", err)
	}
	st, err := os.Stat(path)
	if err != nil || st.Mode().Perm() != 0600 {
		t.Fatal("failure evidence must be private", err)
	}
}
