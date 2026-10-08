package adapter

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRuntimeLogsAreIsolatedAndDoNotAlterSubcommandArguments(t *testing.T) {
	a := Adapter{Base: t.TempDir()}
	for _, id := range []string{strings.Repeat("a", 64), strings.Repeat("b", 64)} {
		args, err := a.DiagnosticArgs([]string{"--debug-log=/shared/log", "--panic-log", "/shared/panic", "restore", "--image-path=/snapshot", id})
		if err != nil {
			t.Fatal(err)
		}
		dir := filepath.Join(a.Base, "logs", id)
		if !strings.HasPrefix(args[0], "--debug-log="+dir+"/") || !strings.HasPrefix(args[1], "--panic-log="+dir+"/") || strings.Contains(strings.Join(args, " "), "/shared/") {
			t.Fatalf("unsafe arguments: %v", args)
		}
		if args[2] != "restore" || args[3] != "--image-path=/snapshot" || args[4] != id {
			t.Fatalf("subcommand changed: %v", args)
		}
		st, err := os.Stat(dir)
		if err != nil || st.Mode().Perm() != 0700 {
			t.Fatalf("private log directory: %v %v", st, err)
		}
	}
	args, err := a.DiagnosticArgs([]string{"exec", "--debug-log=guest-argument", strings.Repeat("a", 64)})
	if err != nil || args[3] != "--debug-log=guest-argument" {
		t.Fatalf("subcommand argument changed: %v %v", args, err)
	}
	args, err = a.DiagnosticArgs([]string{"--version"})
	if err != nil || len(args) != 1 || args[0] != "--version" {
		t.Fatalf("version invocation changed: %v", args)
	}
}
