// Command adapter-runsc exercises the production runsc argument rewrite at
// the same boundary used by the node adapter, then replaces itself with runsc.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/adapter"
	"cellbox.local/cellbox/internal/homevolume"
	"cellbox.local/cellbox/internal/node"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func main() {
	if len(os.Args) < 2 {
		fatal("expected runsc, prepare, set-policy, ticket, or checkpoint command")
	}
	switch os.Args[1] {
	case "runsc":
		run()
	case "prepare":
		prepare(os.Args[2:])
	case "set-policy":
		setPolicy(os.Args[2:])
	case "ticket":
		ticket(os.Args[2:])
	case "checkpoint":
		checkpoint(os.Args[2:])
	case "register-warm":
		registerWarm(os.Args[2:])
	case "claim-warm":
		claimWarm(os.Args[2:])
	case "retire":
		retire(os.Args[2:])
	case "verify-checkpoint":
		verifyCheckpoint(os.Args[2:])
	default:
		fatal("unknown command: " + os.Args[1])
	}
}

func run() {
	flags := flag.NewFlagSet("runsc", flag.ExitOnError)
	base := flags.String("base", "", "isolated Cellbox state directory")
	runsc := flags.String("runsc", "", "runsc executable")
	_ = flags.Parse(os.Args[2:])
	args := flags.Args()
	if *base == "" || *runsc == "" || len(args) == 0 {
		fatal("runsc requires --base, --runsc, and runsc arguments")
	}
	args, err := (adapter.Adapter{Base: *base, Runsc: *runsc}).Rewrite(args)
	if err != nil {
		fatal(err.Error())
	}
	if len(args) == 0 {
		return
	}
	if err := syscall.Exec(*runsc, append([]string{*runsc}, args...), os.Environ()); err != nil {
		fatal("exec runsc: " + err.Error())
	}
}

func prepare(args []string) {
	flags := flag.NewFlagSet("prepare", flag.ExitOnError)
	base := flags.String("base", "", "isolated Cellbox state directory")
	owner := flags.String("owner", "", "owner identifier")
	nodeName := flags.String("node", "warm-home-test-node", "node identity")
	spec := flags.String("spec", "warm-home-test-spec", "sandbox spec identity")
	_ = flags.Parse(args)
	identity, err := homevolume.Prepare(*base, *owner, *nodeName, *spec, true)
	check(err)
	printJSON(identity)
}

// setPolicy writes the same root-owned node-local policy that production warm
// registration and restore read. The script invokes this helper through sudo.
func setPolicy(args []string) {
	flags := flag.NewFlagSet("set-policy", flag.ExitOnError)
	base := flags.String("base", "", "isolated Cellbox state directory")
	sandboxMode := flags.String("sandbox-mode", "root", "root or no-root")
	pageLoading := flags.String("page-loading", "eager", "eager or lazy")
	_ = flags.Parse(args)
	if *base == "" {
		fatal("set-policy requires --base")
	}
	if *sandboxMode != "root" && *sandboxMode != "no-root" {
		fatal("--sandbox-mode must be root or no-root")
	}
	if *pageLoading != "eager" && *pageLoading != "lazy" {
		fatal("--page-loading must be eager or lazy")
	}
	check(os.MkdirAll(*base, 0700))
	check(os.Chmod(*base, 0700))
	path := filepath.Join(*base, "restore.json")
	check(node.AtomicJSON(path, node.RestorePolicy{SandboxMode: *sandboxMode, PageLoading: *pageLoading}))
	check(os.Chmod(path, 0600))
}

func ticket(args []string) {
	flags := flag.NewFlagSet("ticket", flag.ExitOnError)
	base := flags.String("base", "", "isolated Cellbox state directory")
	owner := flags.String("owner", "", "owner identifier")
	uid := flags.String("uid", "", "Pod UID and ticket annotation")
	name := flags.String("name", "", "Pod name")
	namespace := flags.String("namespace", "warm-home-test", "Pod namespace")
	snapshot := flags.String("snapshot", "", "node-local snapshot identifier")
	specHash := flags.String("spec", "warm-home-test-spec", "sandbox spec identity")
	noRoot := flags.Bool("no-root", false, "checkpoint was created from a no-root sandbox")
	_ = flags.Parse(args)
	identity, err := homevolume.Verify(*base, *owner, "", *specHash, "")
	check(err)
	t := node.Ticket{HomeID: identity.ID, OwnerUID: *owner, PodUID: *uid,
		Namespace: *namespace, PodName: *name, Snapshot: *snapshot, SpecHash: *specHash, NoRoot: *noRoot}
	check(node.AtomicJSON(filepath.Join(*base, "tickets", *uid+".json"), t))
	printJSON(t)
}

func checkpoint(args []string) {
	flags := flag.NewFlagSet("checkpoint", flag.ExitOnError)
	base := flags.String("base", "", "isolated Cellbox state directory")
	owner := flags.String("owner", "", "owner identifier")
	uid := flags.String("uid", "", "active Pod UID")
	snapshot := flags.String("snapshot", "", "checkpoint identifier")
	runsc := flags.String("runsc", "", "runsc executable used to create the checkpoint")
	specHash := flags.String("spec", "warm-home-test-spec", "sandbox spec identity")
	noRoot := flags.Bool("no-root", false, "checkpoint was created from a no-root sandbox")
	_ = flags.Parse(args)
	identity, err := homevolume.Verify(*base, *owner, "", *specHash, "")
	check(err)
	check(homevolume.Checkpoint(*base, *owner, identity.ID, *uid, *snapshot))
	path, err := node.SnapshotPath(*base, *owner, *snapshot)
	check(err)
	runscHash, err := node.Digest(*runsc)
	check(err)
	check(node.Seal(path, node.Manifest{OwnerUID: *owner, SpecHash: *specHash, RunscHash: runscHash, HomeID: identity.ID, NoRoot: *noRoot}))
	_, err = node.VerifyCached(path, *owner, *specHash, *runsc)
	check(err)
	fmt.Println(path)
}

func registerWarm(args []string) {
	flags := flag.NewFlagSet("register-warm", flag.ExitOnError)
	base := flags.String("base", "", "isolated Cellbox state directory")
	uid := flags.String("uid", "", "warm Pod UID")
	name := flags.String("name", "", "warm Pod name")
	namespace := flags.String("namespace", "warm-home-test", "warm Pod namespace")
	specHash := flags.String("spec", "warm-home-test-spec", "sandbox spec identity")
	_ = flags.Parse(args)
	pod := &core.Pod{ObjectMeta: meta.ObjectMeta{UID: types.UID(*uid), Name: *name, Namespace: *namespace}}
	check((&node.Backend{Base: *base}).RegisterWarm(context.Background(), pod, *specHash))
}

func claimWarm(args []string) {
	flags := flag.NewFlagSet("claim-warm", flag.ExitOnError)
	base := flags.String("base", "", "isolated Cellbox state directory")
	owner := flags.String("owner", "", "owner identifier")
	uid := flags.String("uid", "", "warm Pod UID")
	name := flags.String("name", "", "warm Pod name")
	namespace := flags.String("namespace", "warm-home-test", "warm Pod namespace")
	snapshot := flags.String("snapshot", "", "node-local snapshot identifier")
	specHash := flags.String("spec", "warm-home-test-spec", "sandbox spec identity")
	_ = flags.Parse(args)
	leasePath := filepath.Join(*base, "warm", *uid+".json")
	deadline := time.Now().Add(15 * time.Second)
	var lease node.WarmLease
	for time.Now().Before(deadline) {
		check(node.ReadJSON(leasePath, &lease))
		if lease.Phase == "waiting" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if lease.Phase != "waiting" {
		fatal("warm adapter did not enter waiting phase before deadline")
	}
	if lease.UID != *uid || lease.Name != *name || lease.Namespace != *namespace || lease.SpecHash != *specHash || lease.SID == "" {
		fatal("warm lease identity did not match the requested assignment")
	}
	identity, err := homevolume.Verify(*base, *owner, "", *specHash, "")
	check(err)
	t := node.Ticket{HomeID: identity.ID, OwnerUID: *owner, PodUID: *uid,
		Namespace: *namespace, PodName: *name, Snapshot: *snapshot, SpecHash: *specHash, NoRoot: lease.NoRoot}
	check(node.AtomicJSON(filepath.Join(*base, "tickets", *uid+".json"), t))
	lease.Phase, lease.Ticket = "claimed", t
	check(node.AtomicJSON(leasePath, lease))
}

func verifyCheckpoint(args []string) {
	flags := flag.NewFlagSet("verify-checkpoint", flag.ExitOnError)
	base := flags.String("base", "", "isolated Cellbox state directory")
	owner := flags.String("owner", "", "owner identifier")
	snapshot := flags.String("snapshot", "", "node-local snapshot identifier")
	runsc := flags.String("runsc", "", "runsc executable")
	specHash := flags.String("spec", "warm-home-test-spec", "sandbox spec identity")
	_ = flags.Parse(args)
	path, err := node.SnapshotPath(*base, *owner, *snapshot)
	check(err)
	_, err = node.VerifyPrepared(path, *owner, *specHash, *runsc)
	check(err)
}

// retire exercises the same invalidation and garbage collection that run after
// Cellbox observes a restored process. A lazy reader must survive unlinking its
// source checkpoint and remain able to fault in pages and checkpoint again.
func retire(args []string) {
	flags := flag.NewFlagSet("retire", flag.ExitOnError)
	base := flags.String("base", "", "isolated Cellbox state directory")
	owner := flags.String("owner", "", "owner identifier")
	snapshot := flags.String("snapshot", "", "node-local snapshot identifier")
	_ = flags.Parse(args)
	backend := &node.Backend{Base: *base}
	w := &api.ResumablePod{ObjectMeta: meta.ObjectMeta{UID: types.UID(*owner)}, Status: api.Status{Snapshot: *snapshot}}
	check(backend.InvalidateSnapshot(context.Background(), w))
	check(backend.CollectGarbage(context.Background()))
}

func printJSON(v any) {
	if err := json.NewEncoder(os.Stdout).Encode(v); err != nil {
		fatal(err.Error())
	}
}

func check(err error) {
	if err != nil {
		fatal(err.Error())
	}
}

func fatal(message string) {
	fmt.Fprintln(os.Stderr, "warm-home helper:", message)
	os.Exit(1)
}
