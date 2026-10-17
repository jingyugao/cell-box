// Example: create a sandbox, use its workspace file API, suspend/freeze it,
// resume/unfreeze it, and continue using the handle returned by that operation.
//
// Required: CELLBOX_API_URL and exactly one of CELLBOX_REVISION_ID or
// CELLBOX_TEMPLATE_ID. Optionally set CELLBOX_CLIENT_ID and CELLBOX_CLIENT_TOKEN.
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	cellbox "github.com/jingyugao/cell-box/sdk/go"
)

const workspaceFile = "cellbox-sdk-lifecycle-example.txt"

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	client, err := cellbox.NewClient(cellbox.Config{})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	revisionID := os.Getenv("CELLBOX_REVISION_ID")
	templateID := os.Getenv("CELLBOX_TEMPLATE_ID")
	if (revisionID == "") == (templateID == "") {
		return fmt.Errorf("set exactly one of CELLBOX_REVISION_ID or CELLBOX_TEMPLATE_ID")
	}
	box, err := client.Create(ctx, cellbox.CreateOptions{
		RevisionID: revisionID,
		TemplateID: templateID,
		OwnerKey:   "go-sdk-lifecycle-example",
	})
	if err != nil {
		printOperationID(err)
		return fmt.Errorf("create sandbox: %w", err)
	}
	fmt.Printf("sandbox ID: %s\n", box.ID)
	defer cleanup(client, box.ID, &box)

	const content = "workspace data survives suspend and resume\n"
	if err := box.Files.Write(ctx, workspaceFile, []byte(content)); err != nil {
		return fmt.Errorf("write workspace file: %w", err)
	}
	read, err := box.Files.Read(ctx, workspaceFile)
	if err != nil {
		return fmt.Errorf("read workspace file: %w", err)
	}
	if !bytes.Equal(read, []byte(content)) {
		return fmt.Errorf("workspace file content mismatch before lifecycle change: got %q", string(read))
	}
	fmt.Printf("file before lifecycle change: %q\n", string(read))
	entries, err := box.Files.List(ctx, ".")
	if err != nil {
		return fmt.Errorf("list workspace: %w", err)
	}
	if !containsFile(entries, workspaceFile) {
		return fmt.Errorf("workspace listing does not contain %q", workspaceFile)
	}
	fmt.Printf("workspace entries: %d\n", len(entries))

	info, err := box.Info(ctx)
	if err != nil {
		return fmt.Errorf("read sandbox capabilities: %w", err)
	}
	var resumed *cellbox.Sandbox
	switch {
	case info.Capabilities.Suspend == "same-node-checkpoint":
		fmt.Println("suspending sandbox")
		suspendKey := "lifecycle-example-suspend-" + box.ID
		fmt.Printf("suspend idempotency key: %s\n", suspendKey)
		if err := box.Suspend(ctx, &cellbox.LifecycleOptions{IdempotencyKey: suspendKey}); err != nil {
			printOperationID(err)
			return fmt.Errorf("suspend sandbox: %w", err)
		}
		resumeKey := "lifecycle-example-resume-" + box.ID
		fmt.Printf("resume idempotency key: %s\n", resumeKey)
		resumed, err = box.Resume(ctx, &cellbox.LifecycleOptions{IdempotencyKey: resumeKey})
	case info.Capabilities.Freeze:
		fmt.Println("freezing sandbox")
		freezeKey := "lifecycle-example-freeze-" + box.ID
		fmt.Printf("freeze idempotency key: %s\n", freezeKey)
		if err := box.Freeze(ctx, &cellbox.LifecycleOptions{IdempotencyKey: freezeKey}); err != nil {
			printOperationID(err)
			return fmt.Errorf("freeze sandbox: %w", err)
		}
		unfreezeKey := "lifecycle-example-unfreeze-" + box.ID
		fmt.Printf("unfreeze idempotency key: %s\n", unfreezeKey)
		err = box.Unfreeze(ctx, &cellbox.LifecycleOptions{IdempotencyKey: unfreezeKey})
		if err == nil {
			// Unfreeze leaves the runtime generation unchanged, but reconnecting
			// also demonstrates how to obtain a current handle after a transition.
			resumed, err = client.Connect(ctx, box.ID)
		}
	default:
		return fmt.Errorf("sandbox %s supports neither suspend nor freeze", box.ID)
	}
	if err != nil {
		printOperationID(err)
		// A lost response can leave the lifecycle operation's outcome unknown.
		// Refresh the cleanup handle if the server is reachable; keep returning
		// the original lifecycle error even if refresh also fails.
		refreshCtx, cancelRefresh := context.WithTimeout(context.Background(), 15*time.Second)
		refreshed, refreshErr := client.Connect(refreshCtx, box.ID)
		cancelRefresh()
		if refreshErr == nil {
			box = refreshed
		} else {
			log.Printf("refresh sandbox %s for cleanup: %v", box.ID, refreshErr)
		}
		return fmt.Errorf("resume sandbox: %w", err)
	}
	if resumed == nil {
		return fmt.Errorf("lifecycle operation returned an empty sandbox handle")
	}
	box = resumed
	fmt.Printf("resumed sandbox with refreshed handle: %s (generation %d)\n", box.ID, box.Generation)

	read, err = box.Files.Read(ctx, workspaceFile)
	if err != nil {
		return fmt.Errorf("read workspace file after resume: %w", err)
	}
	if !bytes.Equal(read, []byte(content)) {
		return fmt.Errorf("workspace file content mismatch after lifecycle change: got %q", string(read))
	}
	fmt.Printf("file after lifecycle change: %q\n", string(read))
	entries, err = box.Files.List(ctx, ".")
	if err != nil {
		return fmt.Errorf("list workspace after lifecycle change: %w", err)
	}
	if !containsFile(entries, workspaceFile) {
		return fmt.Errorf("workspace listing after lifecycle change does not contain %q", workspaceFile)
	}

	result, err := box.Commands.Run(ctx, "printf 'command ran after lifecycle transition\\n'", &cellbox.CommandOptions{Timeout: 10 * time.Second})
	if result != nil {
		fmt.Print(result.Stdout)
		fmt.Fprint(os.Stderr, result.Stderr)
	}
	if err != nil {
		printOperationID(err)
		return fmt.Errorf("run command with refreshed sandbox handle: %w", err)
	}
	return nil
}

func containsFile(entries []cellbox.FileEntry, name string) bool {
	for _, entry := range entries {
		if entry.Name == name && !entry.Directory {
			return true
		}
	}
	return false
}

func cleanup(client *cellbox.Client, id string, box **cellbox.Sandbox) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if *box == nil {
		return
	}
	if current, err := client.Connect(ctx, id); err == nil {
		*box = current
	} else {
		log.Printf("refresh sandbox %s before cleanup: %v", id, err)
	}
	if err := (*box).Destroy(ctx, nil); err != nil {
		var apiErr *cellbox.APIError
		if errors.As(err, &apiErr) {
			log.Printf("destroy sandbox %s: %s (%s)", id, apiErr.Code, apiErr.Message)
			return
		}
		log.Printf("destroy sandbox %s: %v", id, err)
	}
}

func printOperationID(err error) {
	var operationErr *cellbox.OperationError
	if errors.As(err, &operationErr) && operationErr.Operation != nil {
		fmt.Printf("operation ID: %s\n", operationErr.Operation.ID)
	}
}
