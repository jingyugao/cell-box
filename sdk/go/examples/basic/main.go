// Run with CELLBOX_API_URL and either CELLBOX_REVISION_ID or CELLBOX_TEMPLATE_ID
// (the latter resolves the template's stable revision), and optionally CELLBOX_CLIENT_ID
// and CELLBOX_CLIENT_TOKEN set. The token is only needed by authenticated gateways.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	cellbox "github.com/jingyugao/cell-box/sdk/go"
)

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
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	revisionID := os.Getenv("CELLBOX_REVISION_ID")
	templateID := os.Getenv("CELLBOX_TEMPLATE_ID")
	if (revisionID == "") == (templateID == "") {
		return fmt.Errorf("set exactly one of CELLBOX_REVISION_ID or CELLBOX_TEMPLATE_ID")
	}
	sandbox, err := client.Create(ctx, cellbox.CreateOptions{
		RevisionID: revisionID,
		TemplateID: templateID,
		OwnerKey:   "go-sdk-example",
	})
	if err != nil {
		return err
	}
	defer func() {
		// Cleanup needs a fresh context even when the caller's deadline expires.
		cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := sandbox.Destroy(cleanup); err != nil {
			log.Printf("destroy sandbox %s: %v", sandbox.ID, err)
		}
	}()

	result, err := sandbox.Commands.Run(ctx, "printf 'hello from Cellbox\\n'; pwd", &cellbox.CommandOptions{
		Timeout: 10 * time.Second,
	})
	if result != nil {
		fmt.Print(result.Stdout)
		fmt.Fprint(os.Stderr, result.Stderr)
	}
	return err
}
