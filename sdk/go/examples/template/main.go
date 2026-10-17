// Example: create or reuse a template family, build an immutable revision,
// publish it as the stable revision, then create a sandbox from that template.
//
// Required: CELLBOX_API_URL, CELLBOX_PROFILE_ID, CELLBOX_BASE_IMAGE.
// Optional: CELLBOX_CLIENT_ID, CELLBOX_CLIENT_TOKEN, CELLBOX_TEMPLATE_ID,
// CELLBOX_TEMPLATE_NAME, CELLBOX_USER_VERSION, CELLBOX_BUILD_COMMAND,
// CELLBOX_RUN_COMMAND, CELLBOX_BUILD_KEY, CELLBOX_PLATFORM,
// CELLBOX_VERIFY_COMMAND.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	cellbox "github.com/jingyugao/cell-box/sdk/go"
)

const defaultBuildCommand = `mkdir -p /usr/local/bin && cat > /usr/local/bin/template-tool <<'EOF'
#!/bin/sh
echo "template-tool ran inside Cellbox"
EOF
chmod +x /usr/local/bin/template-tool`

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
	buildCtx, cancelBuild := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancelBuild()

	profileID, err := requiredEnv("CELLBOX_PROFILE_ID")
	if err != nil {
		return err
	}
	baseImage, err := requiredEnv("CELLBOX_BASE_IMAGE")
	if err != nil {
		return err
	}
	userVersion := envOr("CELLBOX_USER_VERSION", "1.01")
	customBuildCommand := strings.TrimSpace(os.Getenv("CELLBOX_BUILD_COMMAND"))
	buildCommand := customBuildCommand
	if buildCommand == "" {
		buildCommand = defaultBuildCommand
	}
	// A build key is printed before submission so an operator can safely
	// resubmit the exact request after a client disconnect.
	buildKey := envOr("CELLBOX_BUILD_KEY", "cellbox-sdk-template-"+newID())
	fmt.Printf("build idempotency key: %s\n", buildKey)

	template, err := selectTemplate(buildCtx, client)
	if err != nil {
		return err
	}
	fmt.Printf("template ID: %s\n", template.ID)

	buildOp, err := client.BuildRevision(buildCtx, template.ID, cellbox.CreateRevisionOptions{
		UserVersion:  userVersion,
		BaseImage:    baseImage,
		ProfileID:    profileID,
		BuildCommand: buildCommand,
		RunCommand:   os.Getenv("CELLBOX_RUN_COMMAND"),
		Platform:     envOr("CELLBOX_PLATFORM", "linux/amd64"),
	}, buildKey)
	if err != nil {
		var requestErr *cellbox.RequestError
		var apiErr *cellbox.APIError
		if errors.As(err, &requestErr) && !errors.As(err, &apiErr) {
			fmt.Printf("build request outcome is unknown; recover with key %s and identical input\n", requestErr.IdempotencyKey)
		}
		return fmt.Errorf("submit build: %w", err)
	}
	fmt.Printf("build operation ID: %s\n", buildOp.ID)

	revision, err := client.WaitBuildRevision(buildCtx, buildOp)
	if err != nil {
		var operationErr *cellbox.OperationError
		if errors.As(err, &operationErr) && operationErr.Operation != nil {
			fmt.Printf("build operation ID: %s\n", operationErr.Operation.ID)
		}
		return fmt.Errorf("wait for build: %w", err)
	}
	fmt.Printf("built revision: %s (%s)\n", revision.ID, revision.Version)
	fmt.Println("template and revisions are retained for reuse")

	// Use the generation returned by creation so concurrent edits fail clearly
	// with a conflict instead of silently overwriting another client's update.
	updatedTemplate, err := client.SetTemplateStableRevision(buildCtx, template.ID, revision.ID, template.Generation)
	if err != nil {
		return fmt.Errorf("publish stable revision (template %s, revision %s): %w", template.ID, revision.ID, err)
	}
	fmt.Printf("stable revision: %s (template generation %d)\n", updatedTemplate.StableRevisionID, updatedTemplate.Generation)

	boxCtx, cancelBox := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancelBox()
	box, err := client.Create(boxCtx, cellbox.CreateOptions{
		TemplateID: template.ID,
		OwnerKey:   "go-sdk-template-example",
	})
	if err != nil {
		var operationErr *cellbox.OperationError
		if errors.As(err, &operationErr) && operationErr.Operation != nil {
			fmt.Printf("sandbox create operation ID: %s\n", operationErr.Operation.ID)
			cleanupFailedCreate(client, operationErr.Operation)
		}
		return fmt.Errorf("create sandbox: %w", err)
	}
	fmt.Printf("sandbox ID: %s\n", box.ID)
	defer destroy(box.ID, box)

	command := strings.TrimSpace(os.Getenv("CELLBOX_VERIFY_COMMAND"))
	if command == "" {
		if customBuildCommand == "" {
			command = "/usr/local/bin/template-tool"
		} else {
			command = "printf 'custom template ready\\n'; pwd"
		}
	}
	result, err := box.Commands.Run(boxCtx, command, &cellbox.CommandOptions{Timeout: 20 * time.Second})
	if result != nil {
		fmt.Print(result.Stdout)
		fmt.Fprint(os.Stderr, result.Stderr)
	}
	if err != nil {
		return fmt.Errorf("run %q in sandbox %s: %w", command, box.ID, err)
	}
	return nil
}

func cleanupFailedCreate(client *cellbox.Client, op *cellbox.Operation) {
	if op.TargetID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	box, err := client.Connect(ctx, op.TargetID)
	if err != nil {
		log.Printf("inspect sandbox from create operation %s: %v", op.ID, err)
		return
	}
	if err := box.Destroy(ctx, nil); err != nil {
		log.Printf("destroy sandbox %s after create error: %v", box.ID, err)
	}
}

func selectTemplate(ctx context.Context, client *cellbox.Client) (*cellbox.Template, error) {
	if id := strings.TrimSpace(os.Getenv("CELLBOX_TEMPLATE_ID")); id != "" {
		template, err := client.GetTemplate(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("get template %s: %w", id, err)
		}
		return template, nil
	}
	name := strings.TrimSpace(os.Getenv("CELLBOX_TEMPLATE_NAME"))
	if name == "" {
		name = "go-sdk-example-" + newID()
	}
	template, err := client.CreateTemplate(ctx, cellbox.CreateTemplateOptions{
		Name:        name,
		Description: "Created by the Cellbox Go SDK template example",
	})
	if err != nil {
		var apiErr *cellbox.APIError
		if errors.As(err, &apiErr) && apiErr.StatusCode == 409 {
			return nil, fmt.Errorf("template name %q already exists; set CELLBOX_TEMPLATE_ID to reuse it or choose a unique CELLBOX_TEMPLATE_NAME: %w", name, err)
		}
		return nil, fmt.Errorf("create template %q: %w", name, err)
	}
	return template, nil
}

func destroy(id string, box *cellbox.Sandbox) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := box.Destroy(ctx, nil); err != nil {
		log.Printf("destroy sandbox %s: %v", id, err)
	}
}

func requiredEnv(name string) (string, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return "", fmt.Errorf("set required environment variable %s", name)
	}
	return value, nil
}

func envOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func newID() string {
	return fmt.Sprintf("%x-%x", time.Now().UnixNano(), os.Getpid())
}
