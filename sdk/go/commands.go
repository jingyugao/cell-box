package cellbox

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type Commands struct{ sandbox *Sandbox }

type CommandOptions struct {
	// Cwd is relative to the sandbox workspace, as required by the server.
	Cwd string
	Env map[string]string
	// Timeout limits server execution, independently of the caller's context.
	// Zero uses 30 seconds. Nonzero values must be 1ms..5min, in whole milliseconds.
	Timeout time.Duration
}

// Command represents accepted execution. Wait may be called again after local
// cancellation, with a fresh context. This does not rerun the command.
type Command struct {
	OperationID string
	ExecutionID string
	client      *Client
	operation   *Operation
}

// Run executes a shell script using /bin/sh -c, which must exist in the image.
// A nonzero exit returns both the output and a CommandExitError.
func (c *Commands) Run(ctx context.Context, command string, opts *CommandOptions) (*CommandResult, error) {
	handle, err := c.Start(ctx, command, opts)
	if err != nil {
		return nil, err
	}
	return handle.Wait(ctx)
}

// RunArgv invokes an absolute executable without shell interpretation.
func (c *Commands) RunArgv(ctx context.Context, argv []string, opts *CommandOptions) (*CommandResult, error) {
	handle, err := c.StartArgv(ctx, argv, opts)
	if err != nil {
		return nil, err
	}
	return handle.Wait(ctx)
}

// Start submits a bounded command without waiting for its completion. This is
// not an unbounded background process or a streaming-output API.
func (c *Commands) Start(ctx context.Context, command string, opts *CommandOptions) (*Command, error) {
	return c.StartArgv(ctx, []string{"/bin/sh", "-c", command}, opts)
}

func (c *Commands) StartArgv(ctx context.Context, argv []string, opts *CommandOptions) (*Command, error) {
	if len(argv) == 0 || len(argv) > 128 || !strings.HasPrefix(argv[0], "/") {
		return nil, fmt.Errorf("cellbox: argv must contain 1..128 arguments starting with an absolute executable")
	}
	for _, arg := range argv {
		if len(arg) > 8192 || strings.ContainsRune(arg, 0) {
			return nil, fmt.Errorf("cellbox: invalid command argument")
		}
	}
	options := CommandOptions{}
	if opts != nil {
		options = *opts
	}
	if options.Timeout < 0 || options.Timeout > 5*time.Minute || options.Timeout%time.Millisecond != 0 {
		return nil, fmt.Errorf("cellbox: command timeout must be whole milliseconds between 1ms and 5min, or zero for the default")
	}
	if c.sandbox.Generation == 0 {
		return nil, fmt.Errorf("cellbox: sandbox has no runtime generation; connect after provisioning completes")
	}
	path, err := resourcePath("/v1/boxes/", c.sandbox.ID)
	if err != nil {
		return nil, err
	}
	body := struct {
		Argv               []string          `json:"argv"`
		Cwd                string            `json:"cwd,omitempty"`
		Env                map[string]string `json:"env,omitempty"`
		TimeoutMS          int64             `json:"timeoutMs,omitempty"`
		ExpectedGeneration uint64            `json:"expectedGeneration"`
	}{argv, options.Cwd, options.Env, options.Timeout.Milliseconds(), c.sandbox.Generation}
	op, err := c.sandbox.client.submit(ctx, path+"/execs", body)
	if err != nil {
		return nil, err
	}
	if op.Result["execId"] == "" {
		return nil, &OperationError{Operation: op, Err: fmt.Errorf("missing execution ID")}
	}
	return &Command{OperationID: op.ID, ExecutionID: op.Result["execId"], client: c.sandbox.client, operation: op}, nil
}

func (c *Command) Wait(ctx context.Context) (*CommandResult, error) {
	op, err := c.client.wait(ctx, c.operation)
	if err != nil {
		return nil, err
	}
	execution, err := c.client.GetExecution(ctx, c.ExecutionID)
	if err != nil {
		return nil, &OperationError{Operation: op, Err: err}
	}
	if execution.State != "exited" || execution.Result == nil {
		return nil, &OperationError{Operation: op, Err: &APIError{Code: "UNKNOWN_OUTCOME", Message: "execution has no definitive exit result (state " + execution.State + ")"}}
	}
	if execution.Result.ExitCode != 0 {
		return execution.Result, &CommandExitError{Result: execution.Result}
	}
	return execution.Result, nil
}
