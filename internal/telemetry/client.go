package telemetry

import (
	"context"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// TraceClient includes client-side throttling, serialization and response reads,
// which transport-only instrumentation would miss. Background watches are untouched.
func TraceClient(c client.Client) client.Client {
	wrapped := &tracedClient{Client: c}
	if w, ok := c.(client.WithWatch); ok {
		return &tracedWatchClient{tracedClient: wrapped, watcher: w}
	}
	return wrapped
}

type tracedClient struct{ client.Client }

func call(ctx context.Context, name string, fn func(context.Context) error) error {
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return fn(ctx)
	}
	ctx, span := Start(ctx, "cellbox.kubernetes."+name)
	err := fn(ctx)
	End(span, err)
	return err
}
func (c *tracedClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	return call(ctx, "get", func(ctx context.Context) error { return c.Client.Get(ctx, key, obj, opts...) })
}
func (c *tracedClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	return call(ctx, "list", func(ctx context.Context) error { return c.Client.List(ctx, list, opts...) })
}
func (c *tracedClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	return call(ctx, "create", func(ctx context.Context) error { return c.Client.Create(ctx, obj, opts...) })
}
func (c *tracedClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	return call(ctx, "update", func(ctx context.Context) error { return c.Client.Update(ctx, obj, opts...) })
}
func (c *tracedClient) Patch(ctx context.Context, obj client.Object, p client.Patch, opts ...client.PatchOption) error {
	return call(ctx, "patch", func(ctx context.Context) error { return c.Client.Patch(ctx, obj, p, opts...) })
}
func (c *tracedClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	return call(ctx, "delete", func(ctx context.Context) error { return c.Client.Delete(ctx, obj, opts...) })
}
func (c *tracedClient) Status() client.SubResourceWriter {
	return &tracedStatus{SubResourceWriter: c.Client.Status()}
}

type tracedStatus struct{ client.SubResourceWriter }

func (c *tracedStatus) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	return call(ctx, "status.update", func(ctx context.Context) error { return c.SubResourceWriter.Update(ctx, obj, opts...) })
}
func (c *tracedStatus) Patch(ctx context.Context, obj client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
	return call(ctx, "status.patch", func(ctx context.Context) error { return c.SubResourceWriter.Patch(ctx, obj, p, opts...) })
}

type tracedWatchClient struct {
	*tracedClient
	watcher client.WithWatch
}

func (c *tracedWatchClient) Watch(ctx context.Context, list client.ObjectList, opts ...client.ListOption) (watch.Interface, error) {
	watchCtx, span := Start(ctx, "cellbox.kubernetes.watch_open")
	stream, err := c.watcher.Watch(watchCtx, list, opts...)
	End(span, err)
	return stream, err
}
