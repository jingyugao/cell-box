package resumable

import (
	"context"
	"time"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/boxprovider"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (p *Provider) WatchChanges(ctx context.Context, h boxprovider.Handle) (<-chan struct{}, error) {
	c, ok := p.Client.(client.WithWatch)
	if !ok {
		return nil, nil
	}
	open := func() (watch.Interface, error) {
		w, err := p.get(ctx, h)
		if err != nil {
			return nil, err
		}
		return c.Watch(ctx, &api.ResumablePodList{}, &client.ListOptions{Namespace: h.Namespace, Raw: &meta.ListOptions{ResourceVersion: w.ResourceVersion, FieldSelector: "metadata.name=" + h.Name}})
	}
	stream, err := open()
	if err != nil {
		return nil, err
	}
	changes := make(chan struct{}, 1)
	notify := func() {
		select {
		case changes <- struct{}{}:
		default:
		}
	}
	go func() {
		defer close(changes)
		for {
			if stream == nil {
				select {
				case <-ctx.Done():
					return
				case <-time.After(100 * time.Millisecond):
				}
				stream, _ = open()
				if stream == nil {
					continue
				}
				// Re-observe after reconnecting; the current state covers any gap.
				notify()
			}
			select {
			case <-ctx.Done():
				stream.Stop()
				return
			case event, ok := <-stream.ResultChan():
				if !ok || event.Type == watch.Error {
					stream.Stop()
					stream = nil
					notify()
					continue
				}
				notify()
			}
		}
	}()
	return changes, nil
}
