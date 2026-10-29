package resumable

import (
	"context"
	"time"

	api "github.com/jingyugao/cell-box/api/v1alpha1"
	"github.com/jingyugao/cell-box/internal/boxprovider"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (p *Provider) WatchChanges(ctx context.Context, h boxprovider.Handle) (<-chan struct{}, error) {
	if p.Local != nil {
		observations, err := p.WatchObservations(ctx, h)
		if err != nil {
			return nil, err
		}
		changes := make(chan struct{}, 1)
		go func() {
			defer close(changes)
			for range observations {
				select {
				case changes <- struct{}{}:
				default:
				}
			}
		}()
		return changes, nil
	}
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
		reconnectDelay := 100 * time.Millisecond
		for {
			if stream == nil {
				select {
				case <-ctx.Done():
					return
				case <-time.After(reconnectDelay):
				}
				stream, _ = open()
				if stream == nil {
					reconnectDelay = min(reconnectDelay*2, 5*time.Second)
					continue
				}
				reconnectDelay = 100 * time.Millisecond
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
				reconnectDelay = 100 * time.Millisecond
				notify()
			}
		}
	}()
	return changes, nil
}
