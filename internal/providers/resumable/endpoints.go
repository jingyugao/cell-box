package resumable

import (
	"context"
	"fmt"
	"net"

	"github.com/jingyugao/cell-box/internal/boxprovider"
	"github.com/jingyugao/cell-box/internal/guestapi"
	"k8s.io/apimachinery/pkg/types"
)

func (p *Provider) CachedGuestForExecution(ctx context.Context, h boxprovider.Handle, expected string) (boxprovider.Connection, error) {
	if p.Local == nil {
		return p.GuestForExecution(ctx, h, expected)
	}
	if err := ctx.Err(); err != nil {
		return boxprovider.Connection{}, err
	}
	if err := validateHandle(h); err != nil {
		return boxprovider.Connection{}, err
	}
	endpoint, ok := p.Local.GuestEndpoint(types.NamespacedName{Namespace: h.Namespace, Name: h.Name})
	if !ok {
		return boxprovider.Connection{}, boxprovider.ErrNotReady
	}
	if endpoint.OwnerUID != h.ID || endpoint.BoxID != boxIDFromHandle(h) || endpoint.NodeName != h.NodeName ||
		(expected != "" && endpoint.PodUID != expected) {
		return boxprovider.Connection{}, boxprovider.ErrStaleExecution
	}
	return boxprovider.Connection{URL: "http://" + net.JoinHostPort(endpoint.IP, fmt.Sprint(guestapi.Port))}, nil
}
