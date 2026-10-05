package resumable

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"cellbox.local/cellbox/internal/imagecache"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (p *Provider) CacheImage(ctx context.Context, image, node, namespace string) error {
	if !validImage.MatchString(image) || node == "" || namespace == "" {
		return errors.New("image caching requires an immutable image, node and namespace")
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return err
	}
	immutable := true
	request := &core.ConfigMap{Immutable: &immutable, ObjectMeta: meta.ObjectMeta{
		Name: "cellbox-image-cache-" + hex.EncodeToString(id[:]), Namespace: namespace,
		Labels: map[string]string{imagecache.Label: "true"},
	}, Data: map[string]string{"image": image, "node": node}}
	if err := p.Client.Create(ctx, request); err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		_ = p.Client.Delete(cleanup, request)
	}()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		current := &core.ConfigMap{}
		if err := p.Client.Get(ctx, client.ObjectKeyFromObject(request), current); err != nil {
			return err
		}
		switch current.Annotations[imagecache.Status] {
		case "succeeded":
			return nil
		case "failed":
			return fmt.Errorf("node image cache failed: %s", current.Annotations[imagecache.Message])
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
