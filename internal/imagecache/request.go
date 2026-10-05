// Package imagecache defines node-local image pull requests. Requests carry no
// registry credentials; the prepared registry uses the node's CRI configuration.
package imagecache

const (
	Label   = "cellbox.local/image-cache"
	Status  = "cellbox.local/image-cache-status"
	Message = "cellbox.local/image-cache-message"
)
