// Package archivelimits defines shared workspace archive size limits.
package archivelimits

const (
	// Keep archives bounded while leaving enough room for large workspaces and
	// for compressed archives whose contents do not compress well.
	MaxEntryBytes   int64 = 8 << 30
	MaxContentBytes int64 = 16 << 30
	MaxWireBytes    int64 = 18 << 30
)
