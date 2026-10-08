// Package archivelimits defines shared workspace archive size limits.
package archivelimits

const (
	MaxEntryBytes   int64 = 1 << 30
	MaxContentBytes int64 = 1 << 30
	MaxWireBytes    int64 = 2 << 30
)
