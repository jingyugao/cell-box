package service

import (
	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/catalog"
	"cellbox.local/cellbox/internal/guestapi"
	"cellbox.local/cellbox/internal/objectstorage"
	"cellbox.local/cellbox/internal/volumebackup"
	"time"
)

type Profile struct {
	// Private installations may let client-owned imported images inherit this
	// profile's protected tools and managed HOME without per-digest admission.
	AutoTrustImportedImages bool `json:"autoTrustImportedImages,omitempty"`
	// Exact prepared images explicitly approved by the operator to run Guest.Tools.
	TrustedToolImages []string `json:"trustedToolImages,omitempty"`
	// Images whose protected runner can read a mounted tool-runtime descriptor.
	MountedToolRuntimeImages []string `json:"mountedToolRuntimeImages,omitempty"`
	// Images whose runner authorizes access to single files in the mounted HOME.
	DebugHomeImages        []string `json:"debugHomeImages,omitempty"`
	ID                     string   `json:"id"`
	Provider               string   `json:"provider"`
	Image                  string   `json:"image"`
	NodeName               string   `json:"nodeName,omitempty"`
	Namespace              string   `json:"namespace,omitempty"`
	SharedReadOnlyHostPath string   `json:"sharedReadOnlyHostPath,omitempty"`
	DebugReadOnlyHostPath  string   `json:"debugReadOnlyHostPath,omitempty"`
	DebugReadWriteHostPath string   `json:"debugReadWriteHostPath,omitempty"`
	// PersistentHome records the immutable storage layout, not an operator
	// switch. Config validation enables it for new resumable and local-runsc
	// Boxes; old stored profiles retain false. The initially empty HOME hides
	// image contents. New Boxes reference an independent HOME volume.
	PersistentHome bool `json:"persistentHome,omitempty"`
	// Resource limits are opt-in. Omitted or zero values leave that resource unlimited.
	CPU       float64         `json:"cpu,omitempty"`
	MemoryMiB int64           `json:"memoryMiB,omitempty"`
	Guest     guestapi.Config `json:"guest"`
}
type Config struct {
	CatalogBuilder catalog.Builder `json:"-"`
	// ClientID selects a data namespace; it is not an authenticated identity.
	ClientID                  string               `json:"clientId,omitempty"`
	ObjectStorage             objectstorage.Config `json:"objectStorage,omitempty"`
	VolumeBackups             volumebackup.Config  `json:"volumeBackups,omitempty"`
	Listen                    string               `json:"listen"`
	DataDir                   string               `json:"dataDir"`
	Profiles                  []Profile            `json:"profiles"`
	StartupTimeoutSeconds     int                  `json:"startupTimeoutSeconds,omitempty"`
	OperationRetentionSeconds int                  `json:"operationRetentionSeconds,omitempty"`
	ExecutionRetentionSeconds int                  `json:"executionRetentionSeconds,omitempty"`
	ImageBuild                ImageBuildConfig     `json:"imageBuild,omitempty"`
}
type ImageBuildConfig struct {
	Address          string `json:"address,omitempty"`
	Repository       string `json:"repository,omitempty"`
	GuestBinary      string `json:"guestBinary,omitempty"`
	BuildctlBinary   string `json:"buildctlBinary,omitempty"`
	InsecureRegistry string `json:"insecureRegistry,omitempty"`
}
type APIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *APIError) Error() string         { return e.Message }
func apiError(code, message string) error { return &APIError{Code: code, Message: message} }

type Capabilities struct {
	Exec               bool   `json:"exec"`
	Files              bool   `json:"files"`
	HTTP               bool   `json:"http"`
	WebSocket          bool   `json:"websocket"`
	PTY                bool   `json:"pty"`
	ReconnectExec      bool   `json:"reconnectExec"`
	Suspend            string `json:"suspend"`
	Archives           string `json:"archives"`
	ProtectedTools     bool   `json:"protectedTools"`
	CredentialBatch    bool   `json:"credentialBatch,omitempty"`
	InternalServices   bool   `json:"internalServices,omitempty"`
	RootDebug          bool   `json:"rootDebug,omitempty"`
	SharedDirectory    bool   `json:"sharedDirectory,omitempty"`
	MountedToolRuntime bool   `json:"mountedToolRuntime,omitempty"`
	MountedDebugHome   bool   `json:"mountedDebugHome,omitempty"`
	PersistentHome     bool   `json:"persistentHome,omitempty"`
}
type Box struct {
	Mounts          []Mount      `json:"mounts,omitempty"`
	HomeVolumeID    string       `json:"homeVolumeId,omitempty"`
	TemplateID      string       `json:"templateId"`
	RevisionID      string       `json:"revisionId"`
	TemplateName    string       `json:"templateName"`
	TemplateVersion string       `json:"templateVersion"`
	ID              string       `json:"id"`
	OwnerKey        string       `json:"ownerKey"`
	ProfileID       string       `json:"profileId"`
	Phase           string       `json:"phase"`
	Generation      uint64       `json:"generation"`
	ExecutionID     string       `json:"executionId,omitempty"`
	ObservedAt      *time.Time   `json:"observedAt,omitempty"`
	Version         uint64       `json:"resourceVersion"`
	Image           string       `json:"image"`
	ImageID         string       `json:"imageId,omitempty"`
	Workspace       string       `json:"workspace"`
	Capabilities    Capabilities `json:"capabilities"`
	OperationID     string       `json:"operationId,omitempty"`
	Error           *APIError    `json:"error,omitempty"`
	CreatedAt       time.Time    `json:"createdAt"`
}
type boxRecord struct {
	VolumeMounts         []api.VolumeMount  `json:"volumeMounts,omitempty"`
	HomeVolumeUID        string             `json:"homeVolumeUID,omitempty"`
	Box                  Box                `json:"box"`
	ClientID             string             `json:"clientId"`
	Profile              Profile            `json:"profile"`
	Handle               boxprovider.Handle `json:"handle"`
	ExecutionID          string             `json:"executionId,omitempty"`
	TransitionID         string             `json:"transitionId,omitempty"`
	ObservedTransitionID string             `json:"observedTransitionId,omitempty"`
	Staged               bool               `json:"staged,omitempty"`
	RestoreArchiveID     string             `json:"restoreArchiveId,omitempty"`
	RestoreComplete      bool               `json:"restoreComplete,omitempty"`
	AcceptImageChange    bool               `json:"acceptImageChange,omitempty"`
}
type Operation struct {
	ID         string            `json:"id"`
	Kind       string            `json:"kind"`
	TargetID   string            `json:"targetId"`
	Status     string            `json:"status"`
	Version    uint64            `json:"version"`
	CreatedAt  time.Time         `json:"createdAt"`
	FinishedAt *time.Time        `json:"finishedAt,omitempty"`
	Result     map[string]string `json:"result,omitempty"`
	Box        *Box              `json:"box,omitempty"`
	Error      *APIError         `json:"error,omitempty"`
}
type operationRecord struct {
	Operation Operation `json:"operation"`
	ClientID  string    `json:"clientId"`
}
type Execution struct {
	ID          string               `json:"id"`
	BoxID       string               `json:"boxId"`
	OperationID string               `json:"operationId"`
	State       string               `json:"state"`
	Result      *guestapi.ExecResult `json:"result,omitempty"`
}
type Archive struct {
	TemplateID      string `json:"templateId"`
	RevisionID      string `json:"revisionId"`
	TemplateName    string `json:"templateName"`
	TemplateVersion string `json:"templateVersion"`
	// Recovery and image deletion must not depend on a retained source Box.
	ManifestVersion int               `json:"manifestVersion,omitempty"`
	ID              string            `json:"id"`
	SourceBoxID     string            `json:"sourceBoxId"`
	ProfileID       string            `json:"profileId"`
	ImageID         string            `json:"imageId"`
	Agent           guestapi.Identity `json:"agent"`
	SHA256          string            `json:"sha256"`
	Size            int64             `json:"size"`
	Consistency     string            `json:"consistency"`
	Portable        bool              `json:"portable"`
	CreatedAt       time.Time         `json:"createdAt"`
}
type archiveRecord struct {
	Deleting bool    `json:"deleting,omitempty"`
	Archive  Archive `json:"archive"`
	ClientID string  `json:"clientId"`
}
type executionRecord struct {
	Execution
}
