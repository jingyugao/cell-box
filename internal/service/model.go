package service

import (
	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/guestapi"
	"cellbox.local/cellbox/internal/objectstorage"
	"time"
)

type Profile struct {
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
	// switch. Config validation always enables it for new resumable Boxes;
	// old stored profiles retain false. The initially empty HOME hides image
	// contents and is retained across suspend/resume until the Box is deleted.
	PersistentHome bool            `json:"persistentHome,omitempty"`
	CPU            float64         `json:"cpu,omitempty"`
	MemoryMiB      int64           `json:"memoryMiB,omitempty"`
	Guest          guestapi.Config `json:"guest"`
}
type Config struct {
	// ClientID is a data/idempotency namespace, not an authenticated identity.
	ClientID                  string               `json:"clientId,omitempty"`
	ObjectStorage             objectstorage.Config `json:"objectStorage,omitempty"`
	Listen                    string               `json:"listen"`
	DataDir                   string               `json:"dataDir"`
	PublicURL                 string               `json:"publicUrl,omitempty"`
	ServiceDomain             string               `json:"serviceDomain,omitempty"`
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
	Freeze             bool   `json:"freeze"`
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
	ID              string       `json:"id"`
	OwnerKey        string       `json:"ownerKey"`
	ProfileID       string       `json:"profileId"`
	Phase           string       `json:"phase"`
	Generation      uint64       `json:"generation"`
	Version         uint64       `json:"resourceVersion"`
	Image           string       `json:"image"`
	ImageID         string       `json:"imageId,omitempty"`
	ImportedImageID string       `json:"importedImageId,omitempty"`
	Workspace       string       `json:"workspace"`
	Capabilities    Capabilities `json:"capabilities"`
	OperationID     string       `json:"operationId,omitempty"`
	Error           *APIError    `json:"error,omitempty"`
	CreatedAt       time.Time    `json:"createdAt"`
}
type boxRecord struct {
	Box               Box                `json:"box"`
	ClientID          string             `json:"clientId"`
	Profile           Profile            `json:"profile"`
	Handle            boxprovider.Handle `json:"handle"`
	ExecutionID       string             `json:"executionId,omitempty"`
	Staged            bool               `json:"staged,omitempty"`
	RestoreArchiveID  string             `json:"restoreArchiveId,omitempty"`
	RestoreComplete   bool               `json:"restoreComplete,omitempty"`
	AcceptImageChange bool               `json:"acceptImageChange,omitempty"`
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
type Lease struct {
	ID        string    `json:"id"`
	BoxID     string    `json:"boxId"`
	Purpose   string    `json:"purpose"`
	ExpiresAt time.Time `json:"expiresAt"`
}
type Route struct {
	ID    string `json:"id"`
	BoxID string `json:"boxId"`
	Port  int    `json:"port"`
	URL   string `json:"url"`
}
type Archive struct {
	// Recovery and image deletion must not depend on a retained source Box.
	ImportedImageID string            `json:"importedImageId,omitempty"`
	PreparedImage   string            `json:"preparedImage,omitempty"`
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
type keyRecord struct {
	Hash        string `json:"hash"`
	OperationID string `json:"operationId,omitempty"`
	Expired     bool   `json:"expired,omitempty"`
}
type executionRecord struct {
	Execution
	ResultObject string `json:"resultObject,omitempty"`
}
type purgeRecord struct {
	ClientID string   `json:"clientId"`
	Objects  []string `json:"objects,omitempty"`
	Records  []string `json:"records,omitempty"`
}

type State struct {
	Purges         map[string]purgeRecord         `json:"purges,omitempty"`
	ImportedImages map[string]importedImageRecord `json:"importedImages"`
	Schema         int                            `json:"schema"`
	Boxes          map[string]boxRecord           `json:"boxes"`
	Operations     map[string]operationRecord     `json:"operations"`
	Executions     map[string]executionRecord     `json:"executions"`
	Leases         map[string]Lease               `json:"leases"`
	Routes         map[string]Route               `json:"routes"`
	Archives       map[string]archiveRecord       `json:"archives"`
	Keys           map[string]keyRecord           `json:"keys"`
}

const stateSchema = 3

func newState() State {
	return State{Purges: map[string]purgeRecord{}, ImportedImages: map[string]importedImageRecord{}, Schema: stateSchema, Boxes: map[string]boxRecord{}, Operations: map[string]operationRecord{}, Executions: map[string]executionRecord{}, Leases: map[string]Lease{}, Routes: map[string]Route{}, Archives: map[string]archiveRecord{}, Keys: map[string]keyRecord{}}
}
