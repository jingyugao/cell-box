package service

import (
	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/guestapi"
	"time"
)

type Client struct {
	ID           string `json:"id"`
	TokenEnv     string `json:"tokenEnv"`
	AuthorizeURL string `json:"authorizeUrl,omitempty"`
	Token        string `json:"-"`
}
type Profile struct {
	ID                     string          `json:"id"`
	Provider               string          `json:"provider"`
	Image                  string          `json:"image"`
	NodeName               string          `json:"nodeName,omitempty"`
	Namespace              string          `json:"namespace,omitempty"`
	DebugReadOnlyHostPath  string          `json:"debugReadOnlyHostPath,omitempty"`
	DebugReadWriteHostPath string          `json:"debugReadWriteHostPath,omitempty"`
	CPU                    float64         `json:"cpu,omitempty"`
	MemoryMiB              int64           `json:"memoryMiB,omitempty"`
	Guest                  guestapi.Config `json:"guest"`
	Clients                []string        `json:"clients"`
}
type Config struct {
	Listen                string           `json:"listen"`
	DataDir               string           `json:"dataDir"`
	PublicURL             string           `json:"publicUrl,omitempty"`
	ServiceDomain         string           `json:"serviceDomain,omitempty"`
	Clients               []Client         `json:"clients"`
	Profiles              []Profile        `json:"profiles"`
	StartupTimeoutSeconds int              `json:"startupTimeoutSeconds,omitempty"`
	ImageBuild            ImageBuildConfig `json:"imageBuild,omitempty"`
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
	Exec           bool   `json:"exec"`
	Files          bool   `json:"files"`
	HTTP           bool   `json:"http"`
	WebSocket      bool   `json:"websocket"`
	PTY            bool   `json:"pty"`
	ReconnectExec  bool   `json:"reconnectExec"`
	Freeze         bool   `json:"freeze"`
	Suspend        string `json:"suspend"`
	Archives       string `json:"archives"`
	ProtectedTools bool   `json:"protectedTools"`
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
	Box              Box                `json:"box"`
	ClientID         string             `json:"clientId"`
	Profile          Profile            `json:"profile"`
	Handle           boxprovider.Handle `json:"handle"`
	ExecutionID      string             `json:"executionId,omitempty"`
	Staged           bool               `json:"staged,omitempty"`
	RestoreArchiveID string             `json:"restoreArchiveId,omitempty"`
	RestoreComplete  bool               `json:"restoreComplete,omitempty"`
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
type Grant struct {
	ID        string    `json:"id"`
	RouteID   string    `json:"routeId"`
	Subject   string    `json:"subject"`
	ExpiresAt time.Time `json:"expiresAt"`
	Revoked   bool      `json:"revoked"`
}
type grantRecord struct {
	Grant     Grant  `json:"grant"`
	TokenHash string `json:"tokenHash"`
}
type AccessRequest struct {
	ID          string    `json:"id"`
	RouteID     string    `json:"routeId"`
	BoxID       string    `json:"boxId"`
	CallbackURL string    `json:"callbackUrl"`
	ExpiresAt   time.Time `json:"expiresAt"`
	Approved    bool      `json:"approved"`
	Consumed    bool      `json:"consumed"`
}
type accessRecord struct {
	Request     AccessRequest `json:"request"`
	BrowserHash string        `json:"browserHash"`
	CodeHash    string        `json:"codeHash,omitempty"`
	GrantID     string        `json:"grantId,omitempty"`
	ReturnPath  string        `json:"returnPath"`
}
type sessionRecord struct {
	TokenHash string `json:"tokenHash"`
	GrantID   string `json:"grantId"`
}
type Archive struct {
	ID          string            `json:"id"`
	SourceBoxID string            `json:"sourceBoxId"`
	ProfileID   string            `json:"profileId"`
	ImageID     string            `json:"imageId"`
	Agent       guestapi.Identity `json:"agent"`
	SHA256      string            `json:"sha256"`
	Size        int64             `json:"size"`
	Consistency string            `json:"consistency"`
	CreatedAt   time.Time         `json:"createdAt"`
}
type archiveRecord struct {
	Archive  Archive `json:"archive"`
	ClientID string  `json:"clientId"`
}
type keyRecord struct {
	Hash        string `json:"hash"`
	OperationID string `json:"operationId"`
}
type State struct {
	ImportedImages map[string]importedImageRecord `json:"importedImages"`
	Schema         int                            `json:"schema"`
	Boxes          map[string]boxRecord           `json:"boxes"`
	Operations     map[string]operationRecord     `json:"operations"`
	Executions     map[string]Execution           `json:"executions"`
	Leases         map[string]Lease               `json:"leases"`
	Routes         map[string]Route               `json:"routes"`
	Grants         map[string]grantRecord         `json:"grants"`
	Access         map[string]accessRecord        `json:"access"`
	Sessions       map[string]sessionRecord       `json:"sessions"`
	Archives       map[string]archiveRecord       `json:"archives"`
	Keys           map[string]keyRecord           `json:"keys"`
}

func newState() State {
	return State{ImportedImages: map[string]importedImageRecord{}, Schema: 2, Boxes: map[string]boxRecord{}, Operations: map[string]operationRecord{}, Executions: map[string]Execution{}, Leases: map[string]Lease{}, Routes: map[string]Route{}, Grants: map[string]grantRecord{}, Access: map[string]accessRecord{}, Sessions: map[string]sessionRecord{}, Archives: map[string]archiveRecord{}, Keys: map[string]keyRecord{}}
}
