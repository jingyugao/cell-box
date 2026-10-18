// Package catalog manages local template metadata and image builds. It has no
// runtime provider or Kubernetes client: desired prewarm settings are metadata.
package catalog

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"time"

	"cellbox.local/cellbox/internal/image"
)

type Template struct {
	ID               string    `json:"id"`
	Name             string    `json:"name"`
	Description      string    `json:"description,omitempty"`
	DefaultWarmSlots int       `json:"defaultWarmSlots"`
	StableRevisionID string    `json:"stableRevisionId,omitempty"`
	Generation       uint64    `json:"generation"`
	CreatedAt        time.Time `json:"createdAt"`
	UpdatedAt        time.Time `json:"updatedAt"`
}

type Revision struct {
	ID                string    `json:"id"`
	TemplateID        string    `json:"templateId"`
	TemplateName      string    `json:"templateName"`
	Description       string    `json:"description,omitempty"`
	UserVersion       string    `json:"userVersion"`
	Revision          int       `json:"revision"`
	Version           string    `json:"version"`
	ProfileID         string    `json:"profileId"`
	BaseImage         string    `json:"baseImage"`
	ResolvedBaseImage string    `json:"resolvedBaseImage"`
	Image             string    `json:"image"`
	BuildCommand      string    `json:"buildCommand,omitempty"`
	RunCommand        string    `json:"runCommand,omitempty"`
	Platform          string    `json:"platform"`
	CPU               float64   `json:"cpu"`
	MemoryMiB         int64     `json:"memoryMiB"`
	WarmSlots         int       `json:"warmSlots"`
	Status            string    `json:"status"`
	Generation        uint64    `json:"generation"`
	BuildOperationID  string    `json:"buildOperationId"`
	CreatedAt         time.Time `json:"createdAt"`
	UpdatedAt         time.Time `json:"updatedAt"`
}

type BuildRequest struct {
	UserVersion  string `json:"userVersion"`
	BaseImage    string `json:"baseImage"`
	ProfileID    string `json:"profileId"`
	BuildCommand string `json:"buildCommand,omitempty"`
	RunCommand   string `json:"runCommand,omitempty"`
	Platform     string `json:"platform,omitempty"`
	WarmSlots    *int   `json:"warmSlots,omitempty"`
}

type Profile struct {
	ID        string
	CPU       float64
	MemoryMiB int64
	Snapshot  json.RawMessage
}

type BuildInput struct {
	BuildRequest
	Profile     Profile
	TemplateID  string
	OperationID string
}

type BuildResult struct {
	ResolvedBaseImage string
	Image             string
	Command           []string
	Env               map[string]string
	WorkingDir        string
}

type Builder interface {
	Build(context.Context, BuildInput, io.Writer, func(string) error) (BuildResult, error)
}

type BuilderFunc func(context.Context, BuildInput, io.Writer, func(string) error) (BuildResult, error)

func (f BuilderFunc) Build(ctx context.Context, in BuildInput, w io.Writer, stage func(string) error) (BuildResult, error) {
	return f(ctx, in, w, stage)
}

type Config struct {
	Dir              string
	DB               *sql.DB // Optional borrowed connection; the caller owns its lifetime and SQLite configuration.
	ResolveProfile   func(client, id string) (Profile, error)
	FinalizeProfile  func(Profile, BuildResult) (json.RawMessage, error)
	ImageBuild       image.BuildKitOptions
	InsecureRegistry string
	Builder          Builder // Optional injection; the default uses the configured BuildKit.
}

type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *Error) Error() string           { return e.Message }
func failure(code, message string) error { return &Error{Code: code, Message: message} }

type Operation struct {
	ID         string            `json:"id"`
	Kind       string            `json:"kind"`
	TargetID   string            `json:"targetId"`
	Status     string            `json:"status"`
	Stage      string            `json:"stage"`
	Version    uint64            `json:"version"`
	Input      BuildRequest      `json:"input"`
	CreatedAt  time.Time         `json:"createdAt"`
	StartedAt  *time.Time        `json:"startedAt,omitempty"`
	FinishedAt *time.Time        `json:"finishedAt,omitempty"`
	UpdatedAt  time.Time         `json:"updatedAt"`
	Result     map[string]string `json:"result,omitempty"`
	Error      *Error            `json:"error,omitempty"`
}

type Logs struct {
	Text       string `json:"text"`
	NextOffset int64  `json:"nextOffset"`
	EOF        bool   `json:"eof"`
}
