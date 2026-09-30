package image

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strconv"
	"strings"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"mvdan.cc/sh/v3/syntax"
)

type RegistryAuth struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type RunOptions struct {
	Image      string
	Command    []string
	Entrypoint *string
	Env        map[string]string
	WorkingDir string
	Ports      []int
	Warnings   []string
}

// ParseRunCommand reads shell syntax as data. No expansion or shell execution is used.
func ParseRunCommand(command string) (RunOptions, error) {
	out := RunOptions{Env: map[string]string{}}
	if command == "" {
		return out, nil
	}
	if len(command) > 64<<10 {
		return out, errors.New("runCommand is too long")
	}
	f, err := syntax.NewParser(syntax.Variant(syntax.LangPOSIX)).Parse(strings.NewReader(command), "")
	if err != nil || len(f.Stmts) != 1 {
		return out, errors.New("runCommand must contain one literal docker run command")
	}
	stmt := f.Stmts[0]
	call, ok := stmt.Cmd.(*syntax.CallExpr)
	if !ok || stmt.Background || stmt.Negated || stmt.Coprocess || len(stmt.Redirs) != 0 || len(call.Assigns) != 0 {
		return out, errors.New("runCommand cannot contain shell operators, assignments or redirection")
	}
	var args []string
	for _, word := range call.Args {
		value, err := literalParts(word.Parts, false)
		if err != nil {
			return out, err
		}
		args = append(args, value)
	}
	if len(args) < 3 || args[0] != "docker" || args[1] != "run" {
		return out, errors.New("runCommand must start with docker run")
	}
	args = args[2:]
	for len(args) > 0 && strings.HasPrefix(args[0], "-") {
		flag := args[0]
		args = args[1:]
		if flag == "--" {
			break
		}
		if flag == "-d" || flag == "--detach" || flag == "--rm" {
			out.Warnings = append(out.Warnings, flag+" does not change Cellbox lifecycle; use the sandbox API to destroy it")
			continue
		}
		key, value, attached := strings.Cut(flag, "=")
		if len(flag) > 2 && (strings.HasPrefix(flag, "-e") || strings.HasPrefix(flag, "-w") || strings.HasPrefix(flag, "-p")) && !strings.HasPrefix(flag, "--") {
			value, key, attached = flag[2:], flag[:2], true
		}
		switch key {
		case "-e", "--env", "-w", "--workdir", "-p", "--publish", "--entrypoint", "--name":
		default:
			return out, fmt.Errorf("unsupported docker run option %q", key)
		}
		if !attached {
			if len(args) == 0 {
				return out, fmt.Errorf("docker run option %s requires a value", key)
			}
			value, args = args[0], args[1:]
		}
		switch key {
		case "-e", "--env":
			k, v, ok := strings.Cut(value, "=")
			if !ok || !validEnvironment(k, v) {
				return out, errors.New("environment options require literal NAME=value")
			}
			out.Env[k] = v
		case "-w", "--workdir":
			if !validWorkingDir(value) {
				return out, errors.New("workdir must be a clean absolute directory")
			}
			out.WorkingDir = value
		case "--entrypoint":
			out.Entrypoint = &value
		case "--name":
			out.Warnings = append(out.Warnings, "--name is ignored; Cellbox assigns the sandbox ID")
		case "-p", "--publish":
			port, err := publishPort(value)
			if err != nil {
				return out, err
			}
			out.Ports = append(out.Ports, port)
			out.Warnings = append(out.Warnings, "published ports are metadata; create a Cellbox route to access the service")
		}
	}
	if len(args) == 0 {
		return out, errors.New("docker run image is required")
	}
	out.Image = args[0]
	if len(args) > 1 {
		out.Command = args[1:]
	}
	return out, nil
}

func literalParts(parts []syntax.WordPart, quoted bool) (string, error) {
	var out strings.Builder
	for _, part := range parts {
		switch p := part.(type) {
		case *syntax.Lit:
			value := p.Value
			var decoded strings.Builder
			for i := 0; i < len(value); i++ {
				if value[i] == '\\' && i+1 < len(value) {
					next := value[i+1]
					if !quoted || strings.ContainsRune("$`\"\\\n", rune(next)) {
						i++
						if next != '\n' {
							decoded.WriteByte(next)
						}
						continue
					}
				}
				decoded.WriteByte(value[i])
			}
			out.WriteString(decoded.String())
		case *syntax.SglQuoted:
			out.WriteString(p.Value)
		case *syntax.DblQuoted:
			value, err := literalParts(p.Parts, true)
			if err != nil {
				return "", err
			}
			out.WriteString(value)
		default:
			return "", errors.New("runCommand cannot expand variables or execute shell substitutions")
		}
	}
	return out.String(), nil
}

func validEnvironment(k, v string) bool {
	if k == "" || len(k) > 64 || len(v) > 8192 || strings.ContainsRune(v, 0) {
		return false
	}
	for i, c := range k {
		if !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c == '_' || i > 0 && c >= '0' && c <= '9') {
			return false
		}
	}
	return true
}

func validWorkingDir(dir string) bool {
	return path.IsAbs(dir) && path.Clean(dir) == dir && !strings.ContainsRune(dir, 0)
}

func publishPort(value string) (int, error) {
	if strings.Contains(value, "/") {
		var protocol string
		value, protocol, _ = strings.Cut(value, "/")
		if protocol != "tcp" {
			return 0, errors.New("only TCP published ports are supported")
		}
	}
	parts := strings.Split(value, ":")
	if len(parts) > 3 {
		return 0, errors.New("invalid published port")
	}
	for _, p := range parts[len(parts)-min(len(parts), 2):] {
		n, err := strconv.Atoi(p)
		if err != nil || n < 1 || n > 65535 {
			return 0, errors.New("published ports must be 1..65535")
		}
	}
	n, _ := strconv.Atoi(parts[len(parts)-1])
	if n == 40000 {
		return 0, errors.New("guest control port cannot be published")
	}
	return n, nil
}

func ParseRegistryReference(value, insecureRegistry string) (name.Reference, error) {
	if value == "" || len(value) > 2048 || strings.ContainsAny(value, " \t\r\n\x00") || strings.Contains(value, "://") {
		return nil, errors.New("a docker pull image reference is required, without a URL scheme")
	}
	ref, err := name.ParseReference(value)
	if err != nil {
		return nil, errors.New("invalid image reference")
	}
	if ref.Context().RegistryStr() == insecureRegistry {
		return name.ParseReference(value, name.Insecure)
	}
	return ref, nil
}

type ResolvedImage struct {
	RegistryImage v1.Image
	Source        string
	Platform      string
	Command       []string
	Env           map[string]string
	WorkingDir    string
	Warnings      []string
}

func ResolveRegistryImage(ctx context.Context, value, platform, insecureRegistry string, credentials *RegistryAuth, run RunOptions) (ResolvedImage, error) {
	var out ResolvedImage
	ref, err := ParseRegistryReference(value, insecureRegistry)
	if err != nil {
		return out, err
	}
	if platform == "" {
		platform = "linux/amd64"
	}
	// The published API guest binary currently targets Linux amd64.
	if platform != "linux/amd64" {
		return out, errors.New("image import currently supports linux/amd64")
	}
	opts := []remote.Option{remote.WithContext(ctx), remote.WithPlatform(v1.Platform{OS: "linux", Architecture: "amd64"}), remote.WithTransport(http.DefaultTransport.(*http.Transport).Clone())}
	if credentials == nil {
		opts = append(opts, remote.WithAuthFromKeychain(authn.DefaultKeychain))
	} else {
		opts = append(opts, remote.WithAuth(&authn.Basic{Username: credentials.Username, Password: credentials.Password}))
	}
	img, err := remote.Image(ref, opts...)
	if err != nil {
		return out, errors.New("cannot resolve source image; check registry access and credentials")
	}
	out.RegistryImage = img
	digest, err := img.Digest()
	if err != nil {
		return out, errors.New("cannot resolve source image digest")
	}
	config, err := img.ConfigFile()
	if err != nil {
		return out, errors.New("cannot read source image configuration")
	}
	if config.OS != "linux" || config.Architecture != "amd64" {
		return out, errors.New("source image platform does not match linux/amd64")
	}
	if len(config.Config.OnBuild) != 0 {
		return out, errors.New("images with ONBUILD instructions cannot be imported")
	}
	out.Source, out.Platform = ref.Context().Digest(digest.String()).Name(), platform
	out.Env = map[string]string{}
	for _, entry := range config.Config.Env {
		k, v, ok := strings.Cut(entry, "=")
		if !ok || !validEnvironment(k, v) {
			return out, errors.New("source image has an unsupported environment variable")
		}
		out.Env[k] = v
	}
	for k, v := range run.Env {
		out.Env[k] = v
	}
	out.WorkingDir = config.Config.WorkingDir
	if run.WorkingDir != "" {
		out.WorkingDir = run.WorkingDir
	}
	if out.WorkingDir == "" {
		out.WorkingDir = "/"
	}
	if !validWorkingDir(out.WorkingDir) {
		return out, errors.New("source image has an unsupported working directory")
	}
	entrypoint, command := config.Config.Entrypoint, config.Config.Cmd
	if run.Command != nil {
		command = run.Command
	}
	if run.Entrypoint != nil {
		entrypoint = nil
		if *run.Entrypoint != "" {
			entrypoint = []string{*run.Entrypoint}
		}
		if run.Command == nil {
			command = nil
		}
	}
	out.Command = append(append([]string{}, entrypoint...), command...)
	if len(out.Command) > 128 {
		return out, errors.New("source startup command has too many arguments")
	}
	for _, arg := range out.Command {
		if len(arg) > 8192 || strings.ContainsRune(arg, 0) {
			return out, errors.New("invalid source startup argument")
		}
	}
	out.Warnings = append(out.Warnings, run.Warnings...)
	if config.Config.User != "" {
		out.Warnings = append(out.Warnings, "image USER is replaced by the profile agent UID/GID")
	}
	return out, nil
}

// Mirror with the platform's credentials when source and destination share a
// registry. BuildKit's Docker auth config can select only one credential per
// host, while the user's pull credential need not have platform push access.
func MirrorRegistryImage(ctx context.Context, source ResolvedImage, repository, insecureRegistry string) (string, error) {
	digest, err := source.RegistryImage.Digest()
	if err != nil {
		return "", err
	}
	ref, err := ParseRegistryReference(repository+":cellbox-source-"+digest.Hex, insecureRegistry)
	if err != nil {
		return "", err
	}
	if err := remote.Write(ref, source.RegistryImage, remote.WithContext(ctx), remote.WithAuthFromKeychain(authn.DefaultKeychain)); err != nil {
		return "", errors.New("cannot mirror source image with platform registry credentials")
	}
	return ref.Context().Digest(digest.String()).Name(), nil
}
