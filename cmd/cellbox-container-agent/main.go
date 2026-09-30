package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"cellbox.local/cellbox/internal/guest"
	"cellbox.local/cellbox/internal/guestapi"
)

func main() {
	if err := mainErr(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func mainErr() error {
	if len(os.Args) < 2 {
		return errors.New("usage: cellbox-container-agent serve|token|tool")
	}
	switch os.Args[1] {
	case "__child":
		return guest.RunChild(os.Args[2:])
	case "token":
		if os.Geteuid() != 0 {
			return errors.New("token discovery requires root")
		}
		b, err := os.ReadFile(guestapi.TokenPath)
		if err != nil {
			return err
		}
		_, err = os.Stdout.Write(b)
		return err
	case "tool":
		if len(os.Args) < 3 {
			return errors.New("usage: cellbox-container-agent tool <id> [args...]")
		}
		r, err := guest.ToolClient(os.Args[2], os.Args[3:])
		if err != nil {
			return err
		}
		_, _ = io.WriteString(os.Stdout, r.Stdout)
		_, _ = io.WriteString(os.Stderr, r.Stderr)
		if r.ExitCode != 0 {
			return fmt.Errorf("tool exited %d", r.ExitCode)
		}
		return nil
	case "serve":
		fs := flag.NewFlagSet("serve", flag.ContinueOnError)
		encoded := fs.String("config-base64", "", "base64 JSON guest config")
		staged := fs.Bool("staged", false, "start without workload")
		if err := fs.Parse(os.Args[2:]); err != nil {
			return err
		}
		if *encoded == "" {
			return errors.New("--config-base64 is required")
		}
		b, err := base64.StdEncoding.DecodeString(*encoded)
		if err != nil {
			return err
		}
		if len(b) > 1<<20 {
			return errors.New("guest config too large")
		}
		var c guestapi.Config
		d := json.NewDecoder(strings.NewReader(string(b)))
		d.DisallowUnknownFields()
		if err := d.Decode(&c); err != nil {
			return err
		}
		token, err := guest.Bootstrap(c)
		if err != nil {
			return err
		}
		self, err := os.Executable()
		if err != nil {
			return err
		}
		s, err := guest.NewServer(c, token, self, *staged)
		if err != nil {
			return err
		}
		defer s.Shutdown()
		return s.ListenAndServe()
	default:
		return errors.New("unknown command")
	}
}
