package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/providers/docker"
	"cellbox.local/cellbox/internal/providers/resumable"
	"cellbox.local/cellbox/internal/service"
	"cellbox.local/cellbox/internal/version"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	flags := flag.NewFlagSet("cellbox", flag.ContinueOnError)
	configPath := flags.String("config", "", "path to the Cellbox JSON configuration")
	kubeconfig := flags.String("kubeconfig", "", "explicit kubeconfig path (default: in-cluster credentials)")
	dockerBinary := flags.String("docker", "docker", "Docker CLI executable")
	buildkitAddress := flags.String("buildkit-addr", "", "Unix BuildKit socket for REST image builds")
	imageRepository := flags.String("image-repository", "", "registry repository for prepared images")
	guestBinary := flags.String("guest-binary", "", "static Cellbox guest binary for image builds")
	buildctlBinary := flags.String("buildctl", "", "BuildKit client executable")
	showVersion := flags.Bool("version", false, "print build version and exit")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if *showVersion {
		fmt.Println(version.String("cellbox"))
		return nil
	}
	if *configPath == "" {
		return errors.New("--config is required")
	}
	file, err := os.Open(*configPath)
	if err != nil {
		return fmt.Errorf("cannot open Cellbox configuration: %w", err)
	}
	config, loadErr := service.LoadConfig(file)
	closeErr := file.Close()
	if loadErr != nil {
		return fmt.Errorf("invalid Cellbox configuration: %w", loadErr)
	}
	if closeErr != nil {
		return errors.New("cannot close Cellbox configuration")
	}
	if *buildkitAddress != "" || *imageRepository != "" || *guestBinary != "" || *buildctlBinary != "" {
		config.ImageBuild = service.ImageBuildConfig{Address: *buildkitAddress, Repository: *imageRepository, GuestBinary: *guestBinary, BuildctlBinary: *buildctlBinary}
	}

	providers := make(map[string]boxprovider.Provider)
	for _, profile := range config.Profiles {
		switch profile.Provider {
		case "docker":
			if providers["docker"] == nil {
				providers["docker"] = docker.New(*dockerBinary)
			}
		case "resumable-k8s-pod":
			if providers["resumable-k8s-pod"] == nil {
				var p *resumable.Provider
				if *kubeconfig != "" {
					p, err = resumable.NewForKubeconfig(*kubeconfig)
				} else {
					p, err = resumable.NewInCluster()
				}
				if err != nil {
					// REST configuration errors can include credential or cluster details.
					return errors.New("cannot initialize Kubernetes provider")
				}
				providers["resumable-k8s-pod"] = p
			}
		}
	}

	app, err := service.New(config, providers)
	if err != nil {
		return fmt.Errorf("cannot start Cellbox service: %w", err)
	}
	server := &http.Server{
		Addr:              config.Listen,
		Handler:           app.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    1 << 20,
	}
	signalContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	serveResult := make(chan error, 1)
	go func() { serveResult <- server.ListenAndServe() }()

	var serveErr error
	select {
	case <-signalContext.Done():
		shutdownContext, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		shutdownErr := server.Shutdown(shutdownContext)
		cancel()
		if shutdownErr != nil {
			_ = server.Close()
			serveErr = errors.New("HTTP shutdown did not finish within 15 seconds")
		}
		if err := <-serveResult; err != nil && !errors.Is(err, http.ErrServerClosed) && serveErr == nil {
			serveErr = fmt.Errorf("HTTP server stopped: %w", err)
		}
	case err := <-serveResult:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr = fmt.Errorf("HTTP server stopped: %w", err)
		}
	}
	return errors.Join(serveErr, app.Close())
}
