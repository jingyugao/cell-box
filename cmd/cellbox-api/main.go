package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/objectstorage"
	"cellbox.local/cellbox/internal/providers/docker"
	"cellbox.local/cellbox/internal/providers/resumable"
	"cellbox.local/cellbox/internal/service"
	"cellbox.local/cellbox/internal/telemetry"
	"cellbox.local/cellbox/internal/version"
	"crypto/sha256"
	"encoding/hex"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
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
	flags := flag.NewFlagSet("cellbox-api", flag.ContinueOnError)
	configPath := flags.String("config", "", "path to the Cellbox JSON configuration")
	kubeconfig := flags.String("kubeconfig", "", "explicit kubeconfig path (default: in-cluster credentials)")
	storageConfigPath := flags.String("object-storage-config", "", "shared S3 storage JSON configuration")
	leaderNamespace := flags.String("leader-election-namespace", "", "API Lease namespace (required with object storage)")
	dockerBinary := flags.String("docker", "docker", "Docker CLI executable")
	buildkitAddress := flags.String("buildkit-addr", "", "Unix BuildKit socket for REST image builds")
	imageRepository := flags.String("image-repository", "", "registry repository for prepared images")
	guestBinary := flags.String("guest-binary", "", "static Cellbox guest binary for image builds")
	buildctlBinary := flags.String("buildctl", "", "BuildKit client executable")
	insecureRegistry := flags.String("image-insecure-registry", "", "explicit registry host allowed to use HTTP for image import")
	showVersion := flags.Bool("version", false, "print build version and exit")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if *showVersion {
		fmt.Println(version.String("cellbox-api"))
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
	if *insecureRegistry != "" {
		config.ImageBuild.InsecureRegistry = *insecureRegistry
	}

	if *storageConfigPath != "" {
		data, err := os.ReadFile(*storageConfigPath)
		if err != nil {
			return errors.New("cannot read object storage configuration")
		}
		if err = json.Unmarshal(data, &config.ObjectStorage); err != nil {
			return errors.New("invalid object storage configuration")
		}
		if err = config.ObjectStorage.Validate(); err != nil {
			return err
		}
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

	signalContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	shutdownTracing, err := telemetry.Initialize(signalContext)
	if err != nil {
		return err
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(ctx)
	}()
	if config.ObjectStorage == (objectstorage.Config{}) {
		return serve(signalContext, config, providers)
	}
	if *leaderNamespace == "" {
		return errors.New("--leader-election-namespace is required with object storage")
	}
	var kubeConfig *rest.Config
	if *kubeconfig != "" {
		kubeConfig, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
	} else {
		kubeConfig, err = rest.InClusterConfig()
	}
	if err != nil {
		return errors.New("cannot initialize API leader election")
	}
	client, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		return errors.New("cannot initialize API leader election")
	}
	sum := sha256.Sum256([]byte(config.ObjectStorage.Endpoint + "\n" + config.ObjectStorage.Bucket + "\n" + config.ObjectStorage.Prefix))
	lock := &resourcelock.LeaseLock{LeaseMeta: metav1.ObjectMeta{Namespace: *leaderNamespace, Name: "cellbox-api-" + hex.EncodeToString(sum[:8])}, Client: client.CoordinationV1(), LockConfig: resourcelock.ResourceLockConfig{Identity: string(uuid.NewUUID())}}
	electionContext, cancelElection := context.WithCancel(signalContext)
	defer cancelElection()
	result := make(chan error, 1)
	started := make(chan struct{})
	elector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock: lock, LeaseDuration: 45 * time.Second, RenewDeadline: 30 * time.Second, RetryPeriod: 5 * time.Second,
		// Do not release until the process has stopped all side effects. On loss,
		// cancellation stops service workers; the full lease duration fences takeover.
		ReleaseOnCancel: false,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				close(started)
				result <- serve(ctx, config, providers)
				cancelElection()
			},
			OnStoppedLeading: func() {},
		},
	})
	if err != nil {
		return errors.New("invalid API leader election configuration")
	}
	elector.Run(electionContext)
	select {
	case err := <-result:
		return err
	default:
		if signalContext.Err() != nil {
			select {
			case <-started:
				select {
				case err := <-result:
					return err
				case <-time.After(20 * time.Second):
					return errors.New("API shutdown did not finish in time")
				}
			default:
				return nil
			}
		}
		select {
		case <-result:
			return errors.New("API leader lease was lost")
		case <-time.After(20 * time.Second):
			return errors.New("API leader lease was lost; service did not stop in time")
		}
	}
}

func serve(ctx context.Context, config service.Config, providers map[string]boxprovider.Provider) error {
	app, err := service.NewContext(ctx, config, providers)
	if err != nil {
		return fmt.Errorf("cannot start Cellbox service: %w", err)
	}
	server := &http.Server{Addr: config.Listen, Handler: otelhttp.NewHandler(app.Handler(), "cellbox.http"), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 2 * time.Minute, MaxHeaderBytes: 1 << 20,
		BaseContext: func(net.Listener) context.Context { return ctx },
	}
	serveResult := make(chan error, 1)
	go func() { serveResult <- server.ListenAndServe() }()
	var serveErr error
	select {
	case <-app.Done():
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
		if ctx.Err() == nil && serveErr == nil {
			serveErr = errors.New("Cellbox stopped after a durable state write failure")
		}
	case err := <-serveResult:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr = fmt.Errorf("HTTP server stopped: %w", err)
		}
	}
	return errors.Join(serveErr, app.Close())
}
