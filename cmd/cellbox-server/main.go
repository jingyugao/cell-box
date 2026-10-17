package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cellbox.local/cellbox/internal/objectstorage"
	"cellbox.local/cellbox/internal/service"
	"cellbox.local/cellbox/internal/telemetry"
	"cellbox.local/cellbox/internal/version"
	"crypto/sha256"
	"encoding/hex"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
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
	flags := flag.NewFlagSet("cellbox-server", flag.ContinueOnError)
	configPath := flags.String("config", "", "path to the Cellbox JSON configuration")
	kubeconfig := flags.String("kubeconfig", "", "explicit kubeconfig path (default: in-cluster credentials)")
	storageConfigPath := flags.String("object-storage-config", "", "shared S3 storage JSON configuration")
	localMetadataDir := flags.String("local-metadata-dir", "", "persistent node directory for control metadata; asset content remains in object storage")
	catalogDir := flags.String("catalog-dir", "", "persistent local directory for the SQLite template catalog and build logs")
	initializeLocalMetadata := flags.Bool("initialize-local-metadata", false, "one-time metadata import; stop all old API/controller writers first")
	leaderNamespace := flags.String("leader-election-namespace", "", "server Lease namespace (defaults to managed namespace)")
	var runtime nodeOptions
	flags.StringVar(&runtime.Node, "node", "", "single managed Kubernetes node")
	flags.StringVar(&runtime.Namespace, "namespace", "cell-box", "managed Kubernetes namespace")
	flags.StringVar(&runtime.Socket, "cri-socket", "/run/containerd/containerd.sock", "node CRI Unix socket")
	flags.BoolVar(&runtime.HostMountNamespace, "host-mount-namespace", false, "run runsc in the host mount namespace (requires privileged hostPID Pod)")
	flags.IntVar(&runtime.WarmPoolSize, "warm-pool-size", 2, "maximum idle warm Pods on this node (0 disables)")
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
		fmt.Println(version.String("cellbox-server"))
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
	if *localMetadataDir != "" {
		config.LocalMetadataDir = *localMetadataDir
	}
	if *initializeLocalMetadata {
		config.InitializeLocalMetadata = true
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

	if *catalogDir != "" {
		config.CatalogDir = *catalogDir
	}
	if err = runtime.validate(config); err != nil {
		return err
	}
	needsKubernetes := runtime.Node != ""
	for _, profile := range config.Profiles {
		needsKubernetes = needsKubernetes || profile.Provider == "resumable-k8s-pod"
	}

	signalContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctrl.SetLogger(zap.New())
	shutdownTracing, err := telemetry.InitializeNamed(signalContext, "cellbox-server")
	if err != nil {
		return err
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(ctx)
	}()
	if !needsKubernetes && config.ObjectStorage == (objectstorage.Config{}) {
		return serveUnified(signalContext, config, *dockerBinary, nil, runtime)
	}
	if *leaderNamespace == "" {
		*leaderNamespace = runtime.Namespace
	}
	var kubeConfig *rest.Config
	if *kubeconfig != "" {
		kubeConfig, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
	} else {
		kubeConfig, err = rest.InClusterConfig()
	}
	if err != nil {
		return errors.New("cannot initialize server Kubernetes credentials")
	}
	client, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		return errors.New("cannot initialize server leader election")
	}
	sum := sha256.Sum256([]byte(runtime.Namespace + "\n" + runtime.Node))
	lock := &resourcelock.LeaseLock{LeaseMeta: metav1.ObjectMeta{Namespace: *leaderNamespace, Name: "cellbox-server-" + hex.EncodeToString(sum[:8])}, Client: client.CoordinationV1(), LockConfig: resourcelock.ResourceLockConfig{Identity: string(uuid.NewUUID())}}
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
				result <- serveUnified(ctx, config, *dockerBinary, kubeConfig, runtime)
				cancelElection()
			},
			OnStoppedLeading: func() {},
		},
	})
	if err != nil {
		return errors.New("invalid Server leader election configuration")
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
					return errors.New("Server shutdown did not finish in time")
				}
			default:
				return nil
			}
		}
		select {
		case <-result:
			return errors.New("Server leader lease was lost")
		case <-time.After(20 * time.Second):
			return errors.New("Server leader lease was lost; service did not stop in time")
		}
	}
}
