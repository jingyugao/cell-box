// Package resumable adapts the node-local ResumablePod operator to Cellbox.
package resumable

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/guestapi"
	"cellbox.local/cellbox/internal/homevolume"
	"cellbox.local/cellbox/internal/inventory"
	"cellbox.local/cellbox/internal/runtimecontrol"
	"cellbox.local/cellbox/internal/telemetry"
	"cellbox.local/cellbox/internal/warmtemplates"
	core "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const name = "resumable-k8s-pod"
const managedLabel = "cellbox.local/managed"
const boxLabel = "cellbox.local/box-id"

var validBoxID = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)
var validImage = regexp.MustCompile(`^[a-z0-9][a-z0-9._:/-]*@sha256:[a-f0-9]{64}$`)

type Provider struct {
	Client client.Client
	// Local connects HTTP to the embedded runtime and its durable local journal.
	Local                  *runtimecontrol.Client
	CacheImageLocal        func(context.Context, string, string, string) error
	CreateVolumeLocal      func(context.Context, boxprovider.VolumeSpec) error
	DeleteVolumeLocal      func(context.Context, boxprovider.VolumeSpec) error
	WithStoppedVolumeLocal func(context.Context, boxprovider.VolumeSpec, func(string) error) error
	ConfirmExecutionLocal  func(context.Context, *api.ResumablePod, *core.Pod) (*api.Execution, error)
	warmMu                 sync.RWMutex
	warmSnapshot           warmtemplates.Snapshot
}

func New(c client.Client) *Provider { return &Provider{Client: c} }
func (*Provider) Name() string      { return name }

func crName(boxID string) (string, error) {
	if len(boxID) == 0 || len(boxID) > 55 || !validBoxID.MatchString(boxID) {
		return "", fmt.Errorf("box ID must be a DNS label of at most 55 characters")
	}
	return "cellbox-" + boxID, nil
}

// buildWanted is shared by normal Box creation and warm-template preparation
// so both paths produce the same execution Spec.
func buildWanted(spec boxprovider.Spec) (*api.ResumablePod, error) {
	crname, err := crName(spec.BoxID)
	if err != nil {
		return nil, err
	}
	if spec.Namespace == "" || spec.NodeName == "" {
		return nil, errors.New("namespace and node name are required")
	}
	if spec.HomeVolumeUID != "" && !spec.PersistentHome {
		return nil, errors.New("homeVolumeUID requires persistentHome")
	}
	if spec.PersistentHome && spec.HomeVolumeUID == "" && spec.Mounts == nil {
		return nil, errors.New("persistentHome requires an independent volume")
	}
	if spec.Mounts != nil {
		if len(spec.Mounts) == 0 {
			return nil, errors.New("mounts must not be empty when specified")
		}
		for i, mount := range spec.Mounts {
			if _, err := homevolume.Path(homevolume.DefaultBase, mount.UID); err != nil {
				return nil, fmt.Errorf("mount %d UID: %w", i, err)
			}
			if !path.IsAbs(mount.MountPath) || path.Clean(mount.MountPath) != mount.MountPath || mount.MountPath == "/" || len(mount.MountPath) > 4096 || strings.ContainsAny(mount.MountPath, "\x00\r\n") {
				return nil, fmt.Errorf("mount %d path must be a clean absolute path other than /", i)
			}
		}
	}
	if !validImage.MatchString(spec.Image) {
		return nil, errors.New("immutable image repository@sha256:<digest> is required")
	}
	if host := spec.SharedReadOnlyHostPath; host != "" &&
		(!path.IsAbs(host) || path.Clean(host) != host || host == "/" || len(host) > 4096 || strings.ContainsAny(host, "\x00\r\n")) {
		return nil, errors.New("invalid sharedReadOnlyHostPath")
	}
	if host := spec.DebugReadOnlyHostPath; host != "" &&
		(!path.IsAbs(host) || path.Clean(host) != host || host == "/" || len(host) > 4096 || strings.ContainsAny(host, "\x00\r\n")) {
		return nil, errors.New("debug read-only host path must be a clean absolute directory path other than /")
	}
	if host := spec.DebugReadWriteHostPath; host != "" &&
		(spec.DebugReadOnlyHostPath != "" || !path.IsAbs(host) || path.Clean(host) != host || host == "/" || len(host) > 4096 || strings.ContainsAny(host, "\x00\r\n")) {
		return nil, errors.New("debug read-write host path must be a clean absolute directory path other than /")
	}
	if math.IsNaN(spec.CPU) || math.IsInf(spec.CPU, 0) || spec.CPU < 0 || spec.MemoryMiB < 0 {
		return nil, errors.New("resource limits must be finite and nonnegative")
	}
	// Guard unit conversion overflow, without imposing a resource allocation policy.
	if math.Ceil(spec.CPU*1000) >= float64(math.MaxInt64) || spec.MemoryMiB > math.MaxInt64/(1024*1024) {
		return nil, errors.New("resource limits overflow Kubernetes quantity units")
	}
	config, err := json.Marshal(spec.Config)
	if err != nil {
		return nil, fmt.Errorf("encode guest configuration: %w", err)
	}
	argv := []string{"serve", "--config-base64", base64.StdEncoding.EncodeToString(config)}
	if spec.Staged {
		argv = append(argv, "--staged")
	}
	var limits core.ResourceList
	if spec.CPU > 0 || spec.MemoryMiB > 0 {
		limits = make(core.ResourceList)
		if spec.CPU > 0 {
			limits[core.ResourceCPU] = *resource.NewMilliQuantity(int64(math.Ceil(spec.CPU*1000)), resource.DecimalSI)
		}
		if spec.MemoryMiB > 0 {
			limits[core.ResourceMemory] = *resource.NewQuantity(spec.MemoryMiB*1024*1024, resource.BinarySI)
		}
	}
	zero := int64(0)
	no := false
	container := core.Container{
		Name: "cellbox", Image: spec.Image, ImagePullPolicy: core.PullIfNotPresent,
		Command: []string{guestapi.Binary}, Args: argv,
		Ports:     []core.ContainerPort{{Name: "guest", ContainerPort: guestapi.Port, Protocol: core.ProtocolTCP}},
		Resources: core.ResourceRequirements{Requests: core.ResourceList{core.ResourceCPU: resource.MustParse("0"), core.ResourceMemory: resource.MustParse("0")}, Limits: limits},
		SecurityContext: &core.SecurityContext{RunAsUser: &zero, RunAsNonRoot: &no, Privileged: &no, AllowPrivilegeEscalation: &no,
			Capabilities: &core.Capabilities{Drop: []core.Capability{"ALL"}, Add: []core.Capability{"CHOWN", "SETUID", "SETGID", "FOWNER", "DAC_OVERRIDE"}}},
		ReadinessProbe: &core.Probe{ProbeHandler: core.ProbeHandler{TCPSocket: &core.TCPSocketAction{Port: intstr.FromInt32(guestapi.Port)}}, PeriodSeconds: 1},
	}
	wanted := &api.ResumablePod{
		TypeMeta:   meta.TypeMeta{APIVersion: api.GroupVersion.String(), Kind: api.Kind},
		ObjectMeta: meta.ObjectMeta{Name: crname, Namespace: spec.Namespace, Labels: map[string]string{managedLabel: "true", boxLabel: spec.BoxID}},
		Spec: api.Spec{NodeName: spec.NodeName, DesiredState: "Running", Container: container,
			SharedReadOnlyHostPath: spec.SharedReadOnlyHostPath, DebugReadOnlyHostPath: spec.DebugReadOnlyHostPath,
			DebugReadWriteHostPath: spec.DebugReadWriteHostPath, PersistentHome: spec.PersistentHome, HomeVolumeUID: spec.HomeVolumeUID, Mounts: append([]api.VolumeMount(nil), spec.Mounts...),
			ServicePorts: []core.ServicePort{{Name: "guest", Port: guestapi.Port, TargetPort: intstr.FromInt32(guestapi.Port), Protocol: core.ProtocolTCP}}},
	}
	if len(spec.Inventory) > 0 {
		var record inventory.Record
		if err = json.Unmarshal(spec.Inventory, &record); err != nil || record.ClientID == "" {
			return nil, errors.New("invalid inventory metadata")
		}
		wanted.Annotations = map[string]string{inventory.Annotation: string(spec.Inventory)}
		wanted.Labels[inventory.ClientLabel] = inventory.ClientValue(record.ClientID)
	}
	return wanted, nil
}

func (p *Provider) Create(ctx context.Context, spec boxprovider.Spec) (boxprovider.Handle, error) {
	if p == nil || p.Client == nil {
		return boxprovider.Handle{}, errors.New("Kubernetes client is required")
	}
	wanted, err := buildWanted(spec)
	if err != nil {
		return boxprovider.Handle{}, err
	}

	err = p.Client.Create(ctx, wanted)
	if apierrors.IsAlreadyExists(err) {
		actual := &api.ResumablePod{}
		if err = p.Client.Get(ctx, client.ObjectKeyFromObject(wanted), actual); err != nil {
			return boxprovider.Handle{}, err
		}
		if err = matchExisting(actual, wanted); err != nil {
			return boxprovider.Handle{}, err
		}
		wanted = actual
	} else if err != nil {
		return boxprovider.Handle{}, err
	}
	if wanted.UID == "" {
		actual := &api.ResumablePod{}
		if err = p.Client.Get(ctx, client.ObjectKeyFromObject(wanted), actual); err != nil {
			return boxprovider.Handle{}, err
		}
		wanted = actual
	}
	if wanted.UID == "" {
		return boxprovider.Handle{}, errors.New("created ResumablePod has no UID")
	}
	return boxprovider.Handle{Provider: name, ID: string(wanted.UID), Name: wanted.Name, ImageID: spec.Image, Namespace: wanted.Namespace, NodeName: spec.NodeName}, nil
}

func matchExisting(actual, wanted *api.ResumablePod) error {
	if actual.UID == "" || actual.DeletionTimestamp != nil || actual.Labels[managedLabel] != "true" || actual.Labels[boxLabel] != wanted.Labels[boxLabel] || actual.Annotations[inventory.Annotation] != wanted.Annotations[inventory.Annotation] || actual.Labels[inventory.ClientLabel] != wanted.Labels[inventory.ClientLabel] || actual.Spec.NodeName != wanted.Spec.NodeName || actual.Spec.SharedReadOnlyHostPath != wanted.Spec.SharedReadOnlyHostPath || actual.Spec.DebugReadOnlyHostPath != wanted.Spec.DebugReadOnlyHostPath || actual.Spec.DebugReadWriteHostPath != wanted.Spec.DebugReadWriteHostPath || actual.Spec.PersistentHome != wanted.Spec.PersistentHome || actual.Spec.HomeVolumeUID != wanted.Spec.HomeVolumeUID || !apiequality.Semantic.DeepEqual(actual.Spec.Mounts, wanted.Spec.Mounts) || !apiequality.Semantic.DeepEqual(actual.Spec.Container, wanted.Spec.Container) || !apiequality.Semantic.DeepEqual(actual.Spec.ServicePorts, wanted.Spec.ServicePorts) {
		return fmt.Errorf("ResumablePod %s already exists with different ownership or immutable profile", actual.Name)
	}
	return nil
}

func (p *Provider) get(ctx context.Context, h boxprovider.Handle) (*api.ResumablePod, error) {
	if p == nil || p.Client == nil {
		return nil, errors.New("Kubernetes client is required")
	}
	if h.Provider != name || h.ID == "" || h.Name == "" || h.Namespace == "" || h.NodeName == "" || !strings.HasPrefix(h.Name, "cellbox-") {
		return nil, errors.New("invalid resumable provider handle")
	}
	w := &api.ResumablePod{}
	err := p.Client.Get(ctx, client.ObjectKey{Namespace: h.Namespace, Name: h.Name}, w)
	if apierrors.IsNotFound(err) {
		return nil, boxprovider.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if string(w.UID) != h.ID || w.Labels[managedLabel] != "true" || w.Labels[boxLabel] != strings.TrimPrefix(h.Name, "cellbox-") || w.Spec.NodeName != h.NodeName {
		return nil, errors.New("ResumablePod ownership or identity changed")
	}
	return w, nil
}

func (p *Provider) Inspect(ctx context.Context, h boxprovider.Handle) (boxprovider.Observation, error) {
	w, err := p.get(ctx, h)
	if errors.Is(err, boxprovider.ErrNotFound) {
		return boxprovider.Observation{Phase: "deleted"}, nil
	}
	if err != nil {
		return boxprovider.Observation{}, err
	}
	obs := boxprovider.Observation{ExecutionID: w.Status.PodUID, Message: w.Status.Message, Generation: uint64(w.Status.Cycle), TransitionID: w.Spec.TransitionID, ObservedTransitionID: w.Status.ObservedTransitionID,
		GuestReady: w.Spec.TransitionID != "" && w.Status.ResumeReadyTransitionID == w.Spec.TransitionID && w.Status.ObservedTransitionID == w.Spec.TransitionID && w.Status.ObservedGeneration == w.Generation}
	if w.DeletionTimestamp != nil {
		obs.Phase = "deleting"
		return obs, nil
	}
	switch w.Status.Phase {
	case "", "Creating":
		obs.Phase = "creating"
		// Pending Pods cannot serve I/O. Publish their identity together with
		// readiness instead of requiring a separate durable generation update.
		obs.ExecutionID = ""
	case "Restoring":
		obs.Phase = "resuming"
		obs.ExecutionID = ""
	case "Running":
		obs.Phase, err = p.inspectRunning(ctx, h, w)
		if err != nil {
			return boxprovider.Observation{}, err
		}
	case "Checkpointing", "Suspending":
		obs.Phase = "suspending"
	case "Suspended":
		obs.Phase = "suspended"
		obs.ExecutionID = ""
	case "Failing", "Failed":
		obs.Phase = "failed"
	case "Deleting":
		obs.Phase = "deleting"
	default:
		obs.Phase = "failed"
		obs.Message = "unknown operator phase: " + w.Status.Phase
	}
	return obs, nil
}

// inspectRunning observes execution identity and terminal Pod state only.
// Asynchronous readiness probes do not change the box lifecycle phase.
func (p *Provider) inspectRunning(ctx context.Context, h boxprovider.Handle, w *api.ResumablePod) (string, error) {
	if record, err := p.localExecution(ctx, h, w.Status.PodUID); record != nil || err != nil {
		if err != nil {
			return "", err
		}
		return "running", nil
	}
	if w.Status.PodName == "" || w.Status.PodUID == "" {
		return "failed", nil
	}
	pod := &core.Pod{}
	err := p.Client.Get(ctx, client.ObjectKey{Namespace: w.Namespace, Name: w.Status.PodName}, pod)
	if err != nil && !apierrors.IsNotFound(err) {
		return "", err
	}
	// The CR may have switched to a new Pod while the previous Pod was read.
	// Confirm the observed identity before treating a missing or stale Pod as lost.
	current, getErr := p.get(ctx, h)
	if getErr != nil {
		return "", getErr
	}
	// Condition-only updates are asynchronous health observations, not a new
	// execution. Fence lifecycle/spec changes without restarting the wait each
	// time kubelet's readiness result propagates through the controller.
	if current.UID != w.UID || current.Generation != w.Generation || current.DeletionTimestamp != nil || current.Spec.DesiredState != w.Spec.DesiredState || current.Status.Phase != w.Status.Phase || current.Status.Cycle != w.Status.Cycle || current.Status.PodName != w.Status.PodName || current.Status.PodUID != w.Status.PodUID || !apiequality.Semantic.DeepEqual(current.Status.Execution, w.Status.Execution) {
		return "", errors.New("ResumablePod changed during lifecycle inspection")
	}
	if apierrors.IsNotFound(err) {
		return "failed", nil
	}
	owner := meta.GetControllerOf(pod)
	if owner == nil || owner.UID != w.UID || owner.Kind != api.Kind || owner.APIVersion != api.GroupVersion.String() || string(pod.UID) != w.Status.PodUID {
		return "failed", nil
	}
	if pod.DeletionTimestamp != nil || pod.Status.Phase == core.PodFailed || pod.Status.Phase == core.PodSucceeded {
		return "failed", nil
	}
	return "running", nil
}

var errExecutionLost = errors.New("current Pod execution lost")

func (p *Provider) readyPod(ctx context.Context, w *api.ResumablePod) (*core.Pod, error) {
	if w.Status.Phase != "Running" || w.Status.PodName == "" || w.Status.PodUID == "" {
		return nil, boxprovider.ErrNotReady
	}
	pod := &core.Pod{}
	err := p.Client.Get(ctx, client.ObjectKey{Namespace: w.Namespace, Name: w.Status.PodName}, pod)
	if apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("%w: Pod disappeared", errExecutionLost)
	}
	if err != nil {
		return nil, err
	}
	owner := meta.GetControllerOf(pod)
	if owner == nil || owner.UID != w.UID || owner.Kind != api.Kind || owner.APIVersion != api.GroupVersion.String() || string(pod.UID) != w.Status.PodUID {
		return nil, fmt.Errorf("%w: Pod ownership or UID changed", errExecutionLost)
	}
	if pod.DeletionTimestamp != nil || pod.Status.Phase == core.PodFailed || pod.Status.Phase == core.PodSucceeded {
		return nil, fmt.Errorf("%w: Pod exited or is deleting", errExecutionLost)
	}
	// Direct Guest traffic must respect the controller's snapshot invalidation
	// boundary, just as traffic selected by the Service does.
	if pod.Labels[api.ServingLabel] != "true" {
		return nil, boxprovider.ErrNotReady
	}
	if api.AvailableExecution(w, pod) {
		return pod, nil
	}
	return nil, boxprovider.ErrNotReady
}

func (p *Provider) Action(ctx context.Context, h boxprovider.Handle, action string) error {
	return p.ActionTransition(ctx, h, action, "", "")
}

func (p *Provider) ActionTransition(ctx context.Context, h boxprovider.Handle, action, expectedPrevious, transitionID string) (retErr error) {
	var desired string
	switch action {
	case "suspend":
		desired = "Suspended"
	case "resume":
		desired = "Running"
	case "freeze", "unfreeze":
		return boxprovider.ErrUnsupported
	default:
		return boxprovider.ErrUnsupported
	}
	if p.Local != nil {
		if err := p.Local.Flush(ctx, client.ObjectKey{Namespace: h.Namespace, Name: h.Name}); err != nil {
			return err
		}
	}
	w, err := p.get(ctx, h)
	if err != nil {
		return err
	}
	if w.DeletionTimestamp != nil {
		return errors.New("ResumablePod is deleting")
	}
	if transitionID != "" {
		if w.Spec.TransitionID == transitionID {
			// Recovery/retry of the same durable Cellbox operation.
			if w.Spec.DesiredState == desired {
				return nil
			}
		} else {
			if w.Spec.TransitionID != expectedPrevious {
				return boxprovider.ErrStaleTransition
			}
			if expectedPrevious != "" && w.Status.ObservedTransitionID != expectedPrevious {
				return boxprovider.ErrStaleTransition
			}
			legacyResumeInFlight := action == "resume" && expectedPrevious == "" && w.Spec.TransitionID == "" &&
				w.Spec.DesiredState == "Running" && (w.Status.Phase == "Restoring" || w.Status.Phase == "Running")
			if action == "resume" && w.Status.Phase != "Suspended" && !legacyResumeInFlight {
				return boxprovider.ErrStaleTransition
			}
			if action == "suspend" && w.Status.Phase != "Running" && !(expectedPrevious == "" && w.Spec.DesiredState == "Suspended" && (w.Status.Phase == "Checkpointing" || w.Status.Phase == "Suspending")) {
				return boxprovider.ErrStaleTransition
			}
		}
	}
	if w.Status.Phase == "Failed" || w.Status.Phase == "Failing" {
		return errors.New("ResumablePod failed; explicit operator retry is required")
	}
	if w.Spec.DesiredState == desired && (transitionID == "" || w.Spec.TransitionID == transitionID) {
		return nil
	}
	old := w.DeepCopy()
	telemetry.InjectOperation(ctx, w)
	w.Spec.DesiredState = desired
	if transitionID != "" {
		w.Spec.TransitionID = transitionID
	}
	// An optimistic update prevents stale operations from changing a newer CR.
	if err = p.Client.Patch(ctx, w, client.MergeFromWithOptions(old, client.MergeFromWithOptimisticLock{})); err != nil {
		return err
	}
	return nil
}

var archiveIDPattern = regexp.MustCompile(`^arc-[0-9a-f]{32}$`)
var archiveSHA256Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

func validWorkspaceArchiveTarget(w *api.ResumablePod, h boxprovider.Handle) error {
	if w.DeletionTimestamp != nil || !w.Spec.PersistentHome || w.Status.PodUID != "" || w.Status.PodName != "" || w.Spec.NodeName != h.NodeName {
		return errors.New("workspace archive requires a stopped persistent HOME")
	}
	if w.Status.Phase == "Suspended" && w.Spec.DesiredState == "Suspended" && w.Status.Snapshot != "" {
		return nil
	}
	if w.Status.Phase == "Failed" {
		return nil
	}
	return errors.New("workspace archive requires a suspended or failed execution")
}

// CaptureWorkspaceArchive asks the owning node controller to package a stopped
// persistent HOME. It never changes DesiredState or creates a Pod.
func (p *Provider) CaptureWorkspaceArchive(ctx context.Context, h boxprovider.Handle, id string) (int64, string, error) {
	if !archiveIDPattern.MatchString(id) {
		return 0, "", errors.New("invalid archive identifier")
	}
	if p == nil || p.Client == nil || p.Local == nil {
		return 0, "", errors.New("embedded Kubernetes runtime control is required")
	}
	w, err := p.get(ctx, h)
	if err != nil {
		return 0, "", err
	}
	if err = validWorkspaceArchiveTarget(w, h); err != nil {
		return 0, "", err
	}
	expectedSnapshot := w.Status.Snapshot
	if raw := w.Annotations[api.ArchiveResultAnnotation]; raw != "" {
		var prior api.ArchiveCaptureResult
		if json.Unmarshal([]byte(raw), &prior) == nil && prior.ID == id {
			if prior.Snapshot != expectedSnapshot || prior.SourceUID != string(w.UID) || prior.SourceGeneration != w.Generation {
				return 0, "", errors.New("workspace source changed since archive capture")
			}
			if prior.Error != "" || prior.Size < 1 || !archiveSHA256Pattern.MatchString(prior.SHA256) {
				return 0, "", errors.New("workspace archive request did not complete successfully")
			}
			p.clearArchiveCapture(h, id, true)
			return prior.Size, prior.SHA256, nil
		}
	}
	if raw := w.Annotations[api.ArchiveRequestAnnotation]; raw != "" {
		var request api.ArchiveCaptureRequest
		if err = json.Unmarshal([]byte(raw), &request); err != nil ||
			(request.ID != id && time.Now().Before(request.ExpiresAt)) ||
			(request.ID == id && request.Snapshot != expectedSnapshot) {
			return 0, "", errors.New("another workspace archive request is pending")
		}
		if request.ID != id || !time.Now().Before(request.ExpiresAt) {
			w.Annotations[api.ArchiveRequestAnnotation] = ""
		}
	}
	old := w.DeepCopy()
	if w.Annotations == nil {
		w.Annotations = map[string]string{}
	}
	request := api.ArchiveCaptureRequest{ID: id, Snapshot: expectedSnapshot, ExpiresAt: time.Now().UTC().Add(14 * time.Minute)}
	encoded, _ := json.Marshal(request)
	w.Annotations[api.ArchiveRequestAnnotation] = string(encoded)
	delete(w.Annotations, api.ArchiveResultAnnotation)
	if err = p.Client.Patch(ctx, w, client.MergeFromWithOptions(old, client.MergeFromWithOptimisticLock{})); err != nil {
		return 0, "", err
	}
	completed := false
	defer func() {
		if !completed {
			p.clearArchiveCapture(h, id, false)
		}
	}()

	waitCtx, cancel := context.WithTimeout(ctx, 14*time.Minute)
	defer cancel()
	snapshots, stop, err := p.Local.Subscribe(waitCtx, client.ObjectKey{Namespace: h.Namespace, Name: h.Name})
	if err != nil {
		return 0, "", err
	}
	defer stop()
	for {
		select {
		case <-waitCtx.Done():
			return 0, "", waitCtx.Err()
		case snapshot := <-snapshots:
			if snapshot.Deleted || !validObservationIdentity(snapshot.Resource, h) {
				return 0, "", boxprovider.ErrNotFound
			}
			w = snapshot.Resource
		}
		if err = validWorkspaceArchiveTarget(w, h); err != nil || w.Status.Snapshot != expectedSnapshot {
			return 0, "", errors.New("workspace archive source changed")
		}
		if raw := w.Annotations[api.ArchiveResultAnnotation]; raw != "" {
			var result api.ArchiveCaptureResult
			if json.Unmarshal([]byte(raw), &result) != nil {
				return 0, "", errors.New("invalid workspace archive result")
			}
			if result.ID == id && result.Snapshot == expectedSnapshot {
				if result.Error != "" {
					p.clearArchiveCapture(h, id, false)
					completed = true
					return 0, "", errors.New("node workspace archive capture failed")
				}
				if result.SourceUID != string(w.UID) || result.SourceGeneration != w.Generation || result.Size < 1 || !archiveSHA256Pattern.MatchString(result.SHA256) {
					return 0, "", errors.New("node returned invalid workspace archive metadata")
				}
				p.clearArchiveCapture(h, id, true)
				completed = true
				return result.Size, result.SHA256, nil
			}
		}
		var current api.ArchiveCaptureRequest
		if raw := w.Annotations[api.ArchiveRequestAnnotation]; raw == "" || json.Unmarshal([]byte(raw), &current) != nil || current.ID != id || current.Snapshot != expectedSnapshot {
			return 0, "", errors.New("workspace archive request was replaced")
		}
		if !time.Now().Before(current.ExpiresAt) {
			return 0, "", errors.New("workspace archive request expired")
		}
	}
}

func (p *Provider) clearArchiveCapture(h boxprovider.Handle, id string, preserveResult bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	w, err := p.get(ctx, h)
	if err != nil || w.Annotations[api.ArchiveRequestAnnotation] == "" {
		return
	}
	var request api.ArchiveCaptureRequest
	if json.Unmarshal([]byte(w.Annotations[api.ArchiveRequestAnnotation]), &request) != nil || request.ID != id {
		return
	}
	old := w.DeepCopy()
	delete(w.Annotations, api.ArchiveRequestAnnotation)
	if !preserveResult {
		delete(w.Annotations, api.ArchiveResultAnnotation)
	}
	_ = p.Client.Patch(ctx, w, client.MergeFromWithOptions(old, client.MergeFromWithOptimisticLock{}))
}

func (p *Provider) Destroy(ctx context.Context, h boxprovider.Handle) error {
	if p.Local != nil {
		if err := p.Local.Flush(ctx, client.ObjectKey{Namespace: h.Namespace, Name: h.Name}); err != nil {
			return err
		}
	}
	w, err := p.get(ctx, h)
	if errors.Is(err, boxprovider.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if w.DeletionTimestamp != nil {
		return nil
	}
	uid := w.UID
	err = p.Client.Delete(ctx, w, &client.DeleteOptions{Preconditions: &meta.Preconditions{UID: &uid}})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func (p *Provider) Guest(ctx context.Context, h boxprovider.Handle) (boxprovider.Connection, error) {
	return p.GuestForExecution(ctx, h, "")
}

func containerIdentity(pod *core.Pod, container string) string {
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == container && status.ContainerID != "" && status.State.Running != nil {
			return fmt.Sprintf("%s/%d/%s", status.ContainerID, status.RestartCount, status.State.Running.StartedAt.String())
		}
	}
	return ""
}

func (p *Provider) GuestForExecution(ctx context.Context, h boxprovider.Handle, expected string) (boxprovider.Connection, error) {
	if execution, err := p.localExecution(ctx, h, expected); execution != nil || err != nil {
		if err != nil {
			return boxprovider.Connection{}, err
		}
		return boxprovider.Connection{URL: "http://" + net.JoinHostPort(execution.PodIP, fmt.Sprint(guestapi.Port))}, nil
	}
	w, err := p.get(ctx, h)
	if err != nil {
		return boxprovider.Connection{}, err
	}
	if w.DeletionTimestamp != nil || w.Spec.DesiredState != "Running" {
		return boxprovider.Connection{}, boxprovider.ErrNotReady
	}
	if expected != "" && w.Status.PodUID != expected {
		return boxprovider.Connection{}, boxprovider.ErrStaleExecution
	}
	pod, err := p.readyPod(ctx, w)
	if err != nil {
		return boxprovider.Connection{}, err
	}
	service := &core.Service{}
	if err = p.Client.Get(ctx, client.ObjectKey{Namespace: w.Namespace, Name: w.Name}, service); err != nil {
		return boxprovider.Connection{}, err
	}
	owner := meta.GetControllerOf(service)
	if owner == nil || owner.UID != w.UID || owner.Kind != api.Kind || owner.APIVersion != api.GroupVersion.String() || service.Spec.Selector[api.OwnerLabel] != string(w.UID) || service.Spec.Selector[api.ServingLabel] != "true" {
		return boxprovider.Connection{}, errors.New("guest Service ownership or selector changed")
	}
	validPort := false
	for _, port := range service.Spec.Ports {
		if port.Port == guestapi.Port && port.TargetPort.IntValue() == guestapi.Port {
			validPort = true
		}
	}
	if !validPort {
		return boxprovider.Connection{}, errors.New("guest Service port changed")
	}
	// Resolve only the same live execution observed before the Service lookup.
	current, err := p.get(ctx, h)
	if err != nil {
		return boxprovider.Connection{}, err
	}
	if current.DeletionTimestamp != nil || current.Spec.DesiredState != "Running" || current.Status.Phase != "Running" || current.Status.PodUID != string(pod.UID) || current.Status.PodName != pod.Name {
		return boxprovider.Connection{}, errors.New("guest execution changed during connection resolution")
	}
	currentPod, err := p.readyPod(ctx, current)
	if err != nil {
		return boxprovider.Connection{}, err
	}
	identity := func(resource *api.ResumablePod, pod *core.Pod) string {
		if api.ConfirmedExecution(resource, pod) {
			return resource.Status.Execution.ContainerID
		}
		return containerIdentity(pod, resource.Spec.Container.Name)
	}
	if identity(current, currentPod) != identity(w, pod) {
		return boxprovider.Connection{}, boxprovider.ErrStaleExecution
	}
	ip := currentPod.Status.PodIP
	if api.ConfirmedExecution(current, currentPod) {
		ip = current.Status.Execution.PodIP
	}
	if net.ParseIP(ip) == nil {
		return boxprovider.Connection{}, boxprovider.ErrNotReady
	}
	// The API already resolved and fenced this execution. Avoid waiting for
	// EndpointSlice and Service routing to catch up after a restore.
	return boxprovider.Connection{URL: "http://" + net.JoinHostPort(ip, fmt.Sprint(guestapi.Port))}, nil
}

// A local completion is usable before Kubernetes publication. The persisted
// claim fences identity; CRI must still confirm that the same container is live.
func (p *Provider) localExecution(ctx context.Context, h boxprovider.Handle, expected string) (*api.Execution, error) {
	if p.Local == nil {
		return nil, nil
	}
	r, err := p.Local.LocalRecord(ctx, client.ObjectKey{Namespace: h.Namespace, Name: h.Name})
	if err != nil || r == nil {
		return nil, err
	}
	w := r.Resource
	if !validObservationIdentity(w, h) {
		return nil, boxprovider.ErrStaleExecution
	}
	if expected != "" && w.Status.PodUID != expected {
		return nil, boxprovider.ErrStaleExecution
	}
	if !r.Complete || r.Pod == nil || w.DeletionTimestamp != nil || w.Spec.DesiredState != "Running" || w.Status.Phase != "Running" ||
		w.Status.ObservedGeneration != w.Generation || w.Status.ObservedTransitionID != w.Spec.TransitionID || w.Status.ResumeReadyTransitionID != w.Spec.TransitionID ||
		w.Status.Execution == nil || p.ConfirmExecutionLocal == nil {
		return nil, boxprovider.ErrNotReady
	}
	execution, err := p.ConfirmExecutionLocal(ctx, w, r.Pod)
	if err != nil {
		return nil, err
	}
	if execution == nil || *execution != *w.Status.Execution || net.ParseIP(execution.PodIP) == nil {
		return nil, boxprovider.ErrStaleExecution
	}
	// Recheck local intent after CRI access, including a concurrent suspend or
	// external deletion observed by the background synchronizer.
	current, err := p.Local.StoredResource(ctx, client.ObjectKeyFromObject(w))
	if err != nil {
		return nil, err
	}
	if current == nil || current.UID != w.UID || current.DeletionTimestamp != nil || current.Spec.DesiredState != "Running" || current.Spec.TransitionID != w.Spec.TransitionID || current.Status.Phase != "Running" || !apiequality.Semantic.DeepEqual(current.Status.Execution, execution) {
		return nil, boxprovider.ErrStaleExecution
	}
	return execution, nil
}

var _ boxprovider.Provider = (*Provider)(nil)
