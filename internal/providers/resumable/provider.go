// Package resumable adapts the node-local ResumablePod operator to Cellbox.
package resumable

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path"
	"regexp"
	"strings"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/guestapi"
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

// TokenExec obtains the runtime-generated token from the current guest Pod.
// Implementations must avoid logging the token or the exec response body.
type TokenExec interface {
	Token(context.Context, string, string, string) (string, error)
}

type Provider struct {
	Client client.Client
	Exec   TokenExec
	// ServiceDomain may be overridden for nonstandard cluster DNS suffixes.
	ServiceDomain string
}

func New(c client.Client, exec TokenExec) *Provider { return &Provider{Client: c, Exec: exec} }
func (*Provider) Name() string                      { return name }

func crName(boxID string) (string, error) {
	if len(boxID) == 0 || len(boxID) > 55 || !validBoxID.MatchString(boxID) {
		return "", fmt.Errorf("box ID must be a DNS label of at most 55 characters")
	}
	return "cellbox-" + boxID, nil
}

func (p *Provider) Create(ctx context.Context, spec boxprovider.Spec) (boxprovider.Handle, error) {
	if p == nil || p.Client == nil {
		return boxprovider.Handle{}, errors.New("Kubernetes client is required")
	}
	crname, err := crName(spec.BoxID)
	if err != nil {
		return boxprovider.Handle{}, err
	}
	if spec.Namespace == "" || spec.NodeName == "" {
		return boxprovider.Handle{}, errors.New("namespace and node name are required")
	}
	if !validImage.MatchString(spec.Image) {
		return boxprovider.Handle{}, errors.New("immutable image repository@sha256:<digest> is required")
	}
	if host := spec.DebugReadOnlyHostPath; host != "" &&
		(!path.IsAbs(host) || path.Clean(host) != host || host == "/" || len(host) > 4096 || strings.ContainsAny(host, "\x00\r\n")) {
		return boxprovider.Handle{}, errors.New("debug read-only host path must be a clean absolute directory path other than /")
	}
	if host := spec.DebugReadWriteHostPath; host != "" &&
		(spec.DebugReadOnlyHostPath != "" || !path.IsAbs(host) || path.Clean(host) != host || host == "/" || len(host) > 4096 || strings.ContainsAny(host, "\x00\r\n")) {
		return boxprovider.Handle{}, errors.New("debug read-write host path must be a clean absolute directory path other than /")
	}
	if math.IsNaN(spec.CPU) || math.IsInf(spec.CPU, 0) || spec.CPU <= 0 || spec.CPU > 1024 || spec.MemoryMiB <= 0 || spec.MemoryMiB > 1<<40 {
		return boxprovider.Handle{}, errors.New("positive bounded CPU and memory are required")
	}
	config, err := json.Marshal(spec.Config)
	if err != nil {
		return boxprovider.Handle{}, fmt.Errorf("encode guest configuration: %w", err)
	}
	argv := []string{"serve", "--config-base64", base64.StdEncoding.EncodeToString(config)}
	if spec.Staged {
		argv = append(argv, "--staged")
	}
	cpu := resource.NewMilliQuantity(int64(math.Ceil(spec.CPU*1000)), resource.DecimalSI)
	memory := resource.NewQuantity(spec.MemoryMiB*1024*1024, resource.BinarySI)
	resources := core.ResourceList{core.ResourceCPU: *cpu, core.ResourceMemory: *memory}
	zero := int64(0)
	no := false
	container := core.Container{
		Name: "cellbox", Image: spec.Image, ImagePullPolicy: core.PullIfNotPresent,
		Command: []string{guestapi.Binary}, Args: argv,
		Ports:     []core.ContainerPort{{Name: "guest", ContainerPort: guestapi.Port, Protocol: core.ProtocolTCP}},
		Resources: core.ResourceRequirements{Requests: resources, Limits: resources},
		SecurityContext: &core.SecurityContext{RunAsUser: &zero, RunAsNonRoot: &no, Privileged: &no, AllowPrivilegeEscalation: &no,
			Capabilities: &core.Capabilities{Drop: []core.Capability{"ALL"}, Add: []core.Capability{"CHOWN", "SETUID", "SETGID", "FOWNER", "DAC_OVERRIDE"}}},
		ReadinessProbe: &core.Probe{ProbeHandler: core.ProbeHandler{TCPSocket: &core.TCPSocketAction{Port: intstr.FromInt32(guestapi.Port)}}, PeriodSeconds: 2},
	}
	wanted := &api.ResumablePod{
		TypeMeta:   meta.TypeMeta{APIVersion: api.GroupVersion.String(), Kind: "ResumablePod"},
		ObjectMeta: meta.ObjectMeta{Name: crname, Namespace: spec.Namespace, Labels: map[string]string{managedLabel: "true", boxLabel: spec.BoxID}},
		Spec: api.Spec{NodeName: spec.NodeName, DesiredState: "Running", Container: container,
			DebugReadOnlyHostPath:  spec.DebugReadOnlyHostPath,
			DebugReadWriteHostPath: spec.DebugReadWriteHostPath,
			ServicePorts:           []core.ServicePort{{Name: "guest", Port: guestapi.Port, TargetPort: intstr.FromInt32(guestapi.Port), Protocol: core.ProtocolTCP}}},
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
	return boxprovider.Handle{Provider: name, ID: string(wanted.UID), Name: wanted.Name, Namespace: wanted.Namespace, NodeName: spec.NodeName}, nil
}

func matchExisting(actual, wanted *api.ResumablePod) error {
	if actual.UID == "" || actual.DeletionTimestamp != nil || actual.Labels[managedLabel] != "true" || actual.Labels[boxLabel] != wanted.Labels[boxLabel] || actual.Spec.NodeName != wanted.Spec.NodeName || actual.Spec.DebugReadOnlyHostPath != wanted.Spec.DebugReadOnlyHostPath || actual.Spec.DebugReadWriteHostPath != wanted.Spec.DebugReadWriteHostPath || !apiequality.Semantic.DeepEqual(actual.Spec.Container, wanted.Spec.Container) || !apiequality.Semantic.DeepEqual(actual.Spec.ServicePorts, wanted.Spec.ServicePorts) {
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
		return boxprovider.Observation{State: "deleted"}, nil
	}
	if err != nil {
		return boxprovider.Observation{}, err
	}
	obs := boxprovider.Observation{ExecutionID: w.Status.PodUID, Message: w.Status.Message}
	if w.DeletionTimestamp != nil {
		obs.State = "deleting"
		return obs, nil
	}
	switch w.Status.Phase {
	case "", "Creating":
		obs.State = "provisioning"
	case "Restoring":
		obs.State = "resuming"
	case "Running":
		_, err = p.readyPod(ctx, w)
		if err == nil {
			obs.State = "ready"
		} else if errors.Is(err, errNotReady) {
			obs.State = "provisioning"
			obs.Message = err.Error()
		} else if errors.Is(err, errExecutionLost) {
			obs.State = "failed"
			obs.Message = err.Error()
		} else {
			return boxprovider.Observation{}, err
		}
	case "Checkpointing", "Suspending":
		obs.State = "provisioning"
	case "Suspended":
		obs.State = "suspended"
		obs.ExecutionID = ""
	case "Failing", "Failed":
		obs.State = "failed"
	case "Deleting":
		obs.State = "deleting"
	default:
		obs.State = "failed"
		obs.Message = "unknown operator phase: " + w.Status.Phase
	}
	return obs, nil
}

var errNotReady = errors.New("current Pod is not ready")
var errExecutionLost = errors.New("current Pod execution lost")

func (p *Provider) readyPod(ctx context.Context, w *api.ResumablePod) (*core.Pod, error) {
	if w.Status.Phase != "Running" || w.Status.PodName == "" || w.Status.PodUID == "" {
		return nil, errNotReady
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
	if owner == nil || owner.UID != w.UID || owner.Kind != "ResumablePod" || owner.APIVersion != api.GroupVersion.String() || string(pod.UID) != w.Status.PodUID {
		return nil, fmt.Errorf("%w: Pod ownership or UID changed", errExecutionLost)
	}
	if pod.DeletionTimestamp != nil || pod.Status.Phase == core.PodFailed || pod.Status.Phase == core.PodSucceeded {
		return nil, fmt.Errorf("%w: Pod exited or is deleting", errExecutionLost)
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == core.PodReady && condition.Status == core.ConditionTrue && pod.Status.Phase == core.PodRunning {
			return pod, nil
		}
	}
	return nil, errNotReady
}

func (p *Provider) Action(ctx context.Context, h boxprovider.Handle, action string) error {
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
	w, err := p.get(ctx, h)
	if err != nil {
		return err
	}
	if w.DeletionTimestamp != nil {
		return errors.New("ResumablePod is deleting")
	}
	if w.Status.Phase == "Failed" || w.Status.Phase == "Failing" {
		return errors.New("ResumablePod failed; explicit operator retry is required")
	}
	if w.Spec.DesiredState == desired {
		return nil
	}
	old := w.DeepCopy()
	w.Spec.DesiredState = desired
	// An optimistic update prevents stale operations from changing a newer CR.
	if err = p.Client.Patch(ctx, w, client.MergeFromWithOptions(old, client.MergeFromWithOptimisticLock{})); err != nil {
		return err
	}
	return nil
}

func (p *Provider) Destroy(ctx context.Context, h boxprovider.Handle) error {
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
	if p.Exec == nil {
		return boxprovider.Connection{}, errors.New("Kubernetes Pod exec is not configured")
	}
	w, err := p.get(ctx, h)
	if err != nil {
		return boxprovider.Connection{}, err
	}
	if w.DeletionTimestamp != nil || w.Spec.DesiredState != "Running" {
		return boxprovider.Connection{}, errNotReady
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
	if owner == nil || owner.UID != w.UID || owner.Kind != "ResumablePod" || owner.APIVersion != api.GroupVersion.String() || service.Spec.Selector[api.OwnerLabel] != string(w.UID) || service.Spec.Selector["recovery.gvisor.dev/serving"] != "true" {
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
	token, err := p.Exec.Token(ctx, w.Namespace, pod.Name, w.Spec.Container.Name)
	if err != nil {
		return boxprovider.Connection{}, err
	}
	token = strings.TrimSpace(token)
	if token == "" || strings.ContainsAny(token, "\r\n\x00") {
		return boxprovider.Connection{}, errors.New("guest returned an invalid token")
	}
	// Recheck both identities after exec: a replace/delete during token retrieval
	// must not leak a stale token to a new execution.
	current, err := p.get(ctx, h)
	if err != nil {
		return boxprovider.Connection{}, err
	}
	if current.DeletionTimestamp != nil || current.Spec.DesiredState != "Running" || current.Status.Phase != "Running" || current.Status.PodUID != string(pod.UID) || current.Status.PodName != pod.Name {
		return boxprovider.Connection{}, errors.New("guest execution changed during token retrieval")
	}
	if _, err = p.readyPod(ctx, current); err != nil {
		return boxprovider.Connection{}, err
	}
	domain := p.ServiceDomain
	if domain == "" {
		domain = "svc"
	}
	if !regexp.MustCompile(`^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$`).MatchString(domain) {
		return boxprovider.Connection{}, errors.New("invalid Service DNS suffix")
	}
	return boxprovider.Connection{URL: fmt.Sprintf("http://%s.%s.%s:%d", w.Name, w.Namespace, domain, guestapi.Port), Token: token}, nil
}

var _ boxprovider.Provider = (*Provider)(nil)
