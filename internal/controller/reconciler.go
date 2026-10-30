// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"path"
	"reflect"
	"regexp"
	"strings"
	"time"

	api "github.com/jingyugao/cell-box/api/v1alpha1"
	"github.com/jingyugao/cell-box/internal/boxprovider"
	"github.com/jingyugao/cell-box/internal/homevolume"
	"github.com/jingyugao/cell-box/internal/runtime"
	"github.com/jingyugao/cell-box/internal/runtimecontrol"
	"github.com/jingyugao/cell-box/internal/telemetry"
	"go.opentelemetry.io/otel/trace"
	core "k8s.io/api/core/v1"
	errors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type Reconciler struct {
	client.Client
	Runtime      runtime.Backend
	NodeName     string
	NodeBase     string
	RuntimeClass string
	WarmPool     *WarmPool
	ports        portWaits
}

func (r *Reconciler) nodeBase() string {
	if r.NodeBase != "" {
		return r.NodeBase
	}
	return homevolume.DefaultBase
}

func (r *Reconciler) runtimeClass() string {
	if r.RuntimeClass != "" {
		return r.RuntimeClass
	}
	return api.RuntimeClass
}

type inventoryRuntime interface {
	SyncInventory(context.Context, *api.ResumablePod) error
}
type snapshotInvalidator interface {
	InvalidateSnapshot(context.Context, *api.ResumablePod) error
}
type executionRuntime interface {
	WaitWarmStarted(context.Context, *api.ResumablePod, *core.Pod) (*api.Execution, error)
}

type legacyGuestRuntime interface {
	NeedsGuestUnquiesce(context.Context, *api.ResumablePod) (bool, error)
	ResumeGuest(context.Context, *api.ResumablePod, *core.Pod) error
}

type failureDiagnostics interface {
	CaptureFailure(context.Context, *api.ResumablePod, *core.Pod) error
}

type workspaceArchiveRuntime interface {
	CaptureWorkspaceArchive(context.Context, *api.ResumablePod, api.ArchiveCaptureRequest) (api.ArchiveCaptureResult, error)
}

func (r *Reconciler) syncInventoryPhase(ctx context.Context, w *api.ResumablePod, phase string) (retErr error) {
	syncer, ok := r.Runtime.(inventoryRuntime)
	if !ok {
		return nil
	}
	resource := w.DeepCopy()
	resource.Status.Phase = phase
	return syncer.SyncInventory(ctx, resource)
}

const assignmentMessage = "Preparing a new Pod execution"

var again = ctrl.Result{RequeueAfter: time.Second}
var errServiceCollision = stderrors.New("Service name is already owned by another workload")
var pullableImage = regexp.MustCompile(`^[a-z0-9][a-z0-9._:/-]*@sha256:[a-f0-9]{64}$`)

func (r *Reconciler) phase(ctx context.Context, w *api.ResumablePod, phase, message string, podReady ...bool) (traceResult ctrl.Result, retErr error) {
	w.Status.Message = message
	if err := r.syncInventoryPhase(ctx, w, phase); err != nil {
		return again, err
	}
	markPhase(w, phase, message, podReady...)
	return again, r.Status().Update(ctx, w)
}

func markPhase(w *api.ResumablePod, phase, message string, podReady ...bool) {
	if w.Status.Phase != phase {
		w.Status.Since = meta.Now()
	}
	w.Status.Phase = phase
	w.Status.Message = message
	w.Status.ObservedGeneration = w.Generation
	w.Status.ObservedTransitionID = w.Spec.TransitionID
	ready := meta.ConditionFalse
	if phase == "Running" {
		if len(podReady) == 0 {
			ready = meta.ConditionUnknown
		} else if podReady[0] {
			ready = meta.ConditionTrue
		}
	}
	apimeta.SetStatusCondition(&w.Status.Conditions, meta.Condition{Type: "Ready", Status: ready, Reason: phase, Message: message, ObservedGeneration: w.Generation})
}
func fingerprint(w *api.ResumablePod) string {
	return api.SpecFingerprint(w)
}

// warmFingerprint ignores the selected HOME volume so one warm runtime profile
// can serve Boxes that attach different volumes.
func warmFingerprint(w *api.ResumablePod) string {
	return api.WarmSpecFingerprint(w)
}
func debugHostVolumes(w *api.ResumablePod) []core.Volume {
	if w.Spec.DebugReadWriteHostPath != "" {
		directory := core.HostPathDirectory
		return []core.Volume{{Name: "debug-host", VolumeSource: core.VolumeSource{HostPath: &core.HostPathVolumeSource{Path: w.Spec.DebugReadWriteHostPath, Type: &directory}}}}
	}
	if w.Spec.DebugReadOnlyHostPath == "" {
		return nil
	}
	directory := core.HostPathDirectory
	return []core.Volume{{Name: "debug-host", VolumeSource: core.VolumeSource{HostPath: &core.HostPathVolumeSource{Path: w.Spec.DebugReadOnlyHostPath, Type: &directory}}}}
}
func debugHostMounts(w *api.ResumablePod) []core.VolumeMount {
	if w.Spec.DebugReadWriteHostPath != "" {
		return []core.VolumeMount{{Name: "debug-host", MountPath: api.DebugHomeMountPath}}
	}
	if w.Spec.DebugReadOnlyHostPath == "" {
		return nil
	}
	return []core.VolumeMount{{Name: "debug-host", MountPath: api.DebugHostMountPath, ReadOnly: true}}
}

func workloadMounts(w *api.ResumablePod) []api.VolumeMount {
	if w.Spec.Mounts != nil {
		return w.Spec.Mounts
	}
	if !w.Spec.PersistentHome {
		return nil
	}
	uid := w.Spec.HomeVolumeUID
	if uid == "" {
		uid = string(w.UID)
	}
	return []api.VolumeMount{{UID: uid, MountPath: homevolume.MountPath}}
}

func mountVolumeName(mount api.VolumeMount, index int) string {
	if mount.MountPath == homevolume.MountPath {
		return "agent-home"
	}
	return fmt.Sprintf("cellbox-volume-%d", index)
}

func hostVolumes(w *api.ResumablePod, nodeBases ...string) []core.Volume {
	nodeBase := homevolume.DefaultBase
	if len(nodeBases) != 0 && nodeBases[0] != "" {
		nodeBase = nodeBases[0]
	}
	volumes := debugHostVolumes(w)
	for i, mount := range workloadMounts(w) {
		home, _ := homevolume.Path(nodeBase, mount.UID) // validate checks each volume UID first.
		directory := core.HostPathDirectory
		volumes = append(volumes, core.Volume{Name: mountVolumeName(mount, i), VolumeSource: core.VolumeSource{HostPath: &core.HostPathVolumeSource{Path: home, Type: &directory}}})
	}
	if w.Spec.SharedReadOnlyHostPath != "" {
		directory := core.HostPathDirectory
		volumes = append(volumes, core.Volume{Name: "shared", VolumeSource: core.VolumeSource{HostPath: &core.HostPathVolumeSource{Path: w.Spec.SharedReadOnlyHostPath, Type: &directory}}})
	}
	return volumes
}
func hostMounts(w *api.ResumablePod) []core.VolumeMount {
	mounts := debugHostMounts(w)
	for i, mount := range workloadMounts(w) {
		mounts = append(mounts, core.VolumeMount{Name: mountVolumeName(mount, i), MountPath: mount.MountPath, ReadOnly: mount.ReadOnly})
	}
	if w.Spec.SharedReadOnlyHostPath != "" {
		mounts = append(mounts, core.VolumeMount{Name: "shared", MountPath: api.SharedMountPath, ReadOnly: true})
	}
	return mounts
}
func admittedDebugMount(w *api.ResumablePod, p *core.Pod, nodeBases ...string) bool {
	if len(p.Spec.Containers) != 1 {
		return false
	}
	template := w
	if p.Labels[warmLabel] == "true" {
		// Pooled Pods must not expose a template Box's HOME. The authorized
		// child OCI create binds HOME after this Pod has been claimed.
		template = w.DeepCopy()
		template.Spec.PersistentHome = false
		template.Spec.HomeVolumeUID = ""
		template.Spec.Mounts = nil
	}
	wantVolumes, wantMounts := hostVolumes(template, nodeBases...), hostMounts(template)
	if len(p.Spec.Volumes) != len(wantVolumes) || len(p.Spec.Containers[0].VolumeMounts) != len(wantMounts) {
		return false
	}
	if len(wantVolumes) == 0 {
		return true
	}
	return reflect.DeepEqual(p.Spec.Volumes, wantVolumes) && reflect.DeepEqual(p.Spec.Containers[0].VolumeMounts, wantMounts)
}
func validate(w *api.ResumablePod, nodeBases ...string) error {
	if _, err := boxprovider.NormalizeWaitPorts(w.Spec.WaitPorts); err != nil {
		return err
	}
	nodeBase := homevolume.DefaultBase
	if len(nodeBases) != 0 && nodeBases[0] != "" {
		nodeBase = nodeBases[0]
	}
	c := w.Spec.Container
	if w.Spec.PersistentHome {
		owner := string(w.UID)
		if w.Spec.HomeVolumeUID != "" {
			owner = w.Spec.HomeVolumeUID
		}
		if _, err := homevolume.Path(nodeBase, owner); err != nil {
			return err
		}
	} else if w.Spec.HomeVolumeUID != "" {
		return fmt.Errorf("homeVolumeUID requires persistentHome")
	}
	if w.Spec.Mounts != nil {
		if len(w.Spec.Mounts) == 0 {
			return fmt.Errorf("mounts must not be empty when specified")
		}
		for i, mount := range w.Spec.Mounts {
			if _, err := homevolume.Path(nodeBase, mount.UID); err != nil {
				return fmt.Errorf("mount %d UID: %w", i, err)
			}
			if !path.IsAbs(mount.MountPath) || path.Clean(mount.MountPath) != mount.MountPath || mount.MountPath == "/" || len(mount.MountPath) > 4096 || strings.ContainsAny(mount.MountPath, "\x00\r\n") {
				return fmt.Errorf("mount %d path must be a clean absolute path other than /", i)
			}
		}
	}
	if w.Spec.DesiredState != "Running" && w.Spec.DesiredState != "Suspended" {
		return fmt.Errorf("desiredState must be Running or Suspended")
	}
	if len(c.VolumeMounts)+len(c.VolumeDevices)+len(c.EnvFrom) > 0 || c.Lifecycle != nil || c.RestartPolicy != nil || len(c.ResizePolicy) > 0 || c.Stdin || c.TTY {
		return fmt.Errorf("mounts, external env references, lifecycle hooks, container restart/resize, stdin/TTY unsupported")
	}
	for _, e := range c.Env {
		if e.ValueFrom != nil {
			return fmt.Errorf("env valueFrom unsupported")
		}
	}
	if c.SecurityContext != nil && (c.SecurityContext.Privileged != nil && *c.SecurityContext.Privileged) {
		return fmt.Errorf("privileged containers unsupported")
	}
	for _, p := range c.Ports {
		if p.HostPort != 0 || p.HostIP != "" {
			return fmt.Errorf("host ports unsupported")
		}
	}
	if c.Name == "" || c.Image == "" {
		return fmt.Errorf("container name/image required")
	}
	if host := w.Spec.SharedReadOnlyHostPath; host != "" &&
		(!path.IsAbs(host) || path.Clean(host) != host || host == "/" || len(host) > 4096 || strings.ContainsAny(host, "\x00\r\n")) {
		return fmt.Errorf("sharedReadOnlyHostPath must be a clean absolute directory path other than /")
	}
	if host := w.Spec.DebugReadOnlyHostPath; host != "" &&
		(!path.IsAbs(host) || path.Clean(host) != host || host == "/" || len(host) > 4096 || strings.ContainsAny(host, "\x00\r\n")) {
		return fmt.Errorf("debugReadOnlyHostPath must be a clean absolute directory path other than /")
	}
	if host := w.Spec.DebugReadWriteHostPath; host != "" &&
		(w.Spec.DebugReadOnlyHostPath != "" || !path.IsAbs(host) || path.Clean(host) != host || host == "/" || len(host) > 4096 || strings.ContainsAny(host, "\x00\r\n")) {
		return fmt.Errorf("debugReadWriteHostPath must be a clean absolute directory path other than /")
	}
	if c.ImagePullPolicy != core.PullNever && (c.ImagePullPolicy != core.PullIfNotPresent || !pullableImage.MatchString(c.Image)) {
		return fmt.Errorf("imagePullPolicy must be Never or IfNotPresent with an immutable repository@sha256:<digest> image")
	}
	if w.Status.SpecHash != "" && w.Status.SpecHash != fingerprint(w) {
		return fmt.Errorf("node, container, debug host path and service ports are immutable; recreate the Cellbox to change them")
	}
	return nil
}
func owned(w *api.ResumablePod, o client.Object) bool {
	ref := meta.GetControllerOf(o)
	return ref != nil && ref.UID == w.UID && ref.Kind == api.Kind && ref.APIVersion == api.GroupVersion.String()
}
func (r *Reconciler) pod(ctx context.Context, w *api.ResumablePod) (traceResult *core.Pod, retErr error) {
	if local, ok := r.Client.(*runtimecontrol.Client); ok && w.DeletionTimestamp == nil && (w.Status.Phase == "Suspended" || w.Status.Phase == "Restoring") {
		record, err := local.LocalRecord(ctx, client.ObjectKeyFromObject(w))
		if err != nil {
			return nil, err
		}
		if record != nil {
			if record.Pod == nil {
				return nil, nil
			}
			return record.Pod.DeepCopy(), nil
		}
	}
	if w.Status.PodName == "" {
		return nil, nil
	}
	p := &core.Pod{}
	err := r.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: w.Status.PodName}, p)
	if errors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !owned(w, p) || (w.Status.PodUID != "" && string(p.UID) != w.Status.PodUID) {
		if local, ok := r.Client.(*runtimecontrol.Client); ok && (w.DeletionTimestamp != nil || w.Status.Phase == "Failing") && string(p.UID) != w.Status.PodUID {
			if record, err := local.LocalRecord(ctx, client.ObjectKeyFromObject(w)); err != nil {
				return nil, err
			} else if record != nil && record.Pod != nil && string(record.Pod.UID) == w.Status.PodUID {
				// Cleanup targets the recorded UID, never a same-name replacement.
				return nil, nil
			}
		}
		return nil, fmt.Errorf("Pod identity collision: refusing to operate on %s", p.Name)
	}
	return p, nil
}
func ready(p *core.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == core.PodReady && c.Status == core.ConditionTrue {
			return true
		}
	}
	return false
}
func podFailure(p *core.Pod) string {
	for _, s := range p.Status.ContainerStatuses {
		if s.RestartCount > 0 {
			return "unexpected container restart"
		}
		if s.State.Waiting != nil {
			return s.State.Waiting.Reason + ": " + s.State.Waiting.Message
		}
		if s.State.Terminated != nil {
			return s.State.Terminated.Reason
		}
	}
	return string(p.Status.Phase)
}

func (r *Reconciler) startupFailure(ctx context.Context, p *core.Pod) string {
	message := podFailure(p)
	events := &core.EventList{}
	if err := r.List(ctx, events, client.InNamespace(p.Namespace), client.MatchingFields{"involvedObject.uid": string(p.UID)}); err != nil {
		return message
	}
	var latest time.Time
	for _, event := range events.Items {
		if event.Type == core.EventTypeWarning && event.InvolvedObject.UID == p.UID && !event.LastTimestamp.Time.Before(latest) {
			latest = event.LastTimestamp.Time
			message = event.Reason + ": " + event.Message
		}
	}
	if len(message) > 3000 {
		message = message[:3000]
	}
	return message
}
func (r *Reconciler) begin(ctx context.Context, w *api.ResumablePod) (traceResult ctrl.Result, retErr error) {
	started := time.Now()
	defer func() {
		ctrl.LoggerFrom(ctx).Info("execution preparation finished", "podUID", w.Status.PodUID, "durationMs", time.Since(started).Milliseconds())
	}()
	var warm *core.Pod
	version := w.ResourceVersion
	w.Status.Cycle++
	w.Status.PodName = fmt.Sprintf("cb-%s-%d", string(w.UID)[:8], w.Status.Cycle)
	w.Status.PodUID = ""
	w.Status.Execution = nil
	w.Status.ResumePath = ""
	if w.Status.Snapshot != "" && r.WarmPool != nil {
		var p *core.Pod
		err := traced(ctx, "controller.resume.acquire", func(ctx context.Context) (e error) {
			p, e = r.WarmPool.acquire(ctx, w)
			return e
		})
		if err != nil {
			return again, err
		}
		if p != nil {
			warm = p
			w.Status.PodName = p.Name
			w.Status.PodUID = string(p.UID)
			w.Status.ResumePath = api.ResumePathWarmPool
		}
		ctrl.LoggerFrom(ctx).Info("warm Pod acquisition finished", "podUID", w.Status.PodUID, "durationMs", time.Since(started).Milliseconds())
	}
	if warm == nil {
		if local, ok := r.Client.(*runtimecontrol.Client); ok {
			if handled, err := local.FallbackResume(ctx, w); handled || err != nil {
				return again, err
			}
		}
	}
	phase := "Creating"
	if w.Status.Snapshot != "" {
		phase = "Restoring"
		if warm == nil {
			w.Status.ResumePath = api.ResumePathStandard
		}
	}
	var result ctrl.Result
	var err error
	// A local claim has already committed this assignment.
	if warm == nil || w.ResourceVersion == version {
		err = traced(ctx, "controller.resume.persist_assignment", func(ctx context.Context) (e error) {
			result, e = r.phase(ctx, w, phase, assignmentMessage)
			return e
		})
	}
	ctrl.LoggerFrom(ctx).Info("execution assignment persisted", "podUID", w.Status.PodUID, "durationMs", time.Since(started).Milliseconds())
	if err == nil && warm != nil {
		if backend, ok := r.Runtime.(warmRuntime); ok {
			err = traced(ctx, "controller.resume.activate_warm", func(ctx context.Context) error {
				_, e := backend.ActivateWarm(ctx, w, warm)
				return e
			})
		}
		if err == nil {
			return r.waitWarmStarted(ctx, w, warm)
		}
	}
	return result, err
}

func (r *Reconciler) waitWarmStarted(ctx context.Context, w *api.ResumablePod, p *core.Pod) (ctrl.Result, error) {
	if _, ok := r.Runtime.(listeningRuntime); ok {
		return r.waitListening(ctx, w, p)
	}
	backend, ok := r.Runtime.(executionRuntime)
	if !ok {
		return again, nil
	}
	execution, err := backend.WaitWarmStarted(ctx, w, p)
	if err != nil {
		return again, err
	}
	if execution == nil {
		return ctrl.Result{RequeueAfter: 50 * time.Millisecond}, nil
	}
	if execution.PodUID != string(p.UID) || execution.PodUID != w.Status.PodUID {
		return again, fmt.Errorf("runtime reported another execution")
	}
	w.Status.Execution = execution
	return r.started(ctx, w, p)
}

func (r *Reconciler) confirmExecution(ctx context.Context, w *api.ResumablePod, p *core.Pod) error {
	backend, ok := r.Runtime.(interface {
		ConfirmExecution(context.Context, *api.ResumablePod, *core.Pod) (*api.Execution, error)
	})
	if !ok || w.Status.Execution != nil {
		return nil
	}
	execution, err := backend.ConfirmExecution(ctx, w, p)
	if err != nil {
		return err
	}
	if execution == nil || execution.PodUID != w.Status.PodUID {
		return fmt.Errorf("runtime reported another execution")
	}
	w.Status.Execution = execution
	return nil
}

func (r *Reconciler) started(ctx context.Context, w *api.ResumablePod, p *core.Pod) (traceResult ctrl.Result, retErr error) {
	if err := traced(ctx, "controller.resume.confirm_execution", func(ctx context.Context) error { return r.confirmExecution(ctx, w, p) }); err != nil {
		return again, err
	}
	if backend, ok := r.Runtime.(snapshotInvalidator); ok && w.Status.Snapshot != "" {
		if err := traced(ctx, "controller.resume.invalidate_snapshot", func(ctx context.Context) error { return backend.InvalidateSnapshot(ctx, w) }); err != nil {
			return again, err
		}
	}
	// Replay eligibility is removed durably before either the Service or the
	// API may connect. Publish Running last so its notification is actionable.
	w.Status.Snapshot = ""
	if legacy, ok := r.Runtime.(legacyGuestRuntime); ok && w.Spec.TransitionID != "" {
		needed, err := legacy.NeedsGuestUnquiesce(ctx, w)
		if err != nil {
			return again, err
		}
		if needed {
			if err := legacy.ResumeGuest(ctx, w, p); err != nil {
				return again, err
			}
		}
	}
	if _, ok := r.Runtime.(snapshotInvalidator); ok {
		if err := traced(ctx, "controller.resume.serving", func(ctx context.Context) error { return r.serving(ctx, p, true) }); err != nil {
			return again, err
		}
	}
	var result ctrl.Result
	err := traced(ctx, "controller.resume.commit_running", func(ctx context.Context) (e error) {
		result, e = r.phase(ctx, w, "Running", "Container started; previous snapshot cannot be replayed", ready(p))
		return e
	})
	ctrl.LoggerFrom(ctx).Info("execution started", "podUID", w.Status.PodUID, "podReady", ready(p))
	return result, err
}

func (r *Reconciler) service(ctx context.Context, w *api.ResumablePod) (retErr error) {
	if len(w.Spec.ServicePorts) == 0 {
		return nil
	}
	s := &core.Service{}
	err := r.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: w.Name}, s)
	selector := map[string]string{api.OwnerLabel: string(w.UID), api.ServingLabel: "true"}
	if errors.IsNotFound(err) {
		s = &core.Service{ObjectMeta: meta.ObjectMeta{Name: w.Name, Namespace: w.Namespace}, Spec: core.ServiceSpec{Selector: selector, Ports: w.Spec.ServicePorts}}
		if err = controllerutil.SetControllerReference(w, s, r.Scheme()); err != nil {
			return err
		}
		return r.Create(ctx, s)
	}
	if err != nil {
		return err
	}
	if !owned(w, s) {
		return errServiceCollision
	}
	if reflect.DeepEqual(s.Spec.Selector, selector) && reflect.DeepEqual(s.Spec.Ports, w.Spec.ServicePorts) {
		return nil
	}
	old := s.DeepCopy()
	s.Spec.Selector = selector
	s.Spec.Ports = w.Spec.ServicePorts
	return r.Patch(ctx, s, client.MergeFrom(old))
}
func (r *Reconciler) serving(ctx context.Context, p *core.Pod, on bool) (retErr error) {
	value := "false"
	if on {
		value = "true"
	}
	if p.Labels[api.ServingLabel] == value {
		return nil
	}
	old := p.DeepCopy()
	if p.Labels == nil {
		p.Labels = map[string]string{}
	}
	p.Labels[api.ServingLabel] = value
	return r.Patch(ctx, p, client.MergeFrom(old))
}
func (r *Reconciler) removePod(ctx context.Context, w *api.ResumablePod, p *core.Pod) (bool, error) {
	if p != nil {
		if err := r.serving(ctx, p, false); err != nil {
			return false, err
		}
		if p.DeletionTimestamp == nil {
			uid := p.UID
			if err := r.Delete(ctx, p, &client.DeleteOptions{Preconditions: &meta.Preconditions{UID: &uid}}); err != nil && !errors.IsNotFound(err) {
				return false, err
			}
		}
	}
	target := p
	if target == nil && w.Status.PodUID != "" {
		target = &core.Pod{ObjectMeta: meta.ObjectMeta{Name: w.Status.PodName, Namespace: w.Namespace, UID: types.UID(w.Status.PodUID)}}
	}
	if target != nil {
		if err := r.Runtime.Cleanup(ctx, target); err != nil {
			return false, err
		}
	}
	if p != nil {
		return false, nil
	}
	return true, nil
}
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	result, err := r.reconcile(ctx, req)
	if err != nil {
		return ctrl.Result{}, err
	}
	return result, nil
}
func (r *Reconciler) reconcile(ctx context.Context, req ctrl.Request) (traceResult ctrl.Result, retErr error) {
	readStart := time.Now()
	w := &api.ResumablePod{}
	if err := r.Get(ctx, req.NamespacedName, w); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if w.Spec.NodeName != r.NodeName {
		return ctrl.Result{}, nil
	}
	if w.DeletionTimestamp != nil || (w.Status.Phase != "Restoring" && w.Status.Phase != "Creating") {
		r.stopPortWait(req.NamespacedName)
	}
	localPending := false
	if local, ok := r.Client.(*runtimecontrol.Client); ok {
		record, err := local.LocalRecord(ctx, req.NamespacedName)
		if err != nil {
			return again, err
		}
		localPending = record != nil
		if record != nil && record.Complete && w.Status.Phase != "Failed" && w.DeletionTimestamp == nil {
			// Completion is already actionable locally. Publication runs in the
			// background and must not re-enter the lifecycle while it is pending.
			return ctrl.Result{}, nil
		}
	}
	ctx = telemetry.ExtractOperation(ctx, w)
	if trace.SpanContextFromContext(ctx).IsValid() {
		var span trace.Span
		ctx, span = telemetry.StartAt(ctx, "cellbox.controller.reconcile", readStart, telemetry.String("phase", w.Status.Phase), telemetry.String("pod.uid", w.Status.PodUID))
		defer func() { telemetry.End(span, retErr) }()
	}
	if result, handled, err := r.captureWorkspaceArchive(ctx, w); handled || err != nil {
		return result, err
	}
	if w.Status.Snapshot != "" {
		phase := w.Status.Phase
		// The following begin transition will update the index directly to
		// Restoring; do not briefly republish a stale Suspended state.
		if !(phase == "Suspended" && w.Spec.DesiredState == "Running") &&
			(phase == "Checkpointing" || phase == "Suspending" || phase == "Suspended" ||
				phase == "Restoring" || phase == "Failing" || phase == "Failed" || phase == "Deleting") {
			if err := r.syncInventoryPhase(ctx, w, phase); err != nil {
				return again, err
			}
		}
	}
	if !controllerutil.ContainsFinalizer(w, api.Finalizer) && w.DeletionTimestamp == nil {
		controllerutil.AddFinalizer(w, api.Finalizer)
		return again, r.Update(ctx, w)
	}
	p, err := r.pod(ctx, w)
	if err != nil {
		return again, err
	}
	if w.DeletionTimestamp != nil {
		if w.Status.Phase != "Deleting" {
			return r.phase(ctx, w, "Deleting", "Reclaiming Pod runtime and checkpoint storage")
		}
		if r.WarmPool != nil {
			done, err := r.WarmPool.cleanupUnused(ctx, w)
			if err != nil || !done {
				return again, err
			}
		}
		done, err := r.removePod(ctx, w, p)
		if err != nil || !done {
			return again, err
		}
		if err = r.Runtime.Forget(ctx, w); err != nil {
			return again, err
		}
		// Observe owned Service removal before completing the finalizer.
		s := &core.Service{}
		err = r.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: w.Name}, s)
		if err == nil && owned(w, s) {
			if s.DeletionTimestamp == nil {
				uid := s.UID
				if err = r.Delete(ctx, s, &client.DeleteOptions{Preconditions: &meta.Preconditions{UID: &uid}}); err != nil && !errors.IsNotFound(err) {
					return again, err
				}
			}
			return again, nil
		} else if err != nil && !errors.IsNotFound(err) {
			return again, err
		}
		controllerutil.RemoveFinalizer(w, api.Finalizer)
		return ctrl.Result{}, r.Update(ctx, w)
	}
	if w.Status.Phase == "Failed" && p != nil {
		if err := r.serving(ctx, p, false); err != nil {
			return again, err
		}
	}
	if w.Status.Phase == "Failing" {
		if diagnostics, ok := r.Runtime.(failureDiagnostics); ok {
			if err := diagnostics.CaptureFailure(ctx, w, p); err != nil {
				return again, err
			}
		}
		done, err := r.removePod(ctx, w, p)
		if err != nil || !done {
			return again, err
		}
		return r.phase(ctx, w, "Failed", w.Status.Message)
	}
	if validationErr := validate(w, r.nodeBase()); validationErr != nil {
		if w.Status.Phase == "Failed" {
			return again, nil
		}
		w.Status.RetryNonce = w.Spec.RetryNonce
		return r.phase(ctx, w, "Failing", validationErr.Error())
	}
	if w.Status.Phase == "Failed" {
		if w.Spec.RetryNonce == w.Status.RetryNonce {
			return again, nil
		}
		if w.Status.Snapshot == "" {
			w.Status.RetryNonce = w.Spec.RetryNonce
			return r.phase(ctx, w, "Failed", "No recoverable snapshot; explicit recreation of the CR is required for cold start")
		}
		w.Status.RetryNonce = w.Spec.RetryNonce
		if w.Spec.DesiredState == "Suspended" {
			return r.phase(ctx, w, "Suspended", "Retry acknowledged; snapshot retained")
		}
		return r.begin(ctx, w)
	}
	fail := func(err error) (ctrl.Result, error) {
		if diagnostics, ok := r.Runtime.(failureDiagnostics); ok {
			evidence := w.DeepCopy()
			evidence.Status.Message = err.Error()
			if captureErr := diagnostics.CaptureFailure(ctx, evidence, p); captureErr != nil {
				return again, captureErr
			}
		}
		w.Status.RetryNonce = w.Spec.RetryNonce
		return r.phase(ctx, w, "Failing", err.Error())
	}
	if !localPending {
		if err = r.service(ctx, w); err != nil {
			if stderrors.Is(err, errServiceCollision) || errors.IsInvalid(err) {
				return fail(err)
			}
			return again, err
		}
	}
	switch w.Status.Phase {
	case "":
		w.Status.SpecHash = fingerprint(w)
		w.Status.RetryNonce = w.Spec.RetryNonce
		if w.Spec.DesiredState == "Suspended" {
			return r.phase(ctx, w, "Suspended", "Not started")
		}
		return r.begin(ctx, w)
	case "Creating", "Restoring":
		if p == nil {
			if w.Status.PodUID != "" {
				return fail(fmt.Errorf("Pod disappeared during startup; refusing replacement"))
			}
			p = executionPod(w, w.Status.PodName, r.runtimeClass(), r.nodeBase())
			if err = controllerutil.SetControllerReference(w, p, r.Scheme()); err != nil {
				return again, err
			}
			if err = r.Create(ctx, p); err != nil {
				if errors.IsInvalid(err) || errors.IsForbidden(err) {
					return fail(fmt.Errorf("Pod creation rejected: %w", err))
				}
				return again, err
			}
			return again, nil
		}
		if w.Status.PodUID == "" {
			w.Status.PodUID = string(p.UID)
			return again, r.Status().Update(ctx, w)
		}
		if p.DeletionTimestamp != nil {
			return fail(fmt.Errorf("Pod deleted during startup"))
		}
		if !admittedDebugMount(w, p, r.nodeBase()) || len(p.Spec.InitContainers) > 0 || len(p.Spec.EphemeralContainers) > 0 || (p.Spec.NodeName != "" && p.Spec.NodeName != r.NodeName) || p.Spec.RuntimeClassName == nil || *p.Spec.RuntimeClassName != r.runtimeClass() || p.Spec.HostNetwork || p.Spec.HostPID || p.Spec.HostIPC || p.Spec.RestartPolicy != core.RestartPolicyNever || p.Spec.AutomountServiceAccountToken == nil || *p.Spec.AutomountServiceAccountToken {
			return fail(fmt.Errorf("admitted Pod violates container, debug mount or node constraints"))
		}
		if len(p.Spec.SchedulingGates) > 0 {
			if len(p.Spec.SchedulingGates) != 1 || p.Spec.SchedulingGates[0].Name != api.Gate {
				return fail(fmt.Errorf("unexpected scheduling gate"))
			}
			if err = r.Runtime.Prepare(ctx, w, p); err != nil {
				if stderrors.Is(err, runtime.ErrRetryableStorage) {
					return again, err
				}
				return fail(err)
			}
			old := p.DeepCopy()
			if p.Annotations == nil {
				p.Annotations = map[string]string{}
			}
			p.Annotations[api.TicketAnnotation] = string(p.UID)
			p.Spec.SchedulingGates = nil
			return again, r.Patch(ctx, p, client.MergeFrom(old))
		}
		if backend, ok := r.Runtime.(warmRuntime); ok && p.Labels[warmLabel] == "true" {
			if _, err = backend.ActivateWarm(ctx, w, p); err != nil {
				if stderrors.Is(err, runtime.ErrWarmExpired) {
					if localPending {
						return fail(err)
					}
					if _, err = r.removePod(ctx, w, p); err != nil {
						return again, err
					}
					return r.begin(ctx, w)
				}
				if stderrors.Is(err, runtime.ErrRetryableStorage) {
					return again, err
				}
				return fail(err)
			}
		}

		if p.Status.Phase == core.PodFailed || p.Status.Phase == core.PodSucceeded {
			return fail(fmt.Errorf("Pod exited during startup: %s", podFailure(p)))
		}
		for _, c := range p.Status.ContainerStatuses {
			if c.RestartCount > 0 {
				return fail(fmt.Errorf("unexpected container restart"))
			}
		}
		if w.Status.Phase == "Restoring" {
			if _, ok := r.Runtime.(listeningRuntime); ok {
				return r.waitListening(ctx, w, p)
			}
		}
		if api.ExecutionStarted(p, w.Spec.Container.Name) {
			return r.started(ctx, w, p)
		}
		timeout := w.Spec.StartupTimeoutSeconds
		if timeout == 0 {
			timeout = 90
		}
		if time.Since(w.Status.Since.Time) > time.Duration(timeout)*time.Second {
			return fail(fmt.Errorf("startup timeout: %s", r.startupFailure(ctx, p)))
		}
		if p.Labels[warmLabel] == "true" {
			return r.waitWarmStarted(ctx, w, p)
		}
	case "Running":
		if p == nil || p.DeletionTimestamp != nil || p.Status.Phase == core.PodFailed || p.Status.Phase == core.PodSucceeded {
			return fail(fmt.Errorf("active execution lost; no automatic rollback or cold start"))
		}
		// Older cold-start records lack the address. Populate it once in the
		// controller so restored service requests never discover it on demand.
		if w.Status.Execution == nil && w.Spec.DesiredState == "Running" {
			if err := r.confirmExecution(ctx, w, p); err != nil {
				return again, err
			}
			if w.Status.Execution != nil {
				return again, r.Status().Update(ctx, w)
			}
		}
		if _, asyncCleanup := r.Runtime.(snapshotInvalidator); !asyncCleanup {
			if err = r.Runtime.Forget(ctx, w); err != nil {
				return again, err
			}
		}
		if w.Spec.DesiredState == "Suspended" {
			if err = r.serving(ctx, p, false); err != nil {
				return again, err
			}
			w.Status.Snapshot = fmt.Sprintf("checkpoint-%d", w.Status.Cycle)
			return r.phase(ctx, w, "Checkpointing", "Service endpoint withdrawn; saving sandbox")
		}
		if err = r.serving(ctx, p, api.AvailableExecution(w, p)); err != nil {
			return again, err
		}
		condition := meta.ConditionFalse
		if ready(p) {
			condition = meta.ConditionTrue
		}
		previous := apimeta.FindStatusCondition(w.Status.Conditions, "Ready")
		if previous == nil || previous.Status != condition || w.Status.ObservedGeneration != w.Generation || w.Status.ObservedTransitionID != w.Spec.TransitionID {
			w.Status.ObservedGeneration = w.Generation
			w.Status.ObservedTransitionID = w.Spec.TransitionID
			apimeta.SetStatusCondition(&w.Status.Conditions, meta.Condition{Type: "Ready", Status: condition, Reason: "Running", Message: "Readiness follows the active Pod", ObservedGeneration: w.Generation})
			return again, r.Status().Update(ctx, w)
		}
	case "Checkpointing":
		if p == nil {
			return fail(fmt.Errorf("source Pod missing before checkpoint commit"))
		}
		if err = r.Runtime.Checkpoint(ctx, w, p); err != nil {
			if stderrors.Is(err, runtime.ErrRetryableStorage) {
				return again, err
			}
			return fail(err)
		}
		return r.phase(ctx, w, "Suspending", "Snapshot committed; removing source Pod")
	case "Suspending":
		done, err := r.removePod(ctx, w, p)
		if err != nil || !done {
			return again, err
		}
		w.Status.PodUID = ""
		w.Status.PodName = ""
		return r.phase(ctx, w, "Suspended", "Snapshot retained; Pod execution released")
	case "Suspended":
		if w.Spec.DesiredState == "Running" {
			return r.begin(ctx, w)
		}
	default:
		return fail(fmt.Errorf("unknown lifecycle phase %q", w.Status.Phase))
	}
	return again, nil
}

func (r *Reconciler) captureWorkspaceArchive(ctx context.Context, w *api.ResumablePod) (ctrl.Result, bool, error) {
	raw := w.Annotations[api.ArchiveRequestAnnotation]
	if raw == "" {
		return ctrl.Result{}, false, nil
	}
	var request api.ArchiveCaptureRequest
	if err := json.Unmarshal([]byte(raw), &request); err != nil || !regexp.MustCompile(`^arc-[0-9a-f]{32}$`).MatchString(request.ID) {
		return r.publishArchiveResult(ctx, w, raw, request, api.ArchiveCaptureResult{Error: "invalid archive request"})
	}
	if resultRaw := w.Annotations[api.ArchiveResultAnnotation]; resultRaw != "" {
		var prior api.ArchiveCaptureResult
		if json.Unmarshal([]byte(resultRaw), &prior) == nil && prior.ID == request.ID {
			return ctrl.Result{}, false, nil
		}
	}
	result := api.ArchiveCaptureResult{ID: request.ID, Snapshot: request.Snapshot}
	if !request.ExpiresAt.IsZero() && !time.Now().Before(request.ExpiresAt) {
		result.Error = "archive request expired"
		return r.publishArchiveResult(ctx, w, raw, request, result)
	}
	if err := validateWorkspaceArchiveSource(w, request); err != nil {
		result.Error = err.Error()
		return r.publishArchiveResult(ctx, w, raw, request, result)
	}
	archiver, ok := r.Runtime.(workspaceArchiveRuntime)
	if !ok {
		result.Error = "node runtime does not support workspace archives"
		return r.publishArchiveResult(ctx, w, raw, request, result)
	}
	captureCtx, cancel := context.WithTimeout(ctx, 12*time.Minute)
	captured, captureErr := archiver.CaptureWorkspaceArchive(captureCtx, w, request)
	cancel()
	if captureErr != nil {
		if stderrors.Is(captureErr, runtime.ErrRetryableStorage) {
			return again, true, captureErr
		}
		ctrl.LoggerFrom(ctx).Error(captureErr, "workspace archive capture failed", "namespace", w.Namespace, "name", w.Name, "requestID", request.ID)
		result.Error = "archive capture failed"
	} else if captured.ID != request.ID || captured.Snapshot != request.Snapshot || captured.SourceUID != string(w.UID) || captured.SourceGeneration != w.Generation || captured.Size < 1 || !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(captured.SHA256) {
		result.Error = "archive capture returned invalid metadata"
	} else {
		result = captured
	}
	return r.publishArchiveResult(ctx, w, raw, request, result)
}

func validateWorkspaceArchiveSource(w *api.ResumablePod, request api.ArchiveCaptureRequest) error {
	if w.DeletionTimestamp != nil || !w.Spec.PersistentHome || w.Status.PodUID != "" || w.Status.PodName != "" {
		return stderrors.New("workspace archive requires a stopped persistent HOME")
	}
	if w.Status.SpecHash == "" || w.Status.SpecHash != fingerprint(w) || request.Snapshot != w.Status.Snapshot {
		return stderrors.New("workspace archive source changed")
	}
	if w.Status.Phase == "Suspended" {
		if w.Spec.DesiredState != "Suspended" || w.Status.Snapshot == "" {
			return stderrors.New("workspace archive requires a suspended checkpoint")
		}
		return nil
	}
	if w.Status.Phase != "Failed" {
		return stderrors.New("workspace archive requires a suspended or failed execution")
	}
	return nil
}

func sameWorkspaceArchiveSource(before, current *api.ResumablePod, raw string) bool {
	return current.UID == before.UID && current.Generation == before.Generation && current.DeletionTimestamp == nil &&
		current.Spec.TransitionID == before.Spec.TransitionID &&
		fingerprint(current) == fingerprint(before) && current.Status.SpecHash == before.Status.SpecHash &&
		current.Status.Phase == before.Status.Phase && current.Status.Snapshot == before.Status.Snapshot &&
		current.Status.PodUID == before.Status.PodUID && current.Status.PodName == before.Status.PodName &&
		current.Annotations[api.ArchiveRequestAnnotation] == raw
}

func (r *Reconciler) publishArchiveResult(ctx context.Context, w *api.ResumablePod, raw string, request api.ArchiveCaptureRequest, result api.ArchiveCaptureResult) (ctrl.Result, bool, error) {
	current := &api.ResumablePod{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(w), current); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, false, nil
		}
		return again, true, err
	}
	if current.Annotations[api.ArchiveRequestAnnotation] != raw || !sameWorkspaceArchiveSource(w, current, raw) {
		return ctrl.Result{}, false, nil
	}
	if !request.ExpiresAt.IsZero() && !time.Now().Before(request.ExpiresAt) {
		result = api.ArchiveCaptureResult{ID: request.ID, Snapshot: request.Snapshot, Error: "archive request expired"}
	}
	result.SourceUID, result.SourceGeneration = string(current.UID), current.Generation
	if current.Annotations == nil {
		current.Annotations = map[string]string{}
	}
	encoded, _ := json.Marshal(result)
	current.Annotations[api.ArchiveResultAnnotation] = string(encoded)
	if err := r.Update(ctx, current); err != nil {
		return again, true, fmt.Errorf("publish workspace archive result: %w", err)
	}
	return again, true, nil
}
