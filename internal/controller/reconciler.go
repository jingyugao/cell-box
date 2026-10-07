// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"path"
	"reflect"
	"regexp"
	"strings"
	"time"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/homevolume"
	"cellbox.local/cellbox/internal/runtime"
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
	Runtime  runtime.Backend
	NodeName string
	WarmPool *WarmPool
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

func (r *Reconciler) syncInventoryPhase(ctx context.Context, w *api.ResumablePod, phase string) error {
	syncer, ok := r.Runtime.(inventoryRuntime)
	if !ok {
		return nil
	}
	resource := w.DeepCopy()
	resource.Status.Phase = phase
	return syncer.SyncInventory(ctx, resource)
}

var again = ctrl.Result{RequeueAfter: time.Second}
var errServiceCollision = stderrors.New("Service name is already owned by another workload")
var pullableImage = regexp.MustCompile(`^[a-z0-9][a-z0-9._:/-]*@sha256:[a-f0-9]{64}$`)

func (r *Reconciler) phase(ctx context.Context, w *api.ResumablePod, phase, message string, podReady ...bool) (ctrl.Result, error) {
	w.Status.Message = message
	if err := r.syncInventoryPhase(ctx, w, phase); err != nil {
		return again, err
	}
	if w.Status.Phase != phase {
		w.Status.Since = meta.Now()
	}
	w.Status.Phase = phase
	w.Status.Message = message
	w.Status.ObservedGeneration = w.Generation
	ready := meta.ConditionFalse
	if phase == "Running" {
		if len(podReady) == 0 {
			ready = meta.ConditionUnknown
		} else if podReady[0] {
			ready = meta.ConditionTrue
		}
	}
	apimeta.SetStatusCondition(&w.Status.Conditions, meta.Condition{Type: "Ready", Status: ready, Reason: phase, Message: message, ObservedGeneration: w.Generation})
	return again, r.Status().Update(ctx, w)
}
func fingerprint(w *api.ResumablePod) string {
	legacy := struct {
		Node      string
		Container core.Container
		Ports     []core.ServicePort
	}{w.Spec.NodeName, w.Spec.Container, w.Spec.ServicePorts}
	var b []byte
	if w.Spec.SharedReadOnlyHostPath != "" {
		b, _ = json.Marshal(struct {
			Node                   string
			Container              core.Container
			Ports                  []core.ServicePort
			SharedReadOnlyHostPath string
			DebugReadOnlyHostPath  string
			DebugReadWriteHostPath string
		}{legacy.Node, legacy.Container, legacy.Ports, w.Spec.SharedReadOnlyHostPath, w.Spec.DebugReadOnlyHostPath, w.Spec.DebugReadWriteHostPath})
	} else if w.Spec.DebugReadOnlyHostPath == "" && w.Spec.DebugReadWriteHostPath == "" {
		// Keep the fingerprint of existing workloads unchanged.
		b, _ = json.Marshal(legacy)
	} else if w.Spec.DebugReadWriteHostPath == "" {
		b, _ = json.Marshal(struct {
			Node                  string
			Container             core.Container
			Ports                 []core.ServicePort
			DebugReadOnlyHostPath string
		}{legacy.Node, legacy.Container, legacy.Ports, w.Spec.DebugReadOnlyHostPath})
	} else {
		b, _ = json.Marshal(struct {
			Node                   string
			Container              core.Container
			Ports                  []core.ServicePort
			DebugReadWriteHostPath string
		}{legacy.Node, legacy.Container, legacy.Ports, w.Spec.DebugReadWriteHostPath})
	}
	if w.Spec.PersistentHome {
		// Preserve all existing fingerprints when the optional mode is disabled.
		b, _ = json.Marshal(struct {
			Legacy         json.RawMessage
			PersistentHome bool
		}{b, true})
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
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
func hostVolumes(w *api.ResumablePod) []core.Volume {
	volumes := debugHostVolumes(w)
	if w.Spec.PersistentHome {
		home, _ := homevolume.Path(homevolume.DefaultBase, string(w.UID)) // validate checks the owner first.
		directory := core.HostPathDirectory
		volumes = append(volumes, core.Volume{Name: "agent-home", VolumeSource: core.VolumeSource{HostPath: &core.HostPathVolumeSource{Path: home, Type: &directory}}})
	}
	if w.Spec.SharedReadOnlyHostPath != "" {
		directory := core.HostPathDirectory
		volumes = append(volumes, core.Volume{Name: "shared", VolumeSource: core.VolumeSource{HostPath: &core.HostPathVolumeSource{Path: w.Spec.SharedReadOnlyHostPath, Type: &directory}}})
	}
	return volumes
}
func hostMounts(w *api.ResumablePod) []core.VolumeMount {
	mounts := debugHostMounts(w)
	if w.Spec.PersistentHome {
		mounts = append(mounts, core.VolumeMount{Name: "agent-home", MountPath: homevolume.MountPath})
	}
	if w.Spec.SharedReadOnlyHostPath != "" {
		mounts = append(mounts, core.VolumeMount{Name: "shared", MountPath: api.SharedMountPath, ReadOnly: true})
	}
	return mounts
}
func admittedDebugMount(w *api.ResumablePod, p *core.Pod) bool {
	if len(p.Spec.Containers) != 1 {
		return false
	}
	wantVolumes, wantMounts := hostVolumes(w), hostMounts(w)
	if len(p.Spec.Volumes) != len(wantVolumes) || len(p.Spec.Containers[0].VolumeMounts) != len(wantMounts) {
		return false
	}
	if len(wantVolumes) == 0 {
		return true
	}
	return reflect.DeepEqual(p.Spec.Volumes, wantVolumes) && reflect.DeepEqual(p.Spec.Containers[0].VolumeMounts, wantMounts)
}
func validate(w *api.ResumablePod) error {
	c := w.Spec.Container
	if w.Spec.PersistentHome {
		if _, err := homevolume.Path(homevolume.DefaultBase, string(w.UID)); err != nil {
			return err
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
		return fmt.Errorf("node, container, debug host path and service ports are immutable; revert the change")
	}
	return nil
}
func owned(w *api.ResumablePod, o client.Object) bool {
	ref := meta.GetControllerOf(o)
	return ref != nil && ref.UID == w.UID && ref.Kind == api.Kind && ref.APIVersion == api.GroupVersion.String()
}
func (r *Reconciler) pod(ctx context.Context, w *api.ResumablePod) (*core.Pod, error) {
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
func (r *Reconciler) begin(ctx context.Context, w *api.ResumablePod) (ctrl.Result, error) {
	started := time.Now()
	defer func() {
		ctrl.LoggerFrom(ctx).Info("execution preparation finished", "podUID", w.Status.PodUID, "durationMs", time.Since(started).Milliseconds())
	}()
	var warm *core.Pod
	w.Status.Cycle++
	w.Status.PodName = fmt.Sprintf("cb-%s-%d", string(w.UID)[:8], w.Status.Cycle)
	w.Status.PodUID = ""
	w.Status.Execution = nil
	if w.Status.Snapshot != "" && r.WarmPool != nil && !w.Spec.PersistentHome {
		p, err := r.WarmPool.acquire(ctx, w)
		if err != nil {
			return again, err
		}
		if p != nil {
			warm = p
			w.Status.PodName = p.Name
			w.Status.PodUID = string(p.UID)
		}
		ctrl.LoggerFrom(ctx).Info("warm Pod acquisition finished", "podUID", w.Status.PodUID, "durationMs", time.Since(started).Milliseconds())
	}
	phase := "Creating"
	if w.Status.Snapshot != "" {
		phase = "Restoring"
	}
	result, err := r.phase(ctx, w, phase, "Preparing a new Pod execution")
	ctrl.LoggerFrom(ctx).Info("execution assignment persisted", "podUID", w.Status.PodUID, "durationMs", time.Since(started).Milliseconds())
	if err == nil && warm != nil {
		if backend, ok := r.Runtime.(warmRuntime); ok {
			_, err = backend.ActivateWarm(ctx, w, warm)
		}
		if err == nil {
			return r.waitWarmStarted(ctx, w, warm)
		}
	}
	return result, err
}

func (r *Reconciler) waitWarmStarted(ctx context.Context, w *api.ResumablePod, p *core.Pod) (ctrl.Result, error) {
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

func (r *Reconciler) started(ctx context.Context, w *api.ResumablePod, p *core.Pod) (ctrl.Result, error) {
	if backend, ok := r.Runtime.(snapshotInvalidator); ok && w.Status.Snapshot != "" {
		if err := backend.InvalidateSnapshot(ctx, w); err != nil {
			return again, err
		}
	}
	// Replay eligibility is removed durably before either the Service or the
	// API may connect. Publish Running last so its notification is actionable.
	w.Status.Snapshot = ""
	if _, ok := r.Runtime.(snapshotInvalidator); ok {
		if err := r.serving(ctx, p, true); err != nil {
			return again, err
		}
	}
	result, err := r.phase(ctx, w, "Running", "Container started; previous snapshot cannot be replayed", ready(p))
	ctrl.LoggerFrom(ctx).Info("execution started", "podUID", w.Status.PodUID, "podReady", ready(p))
	return result, err
}
func (r *Reconciler) service(ctx context.Context, w *api.ResumablePod) error {
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
func (r *Reconciler) serving(ctx context.Context, p *core.Pod, on bool) error {
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
func (r *Reconciler) reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	w := &api.ResumablePod{}
	if err := r.Get(ctx, req.NamespacedName, w); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if w.Spec.NodeName != r.NodeName {
		return ctrl.Result{}, nil
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
	if w.Status.Phase == "Failing" {
		done, err := r.removePod(ctx, w, p)
		if err != nil || !done {
			return again, err
		}
		return r.phase(ctx, w, "Failed", w.Status.Message)
	}
	if validationErr := validate(w); validationErr != nil {
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
		w.Status.RetryNonce = w.Spec.RetryNonce
		return r.phase(ctx, w, "Failing", err.Error())
	}
	if err = r.service(ctx, w); err != nil {
		if stderrors.Is(err, errServiceCollision) || errors.IsInvalid(err) {
			return fail(err)
		}
		return again, err
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
			p = executionPod(w, w.Status.PodName)
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
		if !admittedDebugMount(w, p) || len(p.Spec.InitContainers) > 0 || len(p.Spec.EphemeralContainers) > 0 || (p.Spec.NodeName != "" && p.Spec.NodeName != r.NodeName) || p.Spec.RuntimeClassName == nil || *p.Spec.RuntimeClassName != api.RuntimeClass || p.Spec.HostNetwork || p.Spec.HostPID || p.Spec.HostIPC || p.Spec.RestartPolicy != core.RestartPolicyNever || p.Spec.AutomountServiceAccountToken == nil || *p.Spec.AutomountServiceAccountToken {
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
		if previous == nil || previous.Status != condition || w.Status.ObservedGeneration != w.Generation {
			w.Status.ObservedGeneration = w.Generation
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
