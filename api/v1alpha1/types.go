// SPDX-License-Identifier: Apache-2.0

// Package v1alpha1 defines the deliberately narrow, node-local v1alpha1 API.
package v1alpha1

import (
	"time"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var GroupVersion = schema.GroupVersion{Group: "cellbox.jingyugao.github.io", Version: "v1alpha1"}

const (
	Kind                     = "Cellbox"
	Finalizer                = "cellbox.jingyugao.github.io/cleanup"
	OwnerLabel               = "cellbox.jingyugao.github.io/owner"
	ServingLabel             = "cellbox.jingyugao.github.io/serving"
	TicketAnnotation         = "dev.gvisor.internal.recovery.ticket"
	Gate                     = "cellbox.jingyugao.github.io/prepared"
	RuntimeClass             = "runsc-recoverable"
	DebugHostMountPath       = "/var/lib/cellbox/debug/host"
	DebugHomeMountPath       = "/home/debug"
	SharedMountPath          = "/var/lib/cellbox/shared"
	ArchiveRequestAnnotation = "cellbox.jingyugao.github.io/archive-request"
	ArchiveResultAnnotation  = "cellbox.jingyugao.github.io/archive-result"
	ResumePathWarmPool       = "warm_pool"
	ResumePathStandard       = "standard"
)

// ArchiveCaptureRequest asks the node controller to export a stopped workspace
// from persistent HOME. ID is also the object-store archive identifier.
type ArchiveCaptureRequest struct {
	ID        string    `json:"id"`
	Snapshot  string    `json:"snapshot"`
	ExpiresAt time.Time `json:"expiresAt"`
}

// ArchiveCaptureResult is written only after archive content is durable.
type ArchiveCaptureResult struct {
	SourceUID        string `json:"sourceUid,omitempty"`
	SourceGeneration int64  `json:"sourceGeneration,omitempty"`
	ID               string `json:"id"`
	Snapshot         string `json:"snapshot"`
	Size             int64  `json:"size,omitempty"`
	SHA256           string `json:"sha256,omitempty"`
	Error            string `json:"error,omitempty"`
}

type Spec struct {
	NodeName     string `json:"nodeName"`
	DesiredState string `json:"desiredState"`
	// TransitionID identifies the Cellbox lifecycle operation that requested
	// DesiredState. A newer operation must fence stale workers by supplying the
	// previously observed transition ID.
	TransitionID string `json:"transitionID,omitempty"`
	// WaitPorts is the TCP port set that must be listening for this resume
	// transition to complete.
	WaitPorts []int `json:"waitPorts,omitempty"`
	// Container is immutable. The controller owns optional directory mounts.
	Container              core.Container     `json:"container"`
	SharedReadOnlyHostPath string             `json:"sharedReadOnlyHostPath,omitempty"`
	DebugReadOnlyHostPath  string             `json:"debugReadOnlyHostPath,omitempty"`
	DebugReadWriteHostPath string             `json:"debugReadWriteHostPath,omitempty"`
	PersistentHome         bool               `json:"persistentHome,omitempty"`
	HomeVolumeUID          string             `json:"homeVolumeUID,omitempty"`
	Mounts                 []VolumeMount      `json:"mounts,omitempty"`
	ServicePorts           []core.ServicePort `json:"servicePorts,omitempty"`
	RetryNonce             string             `json:"retryNonce,omitempty"`
	StartupTimeoutSeconds  int64              `json:"startupTimeoutSeconds,omitempty"`
}

// VolumeMount attaches one independently managed volume into the guest.
type VolumeMount struct {
	UID       string `json:"uid"`
	MountPath string `json:"mountPath"`
	ReadOnly  bool   `json:"readOnly,omitempty"`
}
type Status struct {
	Phase              string `json:"phase,omitempty"`
	Message            string `json:"message,omitempty"`
	ObservedGeneration int64  `json:"observedGeneration,omitempty"`
	// ObservedTransitionID is set with lifecycle phase updates and identifies
	// the transition whose requested state the controller has observed.
	ObservedTransitionID string           `json:"observedTransitionID,omitempty"`
	SpecHash             string           `json:"specHash,omitempty"`
	Cycle                int64            `json:"cycle,omitempty"`
	PodName              string           `json:"podName,omitempty"`
	PodUID               string           `json:"podUID,omitempty"`
	Execution            *Execution       `json:"execution,omitempty"`
	Snapshot             string           `json:"snapshot,omitempty"`
	RetryNonce           string           `json:"retryNonce,omitempty"`
	Since                meta.Time        `json:"since,omitempty"`
	Conditions           []meta.Condition `json:"conditions,omitempty"`
	// ResumePath records the execution source selected for this resume.
	// Empty means no selection has been reported.
	ResumePath string `json:"resumePath,omitempty"`
}
type ResumablePod struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty"`
	Spec            Spec   `json:"spec"`
	Status          Status `json:"status,omitempty"`
}
type ResumablePodList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty"`
	Items         []ResumablePod `json:"items"`
}

func (r *ResumablePod) DeepCopyObject() runtime.Object { return r.DeepCopy() }
func (r *ResumablePod) DeepCopy() *ResumablePod {
	if r == nil {
		return nil
	}
	out := *r
	out.ObjectMeta = *r.ObjectMeta.DeepCopy()
	out.Spec.Container = *r.Spec.Container.DeepCopy()
	if r.Spec.ServicePorts != nil {
		out.Spec.ServicePorts = make([]core.ServicePort, len(r.Spec.ServicePorts))
		for i := range r.Spec.ServicePorts {
			r.Spec.ServicePorts[i].DeepCopyInto(&out.Spec.ServicePorts[i])
		}
	}
	if r.Spec.Mounts != nil {
		out.Spec.Mounts = append([]VolumeMount(nil), r.Spec.Mounts...)
	}
	if r.Spec.WaitPorts != nil {
		out.Spec.WaitPorts = append([]int(nil), r.Spec.WaitPorts...)
	}
	out.Status.Conditions = append([]meta.Condition(nil), r.Status.Conditions...)
	if r.Status.Execution != nil {
		execution := *r.Status.Execution
		out.Status.Execution = &execution
	}
	return &out
}
func (r *ResumablePodList) DeepCopyObject() runtime.Object {
	out := *r
	out.ListMeta = *r.ListMeta.DeepCopy()
	out.Items = make([]ResumablePod, len(r.Items))
	for i := range r.Items {
		out.Items[i] = *r.Items[i].DeepCopy()
	}
	return &out
}
func AddToScheme(s *runtime.Scheme) error {
	s.AddKnownTypeWithName(GroupVersion.WithKind(Kind), &ResumablePod{})
	s.AddKnownTypeWithName(GroupVersion.WithKind(Kind+"List"), &ResumablePodList{})
	meta.AddToGroupVersion(s, GroupVersion)
	return nil
}
