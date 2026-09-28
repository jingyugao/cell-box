// SPDX-License-Identifier: Apache-2.0

// Package v1alpha1 defines the deliberately narrow, node-local v1alpha1 API.
package v1alpha1

import (
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var GroupVersion = schema.GroupVersion{Group: "recovery.gvisor.dev", Version: "v1alpha1"}

const (
	Finalizer          = "recovery.gvisor.dev/cleanup"
	OwnerLabel         = "recovery.gvisor.dev/owner"
	TicketAnnotation   = "dev.gvisor.internal.recovery.ticket"
	Gate               = "recovery.gvisor.dev/prepared"
	RuntimeClass       = "runsc-recoverable"
	DebugHostMountPath = "/var/lib/cellbox/debug/host"
	DebugHomeMountPath = "/home/debug"
)

type Spec struct {
	NodeName     string `json:"nodeName"`
	DesiredState string `json:"desiredState"`
	// Container is immutable. The controller owns optional debug mounts.
	Container              core.Container     `json:"container"`
	DebugReadOnlyHostPath  string             `json:"debugReadOnlyHostPath,omitempty"`
	DebugReadWriteHostPath string             `json:"debugReadWriteHostPath,omitempty"`
	ServicePorts           []core.ServicePort `json:"servicePorts,omitempty"`
	RetryNonce             string             `json:"retryNonce,omitempty"`
	StartupTimeoutSeconds  int64              `json:"startupTimeoutSeconds,omitempty"`
}
type Status struct {
	Phase              string           `json:"phase,omitempty"`
	Message            string           `json:"message,omitempty"`
	ObservedGeneration int64            `json:"observedGeneration,omitempty"`
	SpecHash           string           `json:"specHash,omitempty"`
	Cycle              int64            `json:"cycle,omitempty"`
	PodName            string           `json:"podName,omitempty"`
	PodUID             string           `json:"podUID,omitempty"`
	Snapshot           string           `json:"snapshot,omitempty"`
	RetryNonce         string           `json:"retryNonce,omitempty"`
	Since              meta.Time        `json:"since,omitempty"`
	Conditions         []meta.Condition `json:"conditions,omitempty"`
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
	out.Status.Conditions = append([]meta.Condition(nil), r.Status.Conditions...)
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
	s.AddKnownTypes(GroupVersion, &ResumablePod{}, &ResumablePodList{})
	meta.AddToGroupVersion(s, GroupVersion)
	return nil
}
