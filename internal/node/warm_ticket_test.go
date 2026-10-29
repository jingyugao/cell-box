package node

import (
	"reflect"
	"testing"

	api "github.com/jingyugao/cell-box/api/v1alpha1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// The wrapper restores the pre-created container with the claiming Box's own
// command line, so the ticket must carry it: command and arguments, in order.
func TestWarmTicketCarriesTheClaimingBoxsCommandLine(t *testing.T) {
	p := &core.Pod{ObjectMeta: meta.ObjectMeta{UID: "pod-uid", Name: "warm-pod", Namespace: "cellbox"}}
	w := &api.ResumablePod{ObjectMeta: meta.ObjectMeta{UID: "box-uid"}}
	w.Spec.Container.Command = []string{"/opt/cellbox/bin/cellbox-container-agent"}
	w.Spec.Container.Args = []string{"serve", "--config-base64", "blob"}
	got := warmTicket(w, p, nil).GuestArgv
	want := []string{"/opt/cellbox/bin/cellbox-container-agent", "serve", "--config-base64", "blob"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("GuestArgv = %v, want %v", got, want)
	}
	// The ticket must not alias the resource's slices.
	got[0] = "changed"
	if w.Spec.Container.Command[0] != "/opt/cellbox/bin/cellbox-container-agent" {
		t.Fatal("the ticket shares memory with the resource")
	}
}
