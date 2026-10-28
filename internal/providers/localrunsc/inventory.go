package localrunsc

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/inventory"
	"cellbox.local/cellbox/internal/objectstorage"
)

var (
	_ boxprovider.InventoryProvider         = (*Provider)(nil)
	_ boxprovider.ResourceInventoryProvider = (*Provider)(nil)
)

func (p *Provider) SetObjects(objects objectstorage.Objects) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.objects = objects
}

// Callers hold the provider mutex (or the startup process fence).
func (p *Provider) records() ([]record, error) {
	entries, err := os.ReadDir(filepath.Join(p.base, "boxes"))
	if err != nil {
		return nil, err
	}
	records := make([]record, 0, len(entries))
	for _, entry := range entries {
		r, err := p.load(entry.Name())
		if errors.Is(err, boxprovider.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	return records, nil
}

func (r record) observation() boxprovider.Observation {
	o := boxprovider.Observation{Phase: r.Phase, Generation: r.Generation, TransitionID: r.TransitionID,
		ObservedTransitionID: r.ObservedTransitionID, Message: r.Message}
	if o.Phase == "" {
		o.Phase = "creating"
	}
	if o.Phase == "running" {
		o.ExecutionID = r.execution()
	}
	return o
}

func (r record) observeState(state runtimeState, exists bool) boxprovider.Observation {
	o := r.observation()
	if (o.Phase == "running" && (!exists || state.Status != "running")) || (o.Phase == "suspended" && exists) {
		o.Phase, o.ExecutionID = "failed", ""
		o.Message = "local runsc state disagrees with the committed lifecycle"
	}
	return o
}

func (r record) inventory(observation boxprovider.Observation) (inventory.Record, error) {
	var item inventory.Record
	if err := json.Unmarshal(r.Spec.Inventory, &item); err != nil {
		return item, err
	}
	var box map[string]json.RawMessage
	if err := json.Unmarshal(item.Box, &box); err != nil {
		return item, err
	}
	var id string
	if item.ClientID == "" || box == nil || json.Unmarshal(box["id"], &id) != nil || id != r.Handle.Name {
		return item, errors.New("invalid local sandbox inventory ownership")
	}
	phase := observation.Phase
	item.Staged = r.Spec.Staged && r.InventoryStage != "running"
	if phase == "running" && item.Staged {
		phase = r.InventoryStage
		if phase == "" {
			phase = "restoring"
		}
	}
	box["phase"], _ = json.Marshal(phase)
	box["generation"], _ = json.Marshal(observation.Generation)
	box["imageId"], _ = json.Marshal(r.Handle.ImageID)
	delete(box, "operationId")
	delete(box, "error")
	delete(box, "executionId")
	if observation.ExecutionID != "" {
		box["executionId"], _ = json.Marshal(observation.ExecutionID)
	}
	if phase == "failed" {
		box["error"], _ = json.Marshal(map[string]string{"code": "RUNTIME_ERROR", "message": observation.Message})
	}
	item.Box, _ = json.Marshal(box)
	item.RuntimeID, item.Snapshot, item.NodeName = r.Handle.ID, r.Snapshot, "local"
	return item, nil
}

func (p *Provider) List(ctx context.Context, namespace, client string) ([]inventory.Record, error) {
	return p.list(ctx, namespace, client, true)
}

func (p *Provider) ListResources(ctx context.Context, namespace, client string) ([]inventory.Record, error) {
	return p.list(ctx, namespace, client, false)
}

func (p *Provider) list(ctx context.Context, namespace, client string, verify bool) ([]inventory.Record, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if namespace != "" || client == "" {
		return nil, errors.New("local inventory requires a client and no namespace")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	records, err := p.records()
	if err != nil {
		return nil, err
	}
	states := map[string]runtimeState{}
	if verify {
		data, err := p.run(ctx, "list", "--format=json")
		if err != nil {
			return nil, err
		}
		var runtime []runtimeState
		if err := json.Unmarshal(data, &runtime); err != nil {
			return nil, err
		}
		for _, state := range runtime {
			states[state.ID] = state
		}
	}
	out := []inventory.Record{}
	for _, r := range records {
		observation := r.observation()
		if verify {
			state, exists := states[r.Handle.ID]
			observation = r.observeState(state, exists)
		}
		item, err := r.inventory(observation)
		if err != nil {
			return nil, err
		}
		if item.ClientID == client {
			out = append(out, item)
		}
	}
	return out, nil
}

func (p *Provider) SetInventoryStage(ctx context.Context, handle boxprovider.Handle, stage string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	r, err := p.owned(handle)
	if err != nil {
		return err
	}
	if !r.Spec.Staged || (stage != "staged" && stage != "running") || (r.InventoryStage == "running" && stage != "running") {
		return boxprovider.ErrStaleTransition
	}
	state, err := p.state(ctx, handle.ID)
	if err != nil {
		return err
	}
	if r.Phase != "running" || state.Status != "running" {
		return boxprovider.ErrNotReady
	}
	r.InventoryStage = stage
	return p.save(r)
}
