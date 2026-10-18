package api_test

import (
	"os"
	"reflect"
	"strings"
	"testing"

	"cellbox.local/cellbox/internal/catalog"
	"cellbox.local/cellbox/internal/guestapi"
	"cellbox.local/cellbox/internal/service"
	"sigs.k8s.io/yaml"
)

// Keep the checked-in wire contract aligned with actual public response types.
func TestOpenAPIResponseShapesAndReferences(t *testing.T) {
	data, err := os.ReadFile("openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err = yaml.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	if doc["openapi"] != "3.1.0" {
		t.Fatal("unexpected OpenAPI version")
	}
	if strings.Contains(string(data), "Idempotency-Key") {
		t.Fatal("public contract must not promise or expose Idempotency-Key")
	}
	paths := doc["paths"].(map[string]any)
	for endpoint, methods := range map[string][]string{"/v1/boxes": {"get", "post"}, "/v1/boxes/{id}:resume": {"post"}, "/v1/boxes/{id}:suspend": {"post"}, "/v1/boxes/{id}/archives": {"post"}, "/v1/boxes:restore": {"post"}, "/v1/archives": {"get"}, "/v1/archives/{id}": {"get", "delete"}, "/v1/archives/{id}/content": {"get"}, "/v1/templates/{id}/revisions": {"get", "post"}, "/v1/volumes": {"get", "post"}, "/v1/volumes/{id}": {"get", "delete"}} {
		for _, method := range methods {
			if path, ok := paths[endpoint].(map[string]any); !ok || path[method] == nil {
				t.Errorf("missing API contract: %s %s", method, endpoint)
			}
		}
	}
	for _, endpoint := range []string{"/v1/boxes/{id}:ensure-running", "/v1/boxes/{id}:reconcile", "/v1/boxes/{id}:upgrade", "/v1/boxes/{id}:rebuild", "/v1/boxes/{id}:freeze", "/v1/boxes/{id}:unfreeze", "/v1/boxes/{id}/leases", "/v1/leases/{id}", "/v1/routes", "/v1/images", "/v1/images:import", "/v1/images/{id}", "/v1/images/{id}/usage"} {
		if paths[endpoint] != nil {
			t.Errorf("removed API must not appear in the public contract: %s", endpoint)
		}
	}
	for endpoint, method := range map[string]string{"/v1/boxes": "post", "/v1/boxes/{id}:destroy": "post", "/v1/boxes/{id}:suspend": "post", "/v1/boxes/{id}:resume": "post", "/v1/boxes/{id}/archives": "post", "/v1/boxes:restore": "post", "/v1/templates/{id}/revisions": "post", "/v1/volumes": "post", "/v1/volumes/{id}": "delete"} {
		operation := paths[endpoint].(map[string]any)[method].(map[string]any)
		parameters, _ := operation["parameters"].([]any)
		for _, parameter := range parameters {
			if p, ok := parameter.(map[string]any); ok && p["name"] == "Idempotency-Key" {
				t.Errorf("%s %s must not promise idempotency", method, endpoint)
			}
		}
	}
	for endpoint, method := range map[string]string{"/v1/boxes": "post", "/v1/boxes/{id}:destroy": "post", "/v1/boxes/{id}:suspend": "post", "/v1/boxes/{id}:resume": "post", "/v1/boxes/{id}:activate": "post", "/v1/boxes/{id}/archives": "post", "/v1/boxes:restore": "post", "/v1/volumes": "post", "/v1/volumes/{id}": "delete"} {
		operation := paths[endpoint].(map[string]any)[method].(map[string]any)
		responses := operation["responses"].(map[string]any)
		if responses["202"] == nil {
			t.Errorf("%s %s must return 202 with an operation", method, endpoint)
		}
	}
	var walk func(any)
	walk = func(v any) {
		switch v := v.(type) {
		case map[string]any:
			if ref, ok := v["$ref"].(string); ok {
				if !strings.HasPrefix(ref, "#/") {
					t.Errorf("external reference %s", ref)
				} else {
					var target any = doc
					for _, part := range strings.Split(strings.TrimPrefix(ref, "#/"), "/") {
						m, ok := target.(map[string]any)
						if !ok {
							t.Errorf("unresolved %s", ref)
							break
						}
						target = m[part]
						if target == nil {
							t.Errorf("unresolved %s", ref)
							break
						}
					}
				}
			}
			for _, child := range v {
				walk(child)
			}
		case []any:
			for _, child := range v {
				walk(child)
			}
		}
	}
	walk(doc)
	schemas := doc["components"].(map[string]any)["schemas"].(map[string]any)
	capabilities := schemas["Capabilities"].(map[string]any)
	archiveCapability := capabilities["properties"].(map[string]any)["archives"].(map[string]any)["enum"]
	if !reflect.DeepEqual(archiveCapability, []any{"none", "stopped-workspace"}) {
		t.Errorf("archive capability enum = %v, want none/stopped-workspace", archiveCapability)
	}
	types := map[string]any{
		"Box": service.Box{}, "Volume": service.Volume{}, "Mount": service.Mount{}, "RuntimeOperation": service.Operation{},
		"TemplateBuildOperation": catalog.Operation{}, "Template": catalog.Template{}, "TemplateRevision": catalog.Revision{},
		"Execution": service.Execution{}, "Capabilities": service.Capabilities{},
		"Archive": service.Archive{}, "Identity": guestapi.Identity{},
		"ExecResult": guestapi.ExecResult{}, "FileEntry": guestapi.FileEntry{},
	}
	for name, value := range types {
		t.Run(name, func(t *testing.T) {
			schema := schemas[name].(map[string]any)
			properties := schema["properties"].(map[string]any)
			required := map[string]bool{}
			for _, field := range schema["required"].([]any) {
				required[field.(string)] = true
			}
			typ := reflect.TypeOf(value)
			for i := 0; i < typ.NumField(); i++ {
				f := typ.Field(i)
				tag := f.Tag.Get("json")
				key, options, _ := strings.Cut(tag, ",")
				if key == "" || key == "-" {
					continue
				}
				if properties[key] == nil {
					t.Errorf("missing schema property %s", key)
				}
				if required[key] == strings.Contains(options, "omitempty") {
					t.Errorf("required flag differs from Go JSON field %s", key)
				}
			}
			if len(properties) != typ.NumField() {
				t.Errorf("schema has %d properties, Go has %d fields", len(properties), typ.NumField())
			}
		})
	}
	operation := schemas["Operation"].(map[string]any)
	variants, ok := operation["oneOf"].([]any)
	if !ok || len(variants) != 2 {
		t.Fatalf("Operation must be a union of runtime and template build operation shapes: %#v", operation["oneOf"])
	}
	createBox := schemas["CreateBox"].(map[string]any)["properties"].(map[string]any)
	for _, field := range []string{"id", "homeVolumeId", "mounts"} {
		if createBox[field] == nil {
			t.Errorf("CreateBox is missing %s", field)
		}
	}
	const resourceIDPattern = `^[a-z0-9]([-a-z0-9]{0,53}[a-z0-9])?$`
	homeVolumeID := createBox["homeVolumeId"].(map[string]any)
	if homeVolumeID["pattern"] != resourceIDPattern || homeVolumeID["maxLength"] != float64(55) {
		t.Error("CreateBox.homeVolumeId must use the lowercase 55-character resource ID rule")
	}
	for _, schemaName := range []string{"CreateBox", "RestoreBox"} {
		properties := schemas[schemaName].(map[string]any)["properties"].(map[string]any)
		id := properties["id"].(map[string]any)
		if id["pattern"] != resourceIDPattern || id["maxLength"] != float64(55) {
			t.Errorf("%s.id must use the lowercase 55-character resource ID rule", schemaName)
		}
	}
	volumePost := paths["/v1/volumes"].(map[string]any)["post"].(map[string]any)
	volumeRequest := volumePost["requestBody"].(map[string]any)["content"].(map[string]any)["application/json"].(map[string]any)["schema"].(map[string]any)
	volumeID := volumeRequest["properties"].(map[string]any)["id"].(map[string]any)
	if volumeID["pattern"] != resourceIDPattern || volumeID["maxLength"] != float64(55) {
		t.Error("volume id must use the lowercase 55-character resource ID rule")
	}
	if _, accepted := schemas["RestoreBox"].(map[string]any)["properties"].(map[string]any)["homeVolumeId"]; accepted {
		t.Error("RestoreBox must not accept homeVolumeId")
	}
	if _, accepted := schemas["RestoreBox"].(map[string]any)["properties"].(map[string]any)["mounts"]; accepted {
		t.Error("RestoreBox must not accept mounts")
	}
	mountItems := createBox["mounts"].(map[string]any)["items"].(map[string]any)
	if mountItems["$ref"] != "#/components/schemas/Mount" {
		t.Errorf("CreateBox.mounts items = %#v, want Mount reference", mountItems)
	}
}
