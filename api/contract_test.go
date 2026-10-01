package api_test

import (
	"os"
	"reflect"
	"strings"
	"testing"

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
	paths := doc["paths"].(map[string]any)
	for endpoint, methods := range map[string][]string{"/v1/boxes": {"get", "post"}, "/v1/checkpoints": {"get"}, "/v1/images": {"post", "get"}, "/v1/images:import": {"post"}, "/v1/images/{id}": {"get", "delete"}, "/v1/images/{id}/usage": {"get"}} {
		for _, method := range methods {
			if path, ok := paths[endpoint].(map[string]any); !ok || path[method] == nil {
				t.Errorf("missing API contract: %s %s", method, endpoint)
			}
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
	types := map[string]any{"ImportedImage": service.ImportedImage{}, "Box": service.Box{}, "Operation": service.Operation{}, "Execution": service.Execution{}, "Capabilities": service.Capabilities{}, "Lease": service.Lease{}, "Route": service.Route{}, "Grant": service.Grant{}, "AccessRequest": service.AccessRequest{}, "Archive": service.Archive{}, "Identity": guestapi.Identity{}, "ExecResult": guestapi.ExecResult{}, "FileEntry": guestapi.FileEntry{}}
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
}
