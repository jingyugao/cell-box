{{/* Relative image names use imageRegistry; fully qualified references override it. */}}
{{- define "cellbox.image" -}}
{{- $image := required "image reference is required" .image -}}
{{- $first := first (splitList "/" $image) -}}
{{- if and (contains "/" $image) (or (contains "." $first) (contains ":" $first) (eq $first "localhost")) -}}
{{- $image -}}
{{- else -}}
{{- printf "%s/%s" (trimSuffix "/" (required "imageRegistry is required for relative image references" .root.Values.imageRegistry)) $image -}}
{{- end -}}

{{- end -}}
{{- define "cellbox.apiNamespace" -}}
{{- default .Release.Namespace .Values.api.namespace -}}
{{- end -}}
