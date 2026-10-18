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

{{- define "cellbox.backupPasswordSecret" -}}
{{- $backups := .Values.api.volumeBackups | default dict -}}
{{- if and $backups.password $backups.passwordSecret -}}
{{- fail "configure only one of api.volumeBackups.password and api.volumeBackups.passwordSecret" -}}
{{- end -}}
{{- if $backups.password -}}
{{- printf "%s-volume-backups" .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $backups.passwordSecret | default "" -}}
{{- end -}}
{{- end -}}

{{- define "cellbox.objectStorageConfig" -}}
{{- $storage := .Values.objectStorage -}}
{{- $config := dict -}}
{{- if $storage.bucket -}}
{{- $config = dict "endpoint" $storage.endpoint "bucket" $storage.bucket "region" $storage.region "prefix" $storage.prefix "pathStyle" $storage.pathStyle -}}
{{- if $storage.credentialsSecret -}}
{{- $_ := set $config "accessKeyEnv" "OSS_ACCESS_KEY" -}}
{{- $_ := set $config "secretKeyEnv" "OSS_SECRET_KEY" -}}
{{- end -}}
{{- end -}}
{{- toJson $config -}}
{{- end -}}
