{{/*
Copyright 2026 Ductile AI, Inc
SPDX-License-Identifier: Apache-2.0
*/}}

{{/* The release's name for everything it makes, at most 50 characters (a StatefulSet's names grow from it). */}}
{{- define "openduct.name" -}}
{{- .Release.Name | trunc 50 | trimSuffix "-" -}}
{{- end -}}

{{- define "openduct.labels" -}}
app.kubernetes.io/name: openduct
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "openduct.selector" -}}
app.kubernetes.io/name: openduct
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/* A cluster: more than one node. */}}
{{- define "openduct.cluster" -}}
{{- if gt (int .Values.replicas) 1 -}}true{{- end -}}
{{- end -}}

{{/* The secret that holds the cluster's secret. */}}
{{- define "openduct.secretName" -}}
{{- if .Values.cluster.secret.existingSecret -}}
{{- .Values.cluster.secret.existingSecret -}}
{{- else -}}
{{- printf "%s-cluster" (include "openduct.name" .) -}}
{{- end -}}
{{- end -}}

{{/*
Every broker, itself included: node <name>-N is broker N+1, reached by its name under the headless service,
as id=replication-address@client-address.
*/}}
{{- define "openduct.brokers" -}}
{{- $name := include "openduct.name" . -}}
{{- $list := list -}}
{{- range $i := until (int .Values.replicas) -}}
{{- $host := printf "%s-%d.%s" $name $i $name -}}
{{- $list = append $list (printf "%d=%s:9093@%s:9092" (add $i 1) $host $host) -}}
{{- end -}}
{{- join "," $list -}}
{{- end -}}

{{- define "openduct.image" -}}
{{- printf "%s:%s" .Values.image.repository (default (printf "v%s" .Chart.AppVersion) .Values.image.tag) -}}
{{- end -}}
