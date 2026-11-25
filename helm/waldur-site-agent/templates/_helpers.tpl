{{/*
Expand the name of the chart.
*/}}
{{- define "waldur-site-agent.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "waldur-site-agent.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "waldur-site-agent.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "waldur-site-agent.labels" -}}
helm.sh/chart: {{ include "waldur-site-agent.chart" . }}
{{ include "waldur-site-agent.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "waldur-site-agent.selectorLabels" -}}
app.kubernetes.io/name: {{ include "waldur-site-agent.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}



{{/*
Create the name of the service account to use
*/}}
{{- define "waldur-site-agent.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "waldur-site-agent.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the secret to use
*/}}
{{- define "waldur-site-agent.secretName" -}}
{{- if .Values.secret.name }}
{{- .Values.secret.name }}
{{- else }}
{{- include "waldur-site-agent.fullname" . }}-secret
{{- end }}
{{- end }}

{{/*
Refuse a deployment in which two pods of the same agent would process the same
offerings. Every pod of event-process or order-process reads the whole
configuration and carries out every offering's orders and API key commands, so
a second pod cannot take a share of the work: the two race each other. An API
key acknowledgement does not name the command it answers, so a race can settle
a newer rotation with an older key value. A strategy other than Recreate runs
the old and new pod side by side during every rollout, even at one replica, so
the effective strategy of each enabled agent (see agentStrategy) must be
Recreate. Report and membership-sync are not checked and may roll.

The two agents may run together: order-process skips offerings with
stomp_enabled, and event-process acts only on those.
*/}}
{{- define "waldur-site-agent.validateAgents" -}}
{{- $agents := .Values.agents -}}
{{- range $name := list "eventProcess" "orderProcess" -}}
{{- $agent := index $agents $name -}}
{{- if $agent.enabled -}}
{{- if gt (int $agent.replicas) 1 -}}
{{- fail (printf "agents.%s.replicas is %d. Every pod processes every configured offering, so extra replicas race each other on its orders and API key commands: run one, and split offerings across releases instead." $name (int $agent.replicas)) -}}
{{- end -}}
{{- $key := "strategy.type" -}}
{{- $strategy := $.Values.strategy -}}
{{- if $agent.strategy -}}
{{- $key = printf "agents.%s.strategy.type" $name -}}
{{- $strategy = $agent.strategy -}}
{{- end -}}
{{- $type := default "RollingUpdate" ($strategy | default dict).type -}}
{{- if ne $type "Recreate" -}}
{{- fail (printf "%s is %s, but agents.%s is enabled. Any strategy other than Recreate runs the old and new pod together during a rollout, and they race each other on orders and API key commands: use Recreate for it." $key $type $name) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
The Deployment strategy of one agent: agents.<name>.strategy when set, which
replaces the global strategy outright (a merge could leave rollingUpdate
parameters under type Recreate, which Kubernetes rejects), else the global
strategy. Renders nothing when neither is set.
Usage: include "waldur-site-agent.agentStrategy" (dict "root" . "name" "report")
*/}}
{{- define "waldur-site-agent.agentStrategy" -}}
{{- $strategy := (index .root.Values.agents .name).strategy | default .root.Values.strategy -}}
{{- with $strategy }}{{ toYaml . }}{{ end -}}
{{- end -}}
