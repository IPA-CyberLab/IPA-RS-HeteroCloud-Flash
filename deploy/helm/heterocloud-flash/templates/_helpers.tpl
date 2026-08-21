{{- define "flash.name" -}}
heterocloud-flash
{{- end }}

{{- define "flash.fullname" -}}
{{- .Values.fullnameOverride | default (include "flash.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "flash.labels" -}}
app.kubernetes.io/name: {{ include "flash.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/part-of: heterocloud
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "flash.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- .Values.serviceAccount.name | default (include "flash.fullname" .) -}}
{{- else -}}
{{- required "serviceAccount.name is required when serviceAccount.create=false" .Values.serviceAccount.name -}}
{{- end -}}
{{- end }}

{{- define "flash.providerSecretName" -}}
{{- if .Values.providerAuth.existingSecret -}}
{{- .Values.providerAuth.existingSecret -}}
{{- else -}}
{{- printf "%s-provider-auth" (include "flash.fullname" .) -}}
{{- end -}}
{{- end }}

{{- define "flash.image" -}}
{{- printf "%s:%s" .Values.image.repository (.Values.image.tag | default .Chart.AppVersion) -}}
{{- end }}

{{- define "flash.podSecurityContext" -}}
runAsNonRoot: true
runAsUser: 65532
runAsGroup: 65532
seccompProfile:
  type: RuntimeDefault
{{- end }}

{{- define "flash.containerSecurityContext" -}}
allowPrivilegeEscalation: false
capabilities:
  drop:
    - ALL
readOnlyRootFilesystem: true
{{- end }}

{{- define "flash.affinity" -}}
podAntiAffinity:
  requiredDuringSchedulingIgnoredDuringExecution:
    - topologyKey: kubernetes.io/hostname
      labelSelector:
        matchLabels:
          app.kubernetes.io/name: {{ include "flash.name" .root }}
          app.kubernetes.io/component: {{ .component }}
{{- end }}

