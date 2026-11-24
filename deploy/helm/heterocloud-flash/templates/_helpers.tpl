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
fsGroup: 65532
fsGroupChangePolicy: OnRootMismatch
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

{{- define "flash.placement.env" -}}
- name: FLASH_ZONE_SPREAD
  value: {{ .Values.controller.zoneSpread | quote }}
- name: FLASH_NETWORK_SPREAD
  value: {{ .Values.controller.networkSpread | quote }}
{{- if .Values.capacity.enabled }}
- name: FLASH_CAPACITY_ENDPOINT
  value: {{ printf "https://%s-capacity.%s.svc/" (include "flash.fullname" .) .Release.Namespace | quote }}
- name: FLASH_CAPACITY_CA_FILE
  value: /capacity-ca/ca.crt
{{- end }}
- name: FLASH_WORKLOAD_IDENTITY_ENDPOINT
  value: {{ .Values.workloadIdentity.endpoint | quote }}
- name: FLASH_HELPER_IMAGE
  value: {{ default (include "flash.image" .) .Values.workloadHelperImage | quote }}
{{- if .Values.persistence.enabled }}
- name: FLASH_PERSISTENT_STORAGE_CLASS
  value: {{ required "persistence.storageClass is required" .Values.persistence.storageClass | quote }}
{{- end }}
{{- if .Values.adminVolumeMounts }}
- name: FLASH_ADMIN_VOLUME_MOUNTS_JSON
  value: {{ .Values.adminVolumeMounts | toJson | quote }}
{{- end }}
{{- if .Values.registry.enabled }}
- name: FLASH_REGISTRY_HOST
  value: {{ required "registry.host is required" .Values.registry.host | quote }}
- name: FLASH_REGISTRY_USERNAME
  value: {{ required "registry.username is required" .Values.registry.username | quote }}
- name: FLASH_REGISTRY_PASSWORD_FILE
  value: /var/run/secrets/registry/password
- name: FLASH_REGISTRY_PULL_SECRET
  value: {{ required "registry.workloadPullSecretName is required" .Values.registry.workloadPullSecretName | quote }}
{{- end }}
{{- end }}

{{- define "flash.placement.mounts" -}}
{{- if .Values.capacity.enabled }}
- name: capacity-ca
  mountPath: /capacity-ca
  readOnly: true
{{- end }}
{{- if .Values.registry.enabled }}
- name: registry-auth
  mountPath: /var/run/secrets/registry
  readOnly: true
{{- end }}
{{- end }}

{{- define "flash.placement.volumes" -}}
{{- if .Values.capacity.enabled }}
- name: capacity-ca
  secret:
    secretName: {{ include "flash.fullname" . }}-capacity-tls
    items:
      - key: ca.crt
        path: ca.crt
{{- end }}
{{- if .Values.registry.enabled }}
- name: registry-auth
  secret:
    secretName: {{ required "registry.authSecretName is required" .Values.registry.authSecretName }}
    defaultMode: 0440
    items:
      - key: {{ required "registry.passwordKey is required" .Values.registry.passwordKey }}
        path: password
{{- end }}
{{- end }}
