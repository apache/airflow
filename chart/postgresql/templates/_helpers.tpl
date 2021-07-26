{{/* vim: set filetype=mustache: */}}

{{/*
Expand the name of the chart.
*/}}
{{- define "postgresql.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "postgresql.primary.fullname" -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- $fullname := default (printf "%s-%s" .Release.Name $name) .Values.fullnameOverride -}}
{{- if .Values.replication.enabled -}}
{{- printf "%s-%s" $fullname "primary" | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s" $fullname | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/*
Return the proper PostgreSQL image name
*/}}
{{- define "postgresql.image" -}}
{{ include "common.images.image" (dict "imageRoot" .Values.image "global" .Values.global) }}
{{- end -}}

{{/*
Return the proper PostgreSQL metrics image name
*/}}
{{- define "postgresql.metrics.image" -}}
{{ include "common.images.image" (dict "imageRoot" .Values.metrics.image "global" .Values.global) }}
{{- end -}}

{{/*
Return the proper image name (for the init container volume-permissions image)
*/}}
{{- define "postgresql.volumePermissions.image" -}}
{{ include "common.images.image" (dict "imageRoot" .Values.volumePermissions.image "global" .Values.global) }}
{{- end -}}

{{/*
Return the proper Docker Image Registry Secret Names
*/}}
{{- define "postgresql.imagePullSecrets" -}}
{{ include "common.images.pullSecrets" (dict "images" (list .Values.image .Values.metrics.image .Values.volumePermissions.image) "global" .Values.global) }}
{{- end -}}

{{/*
Return PostgreSQL postgres user password
*/}}
{{- define "postgresql.postgres.password" -}}
{{- if .Values.global.postgresql.postgresqlPostgresPassword }}
    {{- .Values.global.postgresql.postgresqlPostgresPassword -}}
{{- else if .Values.postgresqlPostgresPassword -}}
    {{- .Values.postgresqlPostgresPassword -}}
{{- else -}}
    {{- randAlphaNum 10 -}}
{{- end -}}
{{- end -}}

{{/*
Return PostgreSQL password
*/}}
{{- define "postgresql.password" -}}
{{- if .Values.global.postgresql.postgresqlPassword }}
    {{- .Values.global.postgresql.postgresqlPassword -}}
{{- else if .Values.postgresqlPassword -}}
    {{- .Values.postgresqlPassword -}}
{{- else -}}
    {{- randAlphaNum 10 -}}
{{- end -}}
{{- end -}}

{{/*
Return PostgreSQL replication password
*/}}
{{- define "postgresql.replication.password" -}}
{{- if .Values.global.postgresql.replicationPassword }}
    {{- .Values.global.postgresql.replicationPassword -}}
{{- else if .Values.replication.password -}}
    {{- .Values.replication.password -}}
{{- else -}}
    {{- randAlphaNum 10 -}}
{{- end -}}
{{- end -}}

{{/*
Return PostgreSQL username
*/}}
{{- define "postgresql.username" -}}
{{- if .Values.global.postgresql.postgresqlUsername }}
    {{- .Values.global.postgresql.postgresqlUsername -}}
{{- else -}}
    {{- .Values.postgresqlUsername -}}
{{- end -}}
{{- end -}}

{{/*
Return PostgreSQL replication username
*/}}
{{- define "postgresql.replication.username" -}}
{{- if .Values.global.postgresql.replicationUser }}
    {{- .Values.global.postgresql.replicationUser -}}
{{- else -}}
    {{- .Values.replication.user -}}
{{- end -}}
{{- end -}}

{{/*
Return PostgreSQL port
*/}}
{{- define "postgresql.port" -}}
{{- if .Values.global.postgresql.servicePort }}
    {{- .Values.global.postgresql.servicePort -}}
{{- else -}}
    {{- .Values.service.port -}}
{{- end -}}
{{- end -}}

{{/*
Return PostgreSQL created database
*/}}
{{- define "postgresql.database" -}}
{{- if .Values.global.postgresql.postgresqlDatabase }}
    {{- .Values.global.postgresql.postgresqlDatabase -}}
{{- else if .Values.postgresqlDatabase -}}
    {{- .Values.postgresqlDatabase -}}
{{- end -}}
{{- end -}}

{{/*
Get the password secret.
*/}}
{{- define "postgresql.secretName" -}}
{{- if .Values.global.postgresql.existingSecret }}
    {{- printf "%s" (tpl .Values.global.postgresql.existingSecret $) -}}
{{- else if .Values.existingSecret -}}
    {{- printf "%s" (tpl .Values.existingSecret $) -}}
{{- else -}}
    {{- printf "%s" (include "common.names.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/*
Return true if we should use an existingSecret.
*/}}
{{- define "postgresql.useExistingSecret" -}}
{{- if or .Values.global.postgresql.existingSecret .Values.existingSecret -}}
    {{- true -}}
{{- end -}}
{{- end -}}

{{/*
Return true if a secret object should be created
*/}}
{{- define "postgresql.createSecret" -}}
{{- if not (include "postgresql.useExistingSecret" .) -}}
    {{- true -}}
{{- end -}}
{{- end -}}

{{/*
Get the configuration ConfigMap name.
*/}}
{{- define "postgresql.configurationCM" -}}
{{- if .Values.configurationConfigMap -}}
{{- printf "%s" (tpl .Values.configurationConfigMap $) -}}
{{- else -}}
{{- printf "%s-configuration" (include "common.names.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/*
Get the extended configuration ConfigMap name.
*/}}
{{- define "postgresql.extendedConfigurationCM" -}}
{{- if .Values.extendedConfConfigMap -}}
{{- printf "%s" (tpl .Values.extendedConfConfigMap $) -}}
{{- else -}}
{{- printf "%s-extended-configuration" (include "common.names.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/*
Return true if a configmap should be mounted with PostgreSQL configuration
*/}}
{{- define "postgresql.mountConfigurationCM" -}}
{{- if or (.Files.Glob "files/postgresql.conf") (.Files.Glob "files/pg_hba.conf") .Values.postgresqlConfiguration .Values.pgHbaConfiguration .Values.configurationConfigMap }}
    {{- true -}}
{{- end -}}
{{- end -}}

{{/*
Get the initialization scripts ConfigMap name.
*/}}
{{- define "postgresql.initdbScriptsCM" -}}
{{- if .Values.initdbScriptsConfigMap -}}
{{- printf "%s" (tpl .Values.initdbScriptsConfigMap $) -}}
{{- else -}}
{{- printf "%s-init-scripts" (include "common.names.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/*
Get the initialization scripts Secret name.
*/}}
{{- define "postgresql.initdbScriptsSecret" -}}
{{- printf "%s" (tpl .Values.initdbScriptsSecret $) -}}
{{- end -}}

{{/*
Get the metrics ConfigMap name.
*/}}
{{- define "postgresql.metricsCM" -}}
{{- printf "%s-metrics" (include "common.names.fullname" .) -}}
{{- end -}}

{{/*
Get the readiness probe command
*/}}
{{- define "postgresql.readinessProbeCommand" -}}
- |
{{- if (include "postgresql.database" .) }}
  exec pg_isready -U {{ include "postgresql.username" . | quote }} -d "dbname={{ include "postgresql.database" . }} {{- if .Values.tls.enabled }} sslcert={{ include "postgresql.tlsCert" . }} sslkey={{ include "postgresql.tlsCertKey" . }}{{- end }}" -h 127.0.0.1 -p {{ template "postgresql.port" . }}
{{- else }}
  exec pg_isready -U {{ include "postgresql.username" . | quote }} {{- if .Values.tls.enabled }} -d "sslcert={{ include "postgresql.tlsCert" . }} sslkey={{ include "postgresql.tlsCertKey" . }}"{{- end }} -h 127.0.0.1 -p {{ template "postgresql.port" . }}
{{- end }}
{{- if contains "bitnami/" .Values.image.repository }}
  [ -f /opt/bitnami/postgresql/tmp/.initialized ] || [ -f /bitnami/postgresql/.initialized ]
{{- end -}}
{{- end -}}

{{/*
Compile all warnings into a single message, and call fail.
*/}}
{{- define "postgresql.validateValues" -}}
{{- $messages := list -}}
{{- $messages := append $messages (include "postgresql.validateValues.ldapConfigurationMethod" .) -}}
{{- $messages := append $messages (include "postgresql.validateValues.psp" .) -}}
{{- $messages := append $messages (include "postgresql.validateValues.tls" .) -}}
{{- $messages := without $messages "" -}}
{{- $message := join "\n" $messages -}}

{{- if $message -}}
{{- printf "\nVALUES VALIDATION:\n%s" $message | fail -}}
{{- end -}}
{{- end -}}

{{/*
Validate values of Postgresql - If ldap.url is used then you don't need the other settings for ldap
*/}}
{{- define "postgresql.validateValues.ldapConfigurationMethod" -}}
{{- if and .Values.ldap.enabled (and (not (empty .Values.ldap.url)) (not (empty .Values.ldap.server))) }}
postgresql: ldap.url, ldap.server
    You cannot set both `ldap.url` and `ldap.server` at the same time.
    Please provide a unique way to configure LDAP.
    More info at https://www.postgresql.org/docs/current/auth-ldap.html
{{- end -}}
{{- end -}}

{{/*
Validate values of Postgresql - If PSP is enabled RBAC should be enabled too
*/}}
{{- define "postgresql.validateValues.psp" -}}
{{- if and .Values.psp.create (not .Values.rbac.create) }}
postgresql: psp.create, rbac.create
    RBAC should be enabled if PSP is enabled in order for PSP to work.
    More info at https://kubernetes.io/docs/concepts/policy/pod-security-policy/#authorizing-policies
{{- end -}}
{{- end -}}

{{/*
Return the appropriate apiVersion for podsecuritypolicy.
*/}}
{{- define "podsecuritypolicy.apiVersion" -}}
{{- if semverCompare "<1.10-0" .Capabilities.KubeVersion.GitVersion -}}
{{- print "extensions/v1beta1" -}}
{{- else -}}
{{- print "policy/v1beta1" -}}
{{- end -}}
{{- end -}}

{{/*
Return the appropriate apiVersion for networkpolicy.
*/}}
{{- define "postgresql.networkPolicy.apiVersion" -}}
{{- if semverCompare ">=1.4-0, <1.7-0" .Capabilities.KubeVersion.GitVersion -}}
"extensions/v1beta1"
{{- else if semverCompare "^1.7-0" .Capabilities.KubeVersion.GitVersion -}}
"networking.k8s.io/v1"
{{- end -}}
{{- end -}}

{{/*
Validate values of Postgresql TLS - When TLS is enabled, so must be VolumePermissions
*/}}
{{- define "postgresql.validateValues.tls" -}}
{{- if and .Values.tls.enabled (not .Values.volumePermissions.enabled) }}
postgresql: tls.enabled, volumePermissions.enabled
    When TLS is enabled you must enable volumePermissions as well to ensure certificates files have
    the right permissions.
{{- end -}}
{{- end -}}

{{/*
Return the path to the cert file.
*/}}
{{- define "postgresql.tlsCert" -}}
{{- if .Values.tls.autoGenerated }}
    {{- printf "/opt/bitnami/postgresql/certs/tls.crt" -}}
{{- else -}}
    {{- required "Certificate filename is required when TLS in enabled" .Values.tls.certFilename | printf "/opt/bitnami/postgresql/certs/%s" -}}
{{- end -}}
{{- end -}}

{{/*
Return the path to the cert key file.
*/}}
{{- define "postgresql.tlsCertKey" -}}
{{- if .Values.tls.autoGenerated }}
    {{- printf "/opt/bitnami/postgresql/certs/tls.key" -}}
{{- else -}}
{{- required "Certificate Key filename is required when TLS in enabled" .Values.tls.certKeyFilename | printf "/opt/bitnami/postgresql/certs/%s" -}}
{{- end -}}
{{- end -}}

{{/*
Return the path to the CA cert file.
*/}}
{{- define "postgresql.tlsCACert" -}}
{{- if .Values.tls.autoGenerated }}
    {{- printf "/opt/bitnami/postgresql/certs/ca.crt" -}}
{{- else -}}
    {{- printf "/opt/bitnami/postgresql/certs/%s" .Values.tls.certCAFilename -}}
{{- end -}}
{{- end -}}

{{/*
Return the path to the CRL file.
*/}}
{{- define "postgresql.tlsCRL" -}}
{{- if .Values.tls.crlFilename -}}
{{- printf "/opt/bitnami/postgresql/certs/%s" .Values.tls.crlFilename -}}
{{- end -}}
{{- end -}}

{{/*
Return true if a TLS credentials secret object should be created
*/}}
{{- define "postgresql.createTlsSecret" -}}
{{- if and .Values.tls.autoGenerated (not .Values.tls.certificatesSecret) }}
    {{- true -}}
{{- end -}}
{{- end -}}

{{/*
Return the path to the CA cert file.
*/}}
{{- define "postgresql.tlsSecretName" -}}
{{- if .Values.tls.autoGenerated }}
    {{- printf "%s-crt" (include "common.names.fullname" .) -}}
{{- else -}}
    {{ required "A secret containing TLS certificates is required when TLS is enabled" .Values.tls.certificatesSecret }}
{{- end -}}
{{- end -}}
