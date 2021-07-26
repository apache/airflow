{{/* vim: set filetype=mustache: */}}
{{/*
Through error when upgrading using empty passwords values that must not be empty.

Usage:
{{- $validationError00 := include "common.validations.values.single.empty" (dict "valueKey" "path.to.password00" "secret" "secretName" "field" "password-00") -}}
{{- $validationError01 := include "common.validations.values.single.empty" (dict "valueKey" "path.to.password01" "secret" "secretName" "field" "password-01") -}}
{{ include "common.errors.upgrade.passwords.empty" (dict "validationErrors" (list $validationError00 $validationError01) "context" $) }}

Required password params:
  - validationErrors - String - Required. List of validation strings to be return, if it is empty it won't throw error.
  - context - Context - Required. Parent context.
*/}}
{{- define "common.errors.upgrade.passwords.empty" -}}
  {{- $validationErrors := join "" .validationErrors -}}
  {{- if and $validationErrors .context.Release.IsUpgrade -}}
    {{- $errorString := "\nPASSWORDS ERROR: You must provide your current passwords when upgrading the release." -}}
    {{- $errorString = print $errorString "\n                 Note that even after reinstallation, old credentials may be needed as they may be kept in persistent volume claims." -}}
    {{- $errorString = print $errorString "\n                 Further information can be obtained at https://docs.bitnami.com/general/how-to/troubleshoot-helm-chart-issues/#credential-errors-while-upgrading-chart-releases" -}}
    {{- $errorString = print $errorString "\n%s" -}}
    {{- printf $errorString $validationErrors | fail -}}
  {{- end -}}
{{- end -}}
