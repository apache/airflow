# Bitnami Common Library Chart

A [Helm Library Chart](https://helm.sh/docs/topics/library_charts/#helm) for grouping common logic between bitnami charts.

## TL;DR

```yaml
dependencies:
  - name: common
    version: 0.x.x
    repository: https://charts.bitnami.com/bitnami
```

```bash
$ helm dependency update
```

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ include "common.names.fullname" . }}
data:
  myvalue: "Hello World"
```

## Introduction

This chart provides a common template helpers which can be used to develop new charts using [Helm](https://helm.sh) package manager.

Bitnami charts can be used with [Kubeapps](https://kubeapps.com/) for deployment and management of Helm Charts in clusters. This Helm chart has been tested on top of [Bitnami Kubernetes Production Runtime](https://kubeprod.io/) (BKPR). Deploy BKPR to get automated TLS certificates, logging and monitoring for your applications.

## Prerequisites

- Kubernetes 1.12+
- Helm 3.1.0

## Parameters

The following table lists the helpers available in the library which are scoped in different sections.

### Affinities

| Helper identifier             | Description                                          | Expected Input                                 |
|-------------------------------|------------------------------------------------------|------------------------------------------------|
| `common.affinities.node.soft` | Return a soft nodeAffinity definition                | `dict "key" "FOO" "values" (list "BAR" "BAZ")` |
| `common.affinities.node.hard` | Return a hard nodeAffinity definition                | `dict "key" "FOO" "values" (list "BAR" "BAZ")` |
| `common.affinities.pod.soft`  | Return a soft podAffinity/podAntiAffinity definition | `dict "component" "FOO" "context" $`           |
| `common.affinities.pod.hard`  | Return a hard podAffinity/podAntiAffinity definition | `dict "component" "FOO" "context" $`           |

### Capabilities

| Helper identifier                            | Description                                                                                    | Expected Input    |
|----------------------------------------------|------------------------------------------------------------------------------------------------|-------------------|
| `common.capabilities.kubeVersion`            | Return the target Kubernetes version (using client default if .Values.kubeVersion is not set). | `.` Chart context |
| `common.capabilities.deployment.apiVersion`  | Return the appropriate apiVersion for deployment.                                              | `.` Chart context |
| `common.capabilities.statefulset.apiVersion` | Return the appropriate apiVersion for statefulset.                                             | `.` Chart context |
| `common.capabilities.ingress.apiVersion`     | Return the appropriate apiVersion for ingress.                                                 | `.` Chart context |
| `common.capabilities.rbac.apiVersion`        | Return the appropriate apiVersion for RBAC resources.                                          | `.` Chart context |
| `common.capabilities.crd.apiVersion`         | Return the appropriate apiVersion for CRDs.                                                    | `.` Chart context |
| `common.capabilities.policy.apiVersion`      | Return the appropriate apiVersion for policy                                                   | `.` Chart context |
| `common.capabilities.supportsHelmVersion`    | Returns true if the used Helm version is 3.3+                                                  | `.` Chart context |

### Errors

| Helper identifier                       | Description                                                                                                                                                            | Expected Input                                                                      |
|-----------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------|
| `common.errors.upgrade.passwords.empty` | It will ensure required passwords are given when we are upgrading a chart. If `validationErrors` is not empty it will throw an error and will stop the upgrade action. | `dict "validationErrors" (list $validationError00 $validationError01)  "context" $` |

### Images

| Helper identifier           | Description                                          | Expected Input                                                                                          |
|-----------------------------|------------------------------------------------------|---------------------------------------------------------------------------------------------------------|
| `common.images.image`       | Return the proper and full image name                | `dict "imageRoot" .Values.path.to.the.image "global" $`, see [ImageRoot](#imageroot) for the structure. |
| `common.images.pullSecrets` | Return the proper Docker Image Registry Secret Names (deprecated: use common.images.renderPullSecrets instead) | `dict "images" (list .Values.path.to.the.image1, .Values.path.to.the.image2) "global" .Values.global` |
| `common.images.renderPullSecrets` | Return the proper Docker Image Registry Secret Names (evaluates values as templates) | `dict "images" (list .Values.path.to.the.image1, .Values.path.to.the.image2) "context" $` |

### Ingress

| Helper identifier                         | Description                                                          | Expected Input                                                                                                                                                                   |
|-------------------------------------------|----------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `common.ingress.backend`                  | Generate a proper Ingress backend entry depending on the API version | `dict "serviceName" "foo" "servicePort" "bar"`, see the [Ingress deprecation notice](https://kubernetes.io/blog/2019/07/18/api-deprecations-in-1-16/) for the syntax differences |
| `common.ingress.supportsPathType`         | Prints "true" if the pathType field is supported                     | `.` Chart context                                                                                                                                                                |
| `common.ingress.supportsIngressClassname` | Prints "true" if the ingressClassname field is supported             | `.` Chart context                                                                                                                                                                |

### Labels

| Helper identifier           | Description                                          | Expected Input    |
|-----------------------------|------------------------------------------------------|-------------------|
| `common.labels.standard`    | Return Kubernetes standard labels                    | `.` Chart context |
| `common.labels.matchLabels` | Return the proper Docker Image Registry Secret Names | `.` Chart context |

### Names

| Helper identifier       | Description                                                | Expected Inpput   |
|-------------------------|------------------------------------------------------------|-------------------|
| `common.names.name`     | Expand the name of the chart or use `.Values.nameOverride` | `.` Chart context |
| `common.names.fullname` | Create a default fully qualified app name.                 | `.` Chart context |
| `common.names.chart`    | Chart name plus version                                    | `.` Chart context |

### Secrets

| Helper identifier         | Description                                                  | Expected Input                                                                                                                                                                                                                  |
|---------------------------|--------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `common.secrets.name`     | Generate the name of the secret.                             | `dict "existingSecret" .Values.path.to.the.existingSecret "defaultNameSuffix" "mySuffix" "context" $` see [ExistingSecret](#existingsecret) for the structure.                                                                  |
| `common.secrets.key`      | Generate secret key.                                         | `dict "existingSecret" .Values.path.to.the.existingSecret "key" "keyName"` see [ExistingSecret](#existingsecret) for the structure.                                                                                             |
| `common.passwords.manage` | Generate secret password or retrieve one if already created. | `dict "secret" "secret-name" "key" "keyName" "providedValues" (list "path.to.password1" "path.to.password2") "length" 10 "strong" false "chartName" "chartName" "context" $`, length, strong and chartNAme fields are optional. |
| `common.secrets.exists`   | Returns whether a previous generated secret already exists.  | `dict "secret" "secret-name" "context" $`                                                                                                                                                                                       |

### Storage

| Helper identifier             | Description                           | Expected Input                                                                                                      |
|-------------------------------|---------------------------------------|---------------------------------------------------------------------------------------------------------------------|
| `common.affinities.node.soft` | Return a soft nodeAffinity definition | `dict "persistence" .Values.path.to.the.persistence "global" $`, see [Persistence](#persistence) for the structure. |

### TplValues

| Helper identifier         | Description                            | Expected Input                                                                                                                                           |
|---------------------------|----------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| `common.tplvalues.render` | Renders a value that contains template | `dict "value" .Values.path.to.the.Value "context" $`, value is the value should rendered as template, context frequently is the chart context `$` or `.` |

### Utils

| Helper identifier              | Description                                                                              | Expected Input                                                         |
|--------------------------------|------------------------------------------------------------------------------------------|------------------------------------------------------------------------|
| `common.utils.fieldToEnvVar`   | Build environment variable name given a field.                                           | `dict "field" "my-password"`                                           |
| `common.utils.secret.getvalue` | Print instructions to get a secret value.                                                | `dict "secret" "secret-name" "field" "secret-value-field" "context" $` |
| `common.utils.getValueFromKey` | Gets a value from `.Values` object given its key path                                    | `dict "key" "path.to.key" "context" $`                                 |
| `common.utils.getKeyFromList`  | Returns first `.Values` key with a defined value or first of the list if all non-defined | `dict "keys" (list "path.to.key1" "path.to.key2") "context" $`         |

### Validations

| Helper identifier                                | Description                                                                                                                   | Expected Input                                                                                                                                                                                                                                                           |
|--------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `common.validations.values.single.empty`         | Validate a value must not be empty.                                                                                           | `dict "valueKey" "path.to.value" "secret" "secret.name" "field" "my-password" "subchart" "subchart" "context" $` secret, field and subchart are optional. In case they are given, the helper will generate a how to get instruction. See [ValidateValue](#validatevalue) |
| `common.validations.values.multiple.empty`       | Validate a multiple values must not be empty. It returns a shared error for all the values.                                   | `dict "required" (list $validateValueConf00 $validateValueConf01) "context" $`. See [ValidateValue](#validatevalue)                                                                                                                                                      |
| `common.validations.values.mariadb.passwords`    | This helper will ensure required password for MariaDB are not empty. It returns a shared error for all the values.            | `dict "secret" "mariadb-secret" "subchart" "true" "context" $` subchart field is optional and could be true or false it depends on where you will use mariadb chart and the helper.                                                                                      |
| `common.validations.values.postgresql.passwords` | This helper will ensure required password for PostgreSQL are not empty. It returns a shared error for all the values.         | `dict "secret" "postgresql-secret" "subchart" "true" "context" $` subchart field is optional and could be true or false it depends on where you will use postgresql chart and the helper.                                                                                |
| `common.validations.values.redis.passwords`      | This helper will ensure required password for Redis<sup>TM</sup> are not empty. It returns a shared error for all the values. | `dict "secret" "redis-secret" "subchart" "true" "context" $` subchart field is optional and could be true or false it depends on where you will use redis chart and the helper.                                                                                          |
| `common.validations.values.cassandra.passwords`  | This helper will ensure required password for Cassandra are not empty. It returns a shared error for all the values.          | `dict "secret" "cassandra-secret" "subchart" "true" "context" $` subchart field is optional and could be true or false it depends on where you will use cassandra chart and the helper.                                                                                  |
| `common.validations.values.mongodb.passwords`    | This helper will ensure required password for MongoDB&reg; are not empty. It returns a shared error for all the values.            | `dict "secret" "mongodb-secret" "subchart" "true" "context" $` subchart field is optional and could be true or false it depends on where you will use mongodb chart and the helper.                                                                                      |

### Warnings

| Helper identifier            | Description                      | Expected Input                                             |
|------------------------------|----------------------------------|------------------------------------------------------------|
| `common.warnings.rollingTag` | Warning about using rolling tag. | `ImageRoot` see [ImageRoot](#imageroot) for the structure. |

## Special input schemas

### ImageRoot

```yaml
registry:
  type: string
  description: Docker registry where the image is located
  example: docker.io

repository:
  type: string
  description: Repository and image name
  example: bitnami/nginx

tag:
  type: string
  description: image tag
  example: 1.16.1-debian-10-r63

pullPolicy:
  type: string
  description: Specify a imagePullPolicy. Defaults to 'Always' if image tag is 'latest', else set to 'IfNotPresent'

pullSecrets:
  type: array
  items:
    type: string
  description: Optionally specify an array of imagePullSecrets (evaluated as templates).

debug:
  type: boolean
  description: Set to true if you would like to see extra information on logs
  example: false

## An instance would be:
# registry: docker.io
# repository: bitnami/nginx
# tag: 1.16.1-debian-10-r63
# pullPolicy: IfNotPresent
# debug: false
```

### Persistence

```yaml
enabled:
  type: boolean
  description: Whether enable persistence.
  example: true

storageClass:
  type: string
  description: Ghost data Persistent Volume Storage Class, If set to "-", storageClassName: "" which disables dynamic provisioning.
  example: "-"

accessMode:
  type: string
  description: Access mode for the Persistent Volume Storage.
  example: ReadWriteOnce

size:
  type: string
  description: Size the Persistent Volume Storage.
  example: 8Gi

path:
  type: string
  description: Path to be persisted.
  example: /bitnami

## An instance would be:
# enabled: true
# storageClass: "-"
# accessMode: ReadWriteOnce
# size: 8Gi
# path: /bitnami
```

### ExistingSecret

```yaml
name:
  type: string
  description: Name of the existing secret.
  example: mySecret
keyMapping:
  description: Mapping between the expected key name and the name of the key in the existing secret.
  type: object

## An instance would be:
# name: mySecret
# keyMapping:
#   password: myPasswordKey
```

#### Example of use

When we store sensitive data for a deployment in a secret, some times we want to give to users the possibility of using theirs existing secrets.

```yaml
# templates/secret.yaml
---
apiVersion: v1
kind: Secret
metadata:
  name: {{ include "common.names.fullname" . }}
  labels:
    app: {{ include "common.names.fullname" . }}
type: Opaque
data:
  password: {{ .Values.password | b64enc | quote }}

# templates/dpl.yaml
---
...
      env:
        - name: PASSWORD
          valueFrom:
            secretKeyRef:
              name: {{ include "common.secrets.name" (dict "existingSecret" .Values.existingSecret "context" $) }}
              key: {{ include "common.secrets.key" (dict "existingSecret" .Values.existingSecret "key" "password") }}
...

# values.yaml
---
name: mySecret
keyMapping:
  password: myPasswordKey
```

### ValidateValue

#### NOTES.txt

```console
{{- $validateValueConf00 := (dict "valueKey" "path.to.value00" "secret" "secretName" "field" "password-00") -}}
{{- $validateValueConf01 := (dict "valueKey" "path.to.value01" "secret" "secretName" "field" "password-01") -}}

{{ include "common.validations.values.multiple.empty" (dict "required" (list $validateValueConf00 $validateValueConf01) "context" $) }}
```

If we force those values to be empty we will see some alerts

```console
$ helm install test mychart --set path.to.value00="",path.to.value01=""
    'path.to.value00' must not be empty, please add '--set path.to.value00=$PASSWORD_00' to the command. To get the current value:

        export PASSWORD_00=$(kubectl get secret --namespace default secretName -o jsonpath="{.data.password-00}" | base64 --decode)

    'path.to.value01' must not be empty, please add '--set path.to.value01=$PASSWORD_01' to the command. To get the current value:

        export PASSWORD_01=$(kubectl get secret --namespace default secretName -o jsonpath="{.data.password-01}" | base64 --decode)
```

## Upgrading

### To 1.0.0

[On November 13, 2020, Helm v2 support was formally finished](https://github.com/helm/charts#status-of-the-project), this major version is the result of the required changes applied to the Helm Chart to be able to incorporate the different features added in Helm v3 and to be consistent with the Helm project itself regarding the Helm v2 EOL.

**What changes were introduced in this major version?**

- Previous versions of this Helm Chart use `apiVersion: v1` (installable by both Helm 2 and 3), this Helm Chart was updated to `apiVersion: v2` (installable by Helm 3 only). [Here](https://helm.sh/docs/topics/charts/#the-apiversion-field) you can find more information about the `apiVersion` field.
- Use `type: library`. [Here](https://v3.helm.sh/docs/faq/#library-chart-support) you can find more information.
- The different fields present in the *Chart.yaml* file has been ordered alphabetically in a homogeneous way for all the Bitnami Helm Charts

**Considerations when upgrading to this version**

- If you want to upgrade to this version from a previous one installed with Helm v3, you shouldn't face any issues
- If you want to upgrade to this version using Helm v2, this scenario is not supported as this version doesn't support Helm v2 anymore
- If you installed the previous version with Helm v2 and wants to upgrade to this version with Helm v3, please refer to the [official Helm documentation](https://helm.sh/docs/topics/v2_v3_migration/#migration-use-cases) about migrating from Helm v2 to v3

**Useful links**

- https://docs.bitnami.com/tutorials/resolve-helm2-helm3-post-migration-issues/
- https://helm.sh/docs/topics/v2_v3_migration/
- https://helm.sh/blog/migrate-from-helm-v2-to-helm-v3/
