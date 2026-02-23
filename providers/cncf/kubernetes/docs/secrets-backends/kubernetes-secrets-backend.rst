 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

.. _kubernetes_secrets_backend:

Kubernetes Secrets Backend
^^^^^^^^^^^^^^^^^^^^^^^^^^

This topic describes how to configure Airflow to use
`Kubernetes Secrets <https://kubernetes.io/docs/concepts/configuration/secret/>`__
as a secrets backend for retrieving connections, variables, and configuration.

This backend discovers secrets using Kubernetes labels, so the secret name does not matter.
This makes it a natural fit when Airflow is running on Kubernetes and integrates well
with tools like `External Secrets Operator (ESO) <https://external-secrets.io/>`__,
`Sealed Secrets <https://sealed-secrets.netlify.app/>`__, or any tool that creates
Kubernetes secrets -- regardless of naming conventions.

Before you begin
""""""""""""""""

Before you start, make sure you have performed the following tasks:

1.  Include the ``cncf.kubernetes`` provider as part of your Airflow installation:

    .. code-block:: bash

        pip install apache-airflow-providers-cncf-kubernetes

2.  Ensure Airflow is running inside a Kubernetes cluster (in-cluster mode).

3.  Ensure the pod's service account has permission to list secrets in the target namespace (by default, the namespace where Airflow runs).

Enabling the secret backend
"""""""""""""""""""""""""""

To enable the Kubernetes secrets backend, specify
:py:class:`~airflow.providers.cncf.kubernetes.secrets.kubernetes_secrets_backend.KubernetesSecretsBackend`
as the ``backend`` in the ``[secrets]`` section of ``airflow.cfg``.

Here is a sample configuration:

.. code-block:: ini

    [secrets]
    backend = airflow.providers.cncf.kubernetes.secrets.kubernetes_secrets_backend.KubernetesSecretsBackend

You can also set this with environment variables:

.. code-block:: bash

    export AIRFLOW__SECRETS__BACKEND=airflow.providers.cncf.kubernetes.secrets.kubernetes_secrets_backend.KubernetesSecretsBackend

You can verify the correct setting of the configuration options with the ``airflow config get-value`` command:

.. code-block:: console

    $ airflow config get-value secrets backend
    airflow.providers.cncf.kubernetes.secrets.kubernetes_secrets_backend.KubernetesSecretsBackend

Backend parameters
""""""""""""""""""

The following parameters can be passed via ``backend_kwargs`` as a JSON dictionary:

* ``namespace``: Kubernetes namespace to query for secrets. If not set, auto-detected from the pod's service account. Default: auto-detect
* ``connections_label``: Label key used to discover connection secrets. Default: ``"airflow.apache.org/connection-id"``
* ``variables_label``: Label key used to discover variable secrets. Default: ``"airflow.apache.org/variable-key"``
* ``config_label``: Label key used to discover config secrets. Default: ``"airflow.apache.org/config-key"``
* ``connections_data_key``: The data key in the Kubernetes secret that holds the connection value. Default: ``"value"``
* ``variables_data_key``: The data key in the Kubernetes secret that holds the variable value. Default: ``"value"``
* ``config_data_key``: The data key in the Kubernetes secret that holds the config value. Default: ``"value"``

For example, if you want to use custom label keys:

.. code-block:: ini

    [secrets]
    backend = airflow.providers.cncf.kubernetes.secrets.kubernetes_secrets_backend.KubernetesSecretsBackend
    backend_kwargs = {"connections_label": "my-org.io/connection", "variables_label": "my-org.io/variable"}

Authentication
""""""""""""""

The backend uses in-cluster Kubernetes authentication directly via
``kubernetes.config.load_incluster_config()``. By default, the namespace is auto-detected
from the pod's service account metadata
(``/var/run/secrets/kubernetes.io/serviceaccount/namespace``). You can override this by
setting the ``namespace`` parameter in ``backend_kwargs`` to query secrets from a different
namespace. No additional authentication configuration is required.

The backend does **not** use an Airflow connection or KubernetesHook, since the secrets backend
itself is used to resolve connections (using a connection would create a circular dependency).

Optional lookup
"""""""""""""""

Optionally connections, variables, or config may be looked up exclusive of each other or in any combination.
This will prevent requests being sent to the Kubernetes API for the excluded type.

If you want to look up some and not others, set the relevant ``*_label`` parameter to ``null``.

For example, if you only want to look up connections and not variables or config:

.. code-block:: ini

    [secrets]
    backend = airflow.providers.cncf.kubernetes.secrets.kubernetes_secrets_backend.KubernetesSecretsBackend
    backend_kwargs = {"variables_label": null, "config_label": null}

Performance
"""""""""""

The backend queries the Kubernetes API with ``resource_version="0"``, which tells the API server to
serve results from its in-memory watch cache. This makes lookups very fast without requiring any
Airflow-side caching.

If multiple secrets match the same label, the backend will use the first one and log a warning.

Storing and Retrieving Connections
""""""""""""""""""""""""""""""""""

To store a connection, create a Kubernetes secret with a label whose key matches ``connections_label``
(default: ``airflow.apache.org/connection-id``) and whose value is the connection id.
The actual secret value goes in the ``value`` data key (or whatever ``connections_data_key`` is set to).

The value should be the
:ref:`connection URI representation <generating_connection_uri>` or the
:ref:`JSON format <connection-serialization-json-example>` of the connection object.

Example secret YAML for a connection named ``smtp_default``:

.. code-block:: yaml

    apiVersion: v1
    kind: Secret
    metadata:
      name: my-smtp-secret       # name can be anything
      labels:
        airflow.apache.org/connection-id: smtp_default
    data:
      value: <base64-encoded-connection-uri>

You can create a connection secret with ``kubectl``:

.. code-block:: bash

    kubectl create secret generic my-smtp-secret \
        --from-literal=value='smtp://user:password@smtp.example.com:587' \
        --namespace=airflow
    kubectl label secret my-smtp-secret \
        airflow.apache.org/connection-id=smtp_default \
        --namespace=airflow

Or using a JSON connection format:

.. code-block:: bash

    kubectl create secret generic my-postgres-secret \
        --from-literal=value='{"conn_type": "postgres", "host": "db.example.com", "login": "user", "password": "pass", "port": 5432, "schema": "mydb"}' \
        --namespace=airflow
    kubectl label secret my-postgres-secret \
        airflow.apache.org/connection-id=my_postgres_db \
        --namespace=airflow

Storing and Retrieving Variables
""""""""""""""""""""""""""""""""

To store a variable, create a Kubernetes secret with a label whose key matches ``variables_label``
(default: ``airflow.apache.org/variable-key``) and whose value is the variable key.

Example secret YAML for a variable named ``my_var``:

.. code-block:: yaml

    apiVersion: v1
    kind: Secret
    metadata:
      name: my-var-secret        # name can be anything
      labels:
        airflow.apache.org/variable-key: my_var
    data:
      value: <base64-encoded-variable-value>

You can create a variable secret with ``kubectl``:

.. code-block:: bash

    kubectl create secret generic my-var-secret \
        --from-literal=value='my_secret_value' \
        --namespace=airflow
    kubectl label secret my-var-secret \
        airflow.apache.org/variable-key=my_var \
        --namespace=airflow

Using with External Secrets Operator
"""""""""""""""""""""""""""""""""""""

The `External Secrets Operator (ESO) <https://external-secrets.io/>`__ can synchronize secrets from
external stores (AWS Secrets Manager, HashiCorp Vault, GCP Secret Manager, Azure Key Vault, etc.)
into Kubernetes secrets. This backend works seamlessly with ESO -- simply configure ESO to add the
appropriate Airflow label to the generated Kubernetes secret, and Airflow will discover it
automatically. The secret name does not matter, only the label.

For example, an ESO ``ExternalSecret`` resource can use ``metadata.labels`` in its target template
to set ``airflow.apache.org/connection-id: <conn-id>``.

This pattern allows you to use a single secrets backend configuration in Airflow while managing
the actual secret values in your preferred external secret store.

Checking configuration
""""""""""""""""""""""

You can use the ``airflow connections get`` command to check if the connection is correctly read from
the backend secret:

.. code-block:: console

    $ airflow connections get smtp_default
    Id: null
    Connection Id: smtp_default
    Connection Type: smtp
    Host: smtp.example.com
    Schema: ''
    Login: user
    Password: password
    Port: 587
    Is Encrypted: null
    Is Extra Encrypted: null
    Extra: {}
    URI: smtp://user:password@smtp.example.com:587

To check that variables are correctly read from the backend secret, you can use
``airflow variables get``:

.. code-block:: console

    $ airflow variables get my_var
    my_secret_value
