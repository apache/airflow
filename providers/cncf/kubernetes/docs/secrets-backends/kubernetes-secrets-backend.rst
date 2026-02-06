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

This backend reads secrets directly from the Kubernetes API using a naming convention,
making it a natural fit when Airflow is running on Kubernetes. It also integrates well
with tools like `External Secrets Operator (ESO) <https://external-secrets.io/>`__
that create Kubernetes secrets from external stores.

Before you begin
""""""""""""""""

Before you start, make sure you have performed the following tasks:

1.  Include the ``cncf.kubernetes`` provider as part of your Airflow installation:

    .. code-block:: bash

        pip install apache-airflow-providers-cncf-kubernetes

2.  Ensure Airflow is running inside a Kubernetes cluster (in-cluster mode).

3.  Ensure the pod's service account has permission to read secrets in the namespace where Airflow runs.

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

* ``connections_prefix``: Specifies the prefix of the secret to read to get Connections. Default: ``"airflow-connections"``
* ``variables_prefix``: Specifies the prefix of the secret to read to get Variables. Default: ``"airflow-variables"``
* ``config_prefix``: Specifies the prefix of the secret to read to get Configurations. Default: ``"airflow-config"``
* ``connections_data_key``: The data key in the Kubernetes secret that holds the connection value. Default: ``"value"``
* ``variables_data_key``: The data key in the Kubernetes secret that holds the variable value. Default: ``"value"``
* ``config_data_key``: The data key in the Kubernetes secret that holds the config value. Default: ``"value"``

For example, if you want to use custom prefixes:

.. code-block:: ini

    [secrets]
    backend = airflow.providers.cncf.kubernetes.secrets.kubernetes_secrets_backend.KubernetesSecretsBackend
    backend_kwargs = {"connections_prefix": "my-connections", "variables_prefix": "my-variables"}

Authentication
""""""""""""""

The backend always uses in-cluster Kubernetes authentication via the pod's service account token.
Internally it delegates to
:py:class:`~airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook`
(with ``conn_id=None, in_cluster=True``), which provides timeout handling, TCP keepalive,
and a properly configured API client. The namespace is auto-detected from the pod's service account
metadata (``/var/run/secrets/kubernetes.io/serviceaccount/namespace``), so secrets are read from the
same namespace where Airflow runs. No additional authentication configuration is required.

The backend does **not** use an Airflow connection, since the secrets backend itself is used to resolve
connections (using a connection would create a circular dependency).

Optional lookup
"""""""""""""""

Optionally connections, variables, or config may be looked up exclusive of each other or in any combination.
This will prevent requests being sent to the Kubernetes API for the excluded type.

If you want to look up some and not others, set the relevant ``*_prefix`` parameter to ``null``.

For example, if you only want to look up connections and not variables or config:

.. code-block:: ini

    [secrets]
    backend = airflow.providers.cncf.kubernetes.secrets.kubernetes_secrets_backend.KubernetesSecretsBackend
    backend_kwargs = {"connections_prefix": "airflow-connections", "variables_prefix": null, "config_prefix": null}

Name sanitization
"""""""""""""""""

Kubernetes secret names must conform to
`DNS subdomain naming rules <https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#dns-subdomain-names>`__
(lowercase alphanumeric characters, ``-``, or ``.``). The backend automatically sanitizes the
secret name by:

* Replacing underscores (``_``) with hyphens (``-``)
* Converting to lowercase

For example, a connection with ``conn_id`` of ``my_postgres_db`` and the default prefix
``airflow-connections`` will be looked up as the Kubernetes secret named
``airflow-connections-my-postgres-db``.

Storing and Retrieving Connections
""""""""""""""""""""""""""""""""""

If you have set ``connections_prefix`` as ``airflow-connections`` (the default), then for a connection
id of ``smtp_default``, the backend will look for a Kubernetes secret named
``airflow-connections-smtp-default`` (note the underscore-to-hyphen conversion).

The value must be stored in the secret's data under the key specified by ``connections_data_key``
(default: ``value``). The value should be the
:ref:`connection URI representation <generating_connection_uri>` or the
:ref:`JSON format <connection-serialization-json-example>` of the connection object.

You can create a connection secret with ``kubectl`` (the ``--namespace`` must match the namespace
where Airflow runs):

.. code-block:: bash

    kubectl create secret generic airflow-connections-smtp-default \
        --from-literal=value='smtp://user:password@smtp.example.com:587' \
        --namespace=airflow

Or using a JSON connection format:

.. code-block:: bash

    kubectl create secret generic airflow-connections-my-postgres-db \
        --from-literal=value='{"conn_type": "postgres", "host": "db.example.com", "login": "user", "password": "pass", "port": 5432, "schema": "mydb"}' \
        --namespace=airflow

Storing and Retrieving Variables
""""""""""""""""""""""""""""""""

If you have set ``variables_prefix`` as ``airflow-variables`` (the default), then for a variable
key of ``my_var``, the backend will look for a Kubernetes secret named
``airflow-variables-my-var``.

The value must be stored in the secret's data under the key specified by ``variables_data_key``
(default: ``value``).

You can create a variable secret with ``kubectl`` (the ``--namespace`` must match the namespace
where Airflow runs):

.. code-block:: bash

    kubectl create secret generic airflow-variables-my-var \
        --from-literal=value='my_secret_value' \
        --namespace=airflow

Using with External Secrets Operator
"""""""""""""""""""""""""""""""""""""

The `External Secrets Operator (ESO) <https://external-secrets.io/>`__ can synchronize secrets from
external stores (AWS Secrets Manager, HashiCorp Vault, GCP Secret Manager, Azure Key Vault, etc.)
into Kubernetes secrets. This backend works seamlessly with ESO -- simply configure ESO to create
Kubernetes secrets using the naming convention expected by the backend
(e.g. ``airflow-connections-<conn-id>``), and Airflow will pick them up automatically.

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
