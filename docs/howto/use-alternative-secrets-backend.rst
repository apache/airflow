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


Alternative secrets backend
---------------------------

.. versionadded:: 1.10.10

In addition to retrieving connections & variables from environment variables or the metastore database, you can enable
an alternative secrets backend to retrieve Airflow connections or Airflow variables,
such as :ref:`AWS SSM Parameter Store <ssm_parameter_store_secrets>`,
:ref:`Hashicorp Vault Secrets<hashicorp_vault_secrets>` or you can :ref:`roll your own <roll_your_own_secrets_backend>`.

Search path
^^^^^^^^^^^
When looking up a connection/variable, by default Airflow will search environment variables first and metastore
database second.

If you enable an alternative secrets backend, it will be searched first, followed by environment variables,
then metastore.  This search ordering is not configurable.

.. _secrets_backend_configuration:

Configuration
^^^^^^^^^^^^^

The ``[secrets]`` section has the following options:

.. code-block:: ini

    [secrets]
    backend =
    backend_kwargs =

Set ``backend`` to the fully qualified class name of the backend you want to enable.

You can provide ``backend_kwargs`` with json and it will be passed as kwargs to the ``__init__`` method of
your secrets backend.

See :ref:`AWS SSM Parameter Store <ssm_parameter_store_secrets>` for an example configuration.

.. _ssm_parameter_store_secrets:

AWS SSM Parameter Store Secrets Backend
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To enable SSM parameter store, specify :py:class:`~airflow.providers.amazon.aws.secrets.systems_manager.SystemsManagerParameterStoreBackend`
as the ``backend`` in  ``[secrets]`` section of ``airflow.cfg``.

Here is a sample configuration:

.. code-block:: ini

    [secrets]
    backend = airflow.providers.amazon.aws.secrets.systems_manager.SystemsManagerParameterStoreBackend
    backend_kwargs = {"connections_prefix": "/airflow/connections", "variables_prefix": "/airflow/variables", "profile_name": "default"}

Storing and Retrieving Connections
""""""""""""""""""""""""""""""""""

If you have set ``connections_prefix`` as ``/airflow/connections``, then for a connection id of ``smtp_default``,
you would want to store your connection at ``/airflow/connections/smtp_default``.

Optionally you can supply a profile name to reference aws profile, e.g. defined in ``~/.aws/config``.

The value of the SSM parameter must be the :ref:`connection URI representation <generating_connection_uri>`
of the connection object.

Storing and Retrieving Variables
""""""""""""""""""""""""""""""""

If you have set ``variables_prefix`` as ``/airflow/variables``, then for an Variable key of ``hello``,
you would want to store your Variable at ``/airflow/variables/hello``.

Optionally you can supply a profile name to reference aws profile, e.g. defined in ``~/.aws/config``.

AWS Secrets Manager Backend
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To enable Secrets Manager, specify :py:class:`~airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend`
as the ``backend`` in  ``[secrets]`` section of ``airflow.cfg``.

Here is a sample configuration:

.. code-block:: ini

    [secrets]
    backend = airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend
    backend_kwargs = {"connections_prefix": "airflow/connections", "variables_prefix": "airflow/variables", "profile_name": "default"}

To authenticate you can either supply a profile name to reference aws profile, e.g. defined in ``~/.aws/config`` or set
environment variables like ``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY``.


Storing and Retrieving Connections
""""""""""""""""""""""""""""""""""

If you have set ``connections_prefix`` as ``airflow/connections``, then for a connection id of ``smtp_default``,
you would want to store your connection at ``airflow/connections/smtp_default``.

Example:

.. code-block:: bash

    aws secretsmanager put-secret-value --secret-id airflow/connections/smtp_default --secret-string "smtps://user:host@relay.example.com:465"

Verify that you can get the secret:

.. code-block:: console

    ❯ aws secretsmanager get-secret-value --secret-id airflow/connections/smtp_default
    {
        "ARN": "arn:aws:secretsmanager:us-east-2:314524341751:secret:airflow/connections/smtp_default-7meuul",
        "Name": "airflow/connections/smtp_default",
        "VersionId": "34f90eff-ea21-455a-9c8f-5ee74b21be672",
        "SecretString": "smtps://user:host@relay.example.com:465",
        "VersionStages": [
            "AWSCURRENT"
        ],
        "CreatedDate": "2020-04-08T02:10:35.132000+01:00"
    }

The value of the secret must be the :ref:`connection URI representation <generating_connection_uri>`
of the connection object.

Storing and Retrieving Variables
""""""""""""""""""""""""""""""""

If you have set ``variables_prefix`` as ``airflow/variables``, then for an Variable key of ``hello``,
you would want to store your Variable at ``airflow/variables/hello``.


.. _hashicorp_vault_secrets:

Hashicorp Vault Secrets Backend
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To enable Hashicorp vault to retrieve Airflow connection/variable, specify :py:class:`~airflow.providers.hashicorp.secrets.vault.VaultBackend`
as the ``backend`` in  ``[secrets]`` section of ``airflow.cfg``.

Here is a sample configuration:

.. code-block:: ini

    [secrets]
    backend = airflow.providers.hashicorp.secrets.vault.VaultBackend
    backend_kwargs = {"connections_path": "connections", "variables_path": "variables", "mount_point": "airflow", "url": "http://127.0.0.1:8200"}

The default KV version engine is ``2``, pass ``kv_engine_version: 1`` in ``backend_kwargs`` if you use
KV Secrets Engine Version ``1``.

You can also set and pass values to Vault client by setting environment variables. All the
environment variables listed at https://www.vaultproject.io/docs/commands/#environment-variables are supported.

Hence, if you set ``VAULT_ADDR`` environment variable like below, you do not need to pass ``url``
key to ``backend_kwargs``:

.. code-block:: bash

    export VAULT_ADDR="http://127.0.0.1:8200"


Storing and Retrieving Connections
""""""""""""""""""""""""""""""""""

If you have set ``connections_path`` as ``connections`` and ``mount_point`` as ``airflow``, then for a connection id of
``smtp_default``, you would want to store your secret as:

.. code-block:: bash

    vault kv put airflow/connections/smtp_default conn_uri=smtps://user:host@relay.example.com:465

Note that the ``Key`` is ``conn_uri``, ``Value`` is ``postgresql://airflow:airflow@host:5432/airflow`` and
``mount_point`` is ``airflow``.

You can make a ``mount_point`` for ``airflow`` as follows:

.. code-block:: bash

    vault secrets enable -path=airflow -version=2 kv

Verify that you can get the secret from ``vault``:

.. code-block:: console

    ❯ vault kv get airflow/connections/smtp_default
    ====== Metadata ======
    Key              Value
    ---              -----
    created_time     2020-03-19T19:17:51.281721Z
    deletion_time    n/a
    destroyed        false
    version          1

    ====== Data ======
    Key         Value
    ---         -----
    conn_uri    smtps://user:host@relay.example.com:465

The value of the Vault key must be the :ref:`connection URI representation <generating_connection_uri>`
of the connection object to get connection.

Storing and Retrieving Variables
""""""""""""""""""""""""""""""""

If you have set ``variables_path`` as ``variables`` and ``mount_point`` as ``airflow``, then for a variable with
``hello`` as key, you would want to store your secret as:

.. code-block:: bash

    vault kv put airflow/variables/hello value=world

Verify that you can get the secret from ``vault``:

.. code-block:: console

    ❯ vault kv get airflow/variables/hello
    ====== Metadata ======
    Key              Value
    ---              -----
    created_time     2020-03-28T02:10:54.301784Z
    deletion_time    n/a
    destroyed        false
    version          1

    ==== Data ====
    Key      Value
    ---      -----
    value    world

Note that the secret ``Key`` is ``value``, and secret ``Value`` is ``world`` and
``mount_point`` is ``airflow``.


.. _secrets_manager_backend:

GCP Secrets Manager Backend
^^^^^^^^^^^^^^^^^^^^^^^^^^^

To enable GCP Secrets Manager to retrieve connection/variables, specify :py:class:`~airflow.providers.google.cloud.secrets.secrets_manager.CloudSecretsManagerBackend`
as the ``backend`` in  ``[secrets]`` section of ``airflow.cfg``.

Available parameters to ``backend_kwargs``:

* ``connections_prefix``: Specifies the prefix of the secret to read to get Connections.
* ``variables_prefix``: Specifies the prefix of the secret to read to get Variables.
* ``gcp_key_path``: Path to GCP Credential JSON file
* ``gcp_scopes``: Comma-separated string containing GCP scopes
* ``sep``: separator used to concatenate connections_prefix and conn_id. Default: "-"

Note: The full GCP Secrets Manager secret id should follow the pattern "[a-zA-Z0-9-_]".

Here is a sample configuration if you want to just retrieve connections:

.. code-block:: ini

    [secrets]
    backend = airflow.providers.google.cloud.secrets.secrets_manager.CloudSecretsManagerBackend
    backend_kwargs = {"connections_prefix": "airflow-connections", "sep": "-"}

Here is a sample configuration if you want to just retrieve variables:

.. code-block:: ini

    [secrets]
    backend = airflow.providers.google.cloud.secrets.secrets_manager.CloudSecretsManagerBackend
    backend_kwargs = {"variables_prefix": "airflow-variables", "sep": "-"}

and if you want to retrieve both Variables and connections use the following sample config:

.. code-block:: ini

    [secrets]
    backend = airflow.providers.google.cloud.secrets.secrets_manager.CloudSecretsManagerBackend
    backend_kwargs = {"connections_prefix": "airflow-connections", "variables_prefix": "airflow-variables", "sep": "-"}


When ``gcp_key_path`` is not provided, it will use the Application Default Credentials in the current environment. You can set up the credentials with:

.. code-block:: ini

    # 1. GOOGLE_APPLICATION_CREDENTIALS environment variable
    export GOOGLE_APPLICATION_CREDENTIALS=path/to/key-file.json

    # 2. Set with SDK
    gcloud auth application-default login
    # If the Cloud SDK has an active project, the project ID is returned. The active project can be set using:
    gcloud config set project

The value of the Secrets Manager secret id must be the :ref:`connection URI representation <generating_connection_uri>`
of the connection object.

.. _roll_your_own_secrets_backend:

Roll your own secrets backend
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A secrets backend is a subclass of :py:class:`airflow.secrets.BaseSecretsBackend`, and just has to implement the
:py:meth:`~airflow.secrets.BaseSecretsBackend.get_connections` method.

There are two options:

* Option 1: a base implmentation of the :py:meth:`~airflow.secrets.BaseSecretsBackend.get_connections` is provided, you just need to implement the
  :py:meth:`~airflow.secrets.BaseSecretsBackend.get_conn_uri` method to make it functional.
* Option 2: simply override the :py:meth:`~airflow.secrets.BaseSecretsBackend.get_connections` method.

Just create your class, and put the fully qualified class name in ``backend`` key in the ``[secrets]``
section of ``airflow.cfg``.  You can you can also pass kwargs to ``__init__`` by supplying json to the
``backend_kwargs`` config param.  See :ref:`Configuration <secrets_backend_configuration>` for more details,
and :ref:`SSM Parameter Store <ssm_parameter_store_secrets>` for an example.

.. note::

    If you are rolling your own secrets backend, you don't strictly need to use airflow's URI format. But
    doing so makes it easier to switch between environment variables, the metastore, and your secrets backend.
