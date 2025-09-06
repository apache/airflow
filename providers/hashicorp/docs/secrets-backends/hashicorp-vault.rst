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

Set up a Vault mount point
""""""""""""""""""""""""""

You can make a ``mount_point`` for ``airflow`` as follows:

.. code-block:: bash

    vault secrets enable -path=airflow -version=2 kv

Optional lookup
"""""""""""""""

Optionally connections, variables, or config may be looked up exclusive of each other or in any combination.
This will prevent requests being sent to Vault for the excluded type.

If you want to look up some and not others in Vault you may do so by setting the relevant ``*_path`` parameter of the ones to be excluded as ``null``.

For example, if you want to set parameter ``connections_path`` to ``"airflow-connections"`` and not look up variables, your configuration file should look like this:

.. code-block:: ini

    [secrets]
    backend = airflow.providers.hashicorp.secrets.vault.VaultBackend
    backend_kwargs = {"connections_path": "airflow-connections", "variables_path": null, "mount_point": "airflow", "url": "http://127.0.0.1:8200"}

Storing and Retrieving Connections using connection URI representation
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

If you have set ``connections_path`` as ``connections`` and ``mount_point`` as ``airflow``, then for a connection id of
``smtp_default``, you would want to store your secret as:

.. code-block:: bash

    vault kv put airflow/connections/smtp_default conn_uri=smtps://user:host@relay.example.com:465

Note that the ``Key`` is ``conn_uri``, ``Value`` is ``smtps://user:host@relay.example.com:465`` and
``mount_point`` is ``airflow``.

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

Storing and Retrieving Connections using Connection class representation
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

If you have set ``connections_path`` as ``connections`` and ``mount_point`` as ``airflow``, then for a connection id of
``smtp_default``, you would want to store your secret as:

.. code-block:: bash

    vault kv put airflow/connections/smtp_default conn_type=smtps login=user password=host host=relay.example.com port=465

Note that the ``Keys`` are parameters of the ``Connection`` class and the ``Value`` their argument.

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
    conn_type   smtps
    login       user
    password    host
    host        relay.example.com
    port        465

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

Storing and Retrieving Config
"""""""""""""""""""""""""""""

If you have set ``config_path`` as ``config`` and ``mount_point`` as ``airflow``, then for config ``sql_alchemy_conn_secret`` with
``sql_alchemy_conn_value`` as value, you would want to store your secret as:

.. code-block:: bash

    vault kv put airflow/config/sql_alchemy_conn_value value=postgres://user:pass@host:5432/db?ssl_mode=disable

Verify that you can get the secret from ``vault``:

.. code-block:: console

    ❯ vault kv get airflow/config/sql_alchemy_conn_value
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
    value    postgres://user:pass@host:5432/db?ssl_mode=disable

Then you can use above secret for ``sql_alchemy_conn_secret`` in your configuration file.

.. code-block:: ini

    [core]
     sql_alchemy_conn_secret: "sql_alchemy_conn_value"

Note that the secret ``Key`` is ``value``, and secret ``Value`` is ``postgres://user:pass@host:5432/db?ssl_mode=disable`` and
``mount_point`` is ``airflow``.

Vault running with self signed certificates
"""""""""""""""""""""""""""""""""""""""""""

Add "verify": "absolute path to ca-certificate file"

.. code-block:: ini

    [secrets]
    backend = airflow.providers.hashicorp.secrets.vault.VaultBackend
    backend_kwargs = {"connections_path": "airflow-connections", "variables_path": null, "mount_point": "airflow", "url": "http://127.0.0.1:8200", "verify": "/etc/ssl/certs/ca-certificates"}

Vault authentication with AWS Assume Role STS
"""""""""""""""""""""""""""""""""""""""""""""

Add parameter "assume_role_kwargs": "The AWS STS assume role auth parameter dict"

For more details, please refer to the AWS Assume Role Authentication documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sts/client/assume_role.html

.. code-block:: ini

    [secrets]
    backend = airflow.providers.hashicorp.secrets.vault.VaultBackend
    backend_kwargs = {"connections_path": "airflow-connections", "variables_path": null, "mount_point": "airflow", "url": "http://127.0.0.1:8200", "auth_type": "aws_iam", "assume_role_kwargs": {"RoleArn":"arn:aws:iam::123456789000:role/hashicorp-aws-iam-role", "RoleSessionName": "Airflow"}}

Using multiple mount points
"""""""""""""""""""""""""""

You can use multiple mount points to store your secrets. For example, you might want to store the Airflow instance configurations
in one Vault KV engine only accessible by your Airflow deployment tools, while storing the variables and connections in another KV engine
available to your Dags, in order to grant them more specific Vault ACLs.

In order to do this, you will need to setup you configuration this way:

* leave ``mount_point`` as JSON ``null``
* if you use ``variables_path`` and/or ``connections_path``, set them as ``"mount_point/path/to/the/secrets"``
  (the string will be split using the separator ``/``, the first element will be the mount point, the remaining
  elements will be the path to the secrets)
* leave ``config_path`` as the empty string ``""``
* if you use ``config_path``, each configuration item will need to be prefixed with the ``mount_point`` used for configs,
  as ``"mount_point/path/to/the/config"`` (here again, the string will be split using the separator ``/``,
  the first element will be the mount point, the remaining elements will be the path to the configuration parameter)

For example:

.. code-block:: ini

    [core]
    sql_alchemy_conn_secret: "deployment_mount_point/airflow/configs/sql_alchemy_conn_value"

    [secrets]
    backend = airflow.providers.hashicorp.secrets.vault.VaultBackend
    backend_kwargs = {"connections_path": "dags_mount_point/airflow/connections", "variables_path": "dags_mount_point/airflow/variables", "config_path": "", mount_point": null, "url": "http://127.0.0.1:8200", "verify": "/etc/ssl/certs/ca-certificates"}
