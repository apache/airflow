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


Yandex.Cloud Lockbox Secret Backend
===================================

This topic describes how to configure Apache Airflow to use `Yandex Lockbox <https://cloud.yandex.com/en/docs/lockbox>`__
as a secret backend and how to manage secrets.

Before you begin
----------------

Before you start, make sure you have installed the ``yandex`` provider in your Apache Airflow installation:

.. code-block:: bash

    pip install apache-airflow-providers-yandex

Enabling the Yandex Lockbox secret backend
------------------------------------------

To enable Yandex Lockbox as secrets backend,
specify :py:class:`~airflow.providers.yandex.secrets.lockbox.LockboxSecretBackend`
as the ``backend`` in  ``[secrets]`` section of ``airflow.cfg`` file.

Here is a sample configuration:

.. code-block:: ini

    [secrets]
    backend = airflow.providers.yandex.secrets.lockbox.LockboxSecretBackend

You can also set this with an environment variable:

.. code-block:: bash

    export AIRFLOW__SECRETS__BACKEND=airflow.providers.yandex.secrets.lockbox.LockboxSecretBackend

You can verify the correct setting of the configuration options by using the ``airflow config get-value`` command:

.. code-block:: console

    $ airflow config get-value secrets backend
    airflow.providers.yandex.secrets.lockbox.LockboxSecretBackend

Backend parameters
------------------

The next step is to configure backend parameters using the ``backend_kwargs`` options.
You can pass the following parameters:

* ``yc_oauth_token``: Specifies the user account OAuth token to connect to Yandex Lockbox with. Looks like ``y3_xxxxx``.
* ``yc_sa_key_json``: Specifies the service account auth JSON. Looks like ``{"id": "...", "service_account_id": "...", "private_key": "..."}``.
* ``yc_sa_key_json_path``: Specifies the service account auth JSON file path. Looks like ``/home/airflow/authorized_key.json``. File content looks like ``{"id": "...", "service_account_id": "...", "private_key": "..."}``.
* ``yc_connection_id``: Specifies the connection ID to connect to Yandex Lockbox with. Default: "yandexcloud_default"
* ``folder_id``: Specifies the folder ID to search for Yandex Lockbox secrets in. If set to None (null in JSON), requests will use the connection folder_id if specified.
* ``connections_prefix``: Specifies the prefix of the secret to read to get Connections. If set to None (null in JSON), requests for connections will not be sent to Yandex Lockbox. Default: "airflow/connections"
* ``variables_prefix``: Specifies the prefix of the secret to read to get Variables. If set to None (null in JSON), requests for variables will not be sent to Yandex Lockbox. Default: "airflow/variables"
* ``config_prefix``: Specifies the prefix of the secret to read to get Configurations. If set to None (null in JSON), requests for variables will not be sent to Yandex Lockbox. Default: "airflow/config"
* ``sep``: Specifies the separator used to concatenate secret_prefix and secret_id. Default: "/"
* ``endpoint``: Specifies an API endpoint. Leave blank to use default.

All options should be passed as a JSON dictionary.

For example, if you want to set parameter ``connections_prefix`` to ``"example-connections-prefix"``
and parameter ``variables_prefix`` to ``"example-variables-prefix"``,
your configuration file should look like this:

.. code-block:: ini

    [secrets]
    backend = airflow.providers.yandex.secrets.lockbox.LockboxSecretBackend
    backend_kwargs = {"connections_prefix": "example-connections-prefix", "variables_prefix": "example-variables-prefix"}

Set-up credentials
------------------

You need to specify credentials or id of yandexcloud connection to connect to Yandex Lockbox with.
Credentials will be used with this priority:

* OAuth Token
* Service Account JSON file
* Service Account JSON
* Yandex Cloud Connection

If no credentials specified, default connection id ``yandexcloud_default`` will be used.

Using OAuth token for authorization as users account
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

First, you need to create `OAuth token <https://cloud.yandex.com/en/docs/iam/concepts/authorization/oauth-token>`__ for user account.
It will looks like ``y3_Vdheub7w9bIut67GHeL345gfb5GAnd3dZnf08FRbvjeUFvetYiohGvc``.

Then you need to specify the ``folder_id`` and token in the ``backend_kwargs``:

.. code-block:: ini

    [secrets]
    backend_kwargs = {"folder_id": "b1g66mft1vopnevbn57j", "yc_oauth_token": "y3_Vdheub7w9bIut67GHeL345gfb5GAnd3dZnf08FRbvjeUFvetYiohGvc"}

Using Authorized keys for authorization as service account
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Before you start, make sure you have `created <https://cloud.yandex.com/en/docs/iam/operations/sa/create>`__
a Yandex Cloud `Service Account <https://cloud.yandex.com/en/docs/iam/concepts/users/service-accounts>`__
with the permissions ``lockbox.viewer`` and ``lockbox.payloadViewer``.

First, you need to create `Authorized key <https://cloud.yandex.com/en/docs/iam/concepts/authorization/key>`__
for your service account and save the generated JSON file with public and private key parts.

Then you need to specify the ``folder_id`` and key in the ``backend_kwargs``:

.. code-block:: ini

    [secrets]
    backend_kwargs = {"folder_id": "b1g66mft1vopnevbn57j", "yc_sa_key_json": {"id": "...", "service_account_id": "...", "private_key": "..."}"}

Alternatively, you can specify the path to JSON file in the ``backend_kwargs``:

.. code-block:: ini

    [secrets]
    backend_kwargs = {"folder_id": "b1g66mft1vopnevbn57j", "yc_sa_key_json_path": "/home/airflow/authorized_key.json"}

Using Yandex Cloud Connection for authorization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

First, you need to create :ref:`Yandex Cloud Connection <yandex_cloud_connection>`.

Then you need to specify the ``connection_id`` in the ``backend_kwargs``:

.. code-block:: ini

    [secrets]
    backend_kwargs = {"yc_connection_id": "my_yc_connection"}

If no credentials specified, Lockbox Secret Backend will try to use default connection id ``yandexcloud_default``.

Lockbox Secret Backend will try to use default folder id from Connection,
also you can specify the ``folder_id`` in the ``backend_kwargs``:

.. code-block:: ini

    [secrets]
    backend_kwargs = {"folder_id": "b1g66mft1vopnevbn57j", "yc_connection_id": "my_yc_connection"}

Storing and Retrieving Connections
----------------------------------

To store a Connection, you need to `create secret <https://cloud.yandex.com/en/docs/lockbox/operations/secret-create>`__
with name in format ``{connections_prefix}{sep}{connection_name}``
and payload contains text value with any key.

Storing a Connection as a URI
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The main way is to save connections as a :ref:`connection URI representation <generating_connection_uri>`.

Example: ``mysql://myname:mypassword@myhost.com?this_param=some+val&that_param=other+val%2A``

Here is an example of secret creation with the ``yc`` cli:

.. code-block:: console

    $ yc lockbox secret create \
        --name airflow/connections/mysqldb \
        --payload '[{"key": "value", "text_value": "mysql://myname:mypassword@myhost.com?this_param=some+val&that_param=other+val%2A"}]'
    done (1s)
    name: airflow/connections/mysqldb

Storing a Connection as a JSON
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Alternatively, you can save connections in JSON format:

.. code-block:: json

    {
      "conn_type": "mysql",
      "host": "myhost.com",
      "login": "myname",
      "password": "mypassword",
      "extra": {
        "this_param": "some val",
        "that_param": "other val*"
      }
    }

Here is an example of secret creation with the ``yc`` cli:

.. code-block:: console

    $ yc lockbox secret create \
        --name airflow/connections/mysqldbjson \
        --payload '[{"key": "value", "text_value": "{\"conn_type\": \"mysql\", \"host\": \"myhost.com\", \"login\": \"myname\", \"password\": \"mypassword\", \"extra\": {\"this_param\": \"some val\", \"that_param\": \"other val*\"}}"}]'
    done (1s)
    name: airflow/connections/mysqldbjson

Retrieving Connection
~~~~~~~~~~~~~~~~~~~~~

To check the connection is correctly read from the Lockbox Secret Backend, you can use ``airflow connections get``:

.. code-block:: console

    $ airflow connections get mysqldb -o json
    [{"id": null, "conn_id": "mysqldb", "conn_type": "mysql", "description": null, "host": "myhost.com", "schema": "", "login": "myname", "password": "mypassword", "port": null, "is_encrypted": "False", "is_extra_encrypted": "False", "extra_dejson": {"this_param": "some val", "that_param": "other val*"}, "get_uri": "mysql://myname:mypassword@myhost.com/?this_param=some+val&that_param=other+val%2A"}]

Storing and Retrieving Variables
--------------------------------

To store a Variable, you need to `create secret <https://cloud.yandex.com/en/docs/lockbox/operations/secret-create>`__
with name in format ``{variables_prefix}{sep}{variable_name}``
and payload contains text value with any key.

This is an example variable value: ``some_secret_data``

Here is an example of secret creation with the ``yc`` cli:

.. code-block:: console

    $ yc lockbox secret create \
        --name airflow/variables/my_variable \
        --payload '[{"key": "value", "text_value": "some_secret_data"}]'
    done (1s)
    name: airflow/variables/my_variable

To check the variable is correctly read from the Lockbox Secret Backend, you can use ``airflow variables get``:

.. code-block:: console

    $ airflow variables get my_variable
    some_secret_data

Storing and Retrieving Configs
------------------------------

You can store some sensitive configs in the Lockbox Secret Backend.

For example, we will provide a secret for ``sentry.sentry_dsn`` and use ``sentry_dsn_value`` as the config value name.

To store a Config, you need to `create secret <https://cloud.yandex.com/en/docs/lockbox/operations/secret-create>`__
with name in format ``{config_prefix}{sep}{config_value_name}``
and payload contains text value with any key.

Here is an example of secret creation with the ``yc`` cli:

.. code-block:: console

    $ yc lockbox secret create \
        --name airflow/config/sentry_dsn_value \
        --payload '[{"key": "value", "text_value": "https://public@sentry.example.com/1"}]'
    done (1s)
    name: airflow/config/sentry_dsn_value

Then, we need to specify the config value name as ``{key}_secret`` in the Apache Airflow configuration:

.. code-block:: ini

    [sentry]
    sentry_dsn_secret = sentry_dsn_value

To check the config value is correctly read from the Lockbox Secret Backend, you can use ``airflow config get-value``:

.. code-block:: console

    $ airflow config get-value sentry sentry_dsn
    https://public@sentry.example.com/1

Clean up
--------

You can easily delete your secret with the ``yc`` cli:

.. code-block:: console

    $ yc lockbox secret delete --name airflow/connections/mysqldb
    name: airflow/connections/mysqldb
