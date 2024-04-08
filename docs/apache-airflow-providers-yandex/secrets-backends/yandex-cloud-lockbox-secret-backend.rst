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

This topic describes how to configure Apache Airflow to use `Yandex Lockbox <https://cloud.yandex.com/docs/lockbox>`__
as a secret backend and how to manage secrets.

Getting started
---------------

Before you start, make sure you have installed the ``yandex`` provider in your Apache Airflow installation:

.. code-block:: bash

    pip install apache-airflow-providers-yandex

Enabling the Yandex Lockbox secret backend
------------------------------------------

To enable Yandex Lockbox as a secret backend,
specify :py:class:`~airflow.providers.yandex.secrets.lockbox.LockboxSecretBackend`
as your ``backend`` in the ``[secrets]`` section of the ``airflow.cfg`` file.

Here is a sample configuration:

.. code-block:: ini

    [secrets]
    backend = airflow.providers.yandex.secrets.lockbox.LockboxSecretBackend

You can also set this with an environment variable:

.. code-block:: bash

    export AIRFLOW__SECRETS__BACKEND=airflow.providers.yandex.secrets.lockbox.LockboxSecretBackend

You can verify whether the configuration options have been set up correctly
using the ``airflow config get-value`` command:

.. code-block:: console

    $ airflow config get-value secrets backend
    airflow.providers.yandex.secrets.lockbox.LockboxSecretBackend

Backend parameters
------------------

The next step is to configure backend parameters using the ``backend_kwargs`` options
that allow you to provide the following parameters:

* ``yc_oauth_token``: Specifies the user account OAuth token to connect to Yandex Lockbox. The parameter value should look like ``y3_xx123``.
* ``yc_sa_key_json``: Specifies the service account key in JSON. The parameter value should look like ``{"id": "...", "service_account_id": "...", "private_key": "..."}``.
* ``yc_sa_key_json_path``: Specifies the service account key in JSON file path. The parameter value should look like ``/home/airflow/authorized_key.json``, while the file content should have the following format: ``{"id": "...", "service_account_id": "...", "private_key": "..."}``.
* ``yc_connection_id``: Specifies the connection ID to connect to Yandex Lockbox. The default value is ``yandexcloud_default``.
* ``folder_id``: Specifies the folder ID to search for Yandex Lockbox secrets in. If set to ``None`` (``null`` in JSON), the requests will use the connection ``folder_id``, if specified.
* ``connections_prefix``: Specifies the prefix of the secret to read to get connections. If set to ``None`` (``null`` in JSON), the requests for connections will not be sent to Yandex Lockbox. The default value is ``airflow/connections``.
* ``variables_prefix``: Specifies the prefix of the secret to read to get variables. If set to ``None`` (``null`` in JSON), the requests for variables will not be sent to Yandex Lockbox. The default value is ``airflow/variables``.
* ``config_prefix``: Specifies the prefix of the secret to read to get configurations. If set to ``None`` (``null`` in JSON), the requests for variables will not be sent to Yandex Lockbox. The default value is ``airflow/config``.
* ``sep``: Specifies the separator to concatenate ``secret_prefix`` and ``secret_id``. The default value is ``/``.
* ``endpoint``: Specifies the API endpoint. If set to ``None`` (``null`` in JSON), the requests will use the connection endpoint, if specified; otherwise, they will use the default endpoint.

Make sure to provide all options as a JSON dictionary.

For example, if you want to set ``connections_prefix`` to ``"example-connections-prefix"``
and ``variables_prefix`` to ``"example-variables-prefix"``,
your configuration file should look like this:

.. code-block:: ini

    [secrets]
    backend = airflow.providers.yandex.secrets.lockbox.LockboxSecretBackend
    backend_kwargs = {"connections_prefix": "example-connections-prefix", "variables_prefix": "example-variables-prefix"}

Setting up credentials
----------------------

You need to specify credentials or the ID of the ``yandexcloud`` connection to connect to Yandex Lockbox.

The credentials will be used with the following priority:

* OAuth token
* Service account key in JSON from file
* Service account key in JSON
* Yandex Cloud connection

If you do not specify any credentials, the system will use the default connection ID: ``yandexcloud_default``.

Using an OAuth token to authorize as a user account
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

First, you need to create
an `OAuth token <https://cloud.yandex.com/docs/iam/concepts/authorization/oauth-token>`__ for your user account.
Your token will look like this: ``y3_Vd3eub7w9bIut67GHeL345gfb5GAnd3dZnf08FR1vjeUFve7Yi8hGvc``.

Then, you need to specify the ``folder_id`` and your token in ``backend_kwargs``:

.. code-block:: ini

    [secrets]
    backend_kwargs = {"folder_id": "b1g66mft1vo1n4vbn57j", "yc_oauth_token": "y3_Vd3eub7w9bIut67GHeL345gfb5GAnd3dZnf08FR1vjeUFve7Yi8hGvc"}

Using authorized keys to authorize as a service account
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Before you start, make sure you have `created <https://cloud.yandex.com/docs/iam/operations/sa/create>`__
a Yandex Cloud `service account <https://cloud.yandex.com/docs/iam/concepts/users/service-accounts>`__
with the ``lockbox.viewer`` and ``lockbox.payloadViewer`` permissions.

First, you need to create an `authorized key <https://cloud.yandex.com/docs/iam/concepts/authorization/key>`__
for your service account and save the generated JSON file with both public and private key parts.

Then, you need to specify the ``folder_id`` and key in ``backend_kwargs``:

.. code-block:: ini

    [secrets]
    backend_kwargs = {"folder_id": "b1g66mft1vo1n4vbn57j", "yc_sa_key_json": {"id": "...", "service_account_id": "...", "private_key": "..."}"}

Alternatively, you can specify the path to the JSON file in ``backend_kwargs``:

.. code-block:: ini

    [secrets]
    backend_kwargs = {"folder_id": "b1g66mft1vo1n4vbn57j", "yc_sa_key_json_path": "/home/airflow/authorized_key.json"}

Using Yandex Cloud connection for authorization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

First, you need to create :ref:`Yandex Cloud connection <yandex_cloud_connection>`.

Then, you need to specify the ``connection_id`` in ``backend_kwargs``:

.. code-block:: ini

    [secrets]
    backend_kwargs = {"yc_connection_id": "my_yc_connection"}

If you do not specify any credentials,
Lockbox Secret Backend will try to use the default connection ID: ``yandexcloud_default``.

Lockbox Secret Backend will try to use the default folder ID from your connection.
You can also specify the ``folder_id`` in the ``backend_kwargs``:

.. code-block:: ini

    [secrets]
    backend_kwargs = {"folder_id": "b1g66mft1vo1n4vbn57j", "yc_connection_id": "my_yc_connection"}

Storing and retrieving connections
----------------------------------

To store a connection, you need to `create a secret <https://cloud.yandex.com/docs/lockbox/operations/secret-create>`__
with a name in the following format: ``{connections_prefix}{sep}{connection_name}``.

The payload must contain a text value with any key.

Storing a connection as a URI
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The main way to save connections is using a :ref:`connection URI representation <generating_connection_uri>`, such as
``mysql://myname:mypassword@myhost.com?this_param=some+val&that_param=other+val%2A``.

Here is an example of creating a secret with the ``yc`` CLI:

.. code-block:: console

    $ yc lockbox secret create \
        --name airflow/connections/mysqldb \
        --payload '[{"key": "value", "text_value": "mysql://myname:mypassword@myhost.com?this_param=some+val&that_param=other+val%2A"}]'
    done (1s)
    name: airflow/connections/mysqldb

Storing a connection as JSON
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Another way to store connections is using JSON format:

.. code-block:: json

    {
      "conn_type": "mysql",
      "host": "host.com",
      "login": "myname",
      "password": "mypassword",
      "extra": {
        "this_param": "some val",
        "that_param": "other val*"
      }
    }

Here is an example of creating a secret with the ``yc`` CLI:

.. code-block:: console

    $ yc lockbox secret create \
        --name airflow/connections/my_sql_db_json \
        --payload '[{"key": "value", "text_value": "{\"conn_type\": \"mysql\", \"host\": \"host.com\", \"login\": \"myname\", \"password\": \"mypassword\", \"extra\": {\"this_param\": \"some val\", \"that_param\": \"other val*\"}}"}]'
    done (1s)
    name: airflow/connections/my_sql_db_json

Retrieving connection
~~~~~~~~~~~~~~~~~~~~~

To check the connection is correctly read from the Lockbox Secret Backend, you can use ``airflow connections get``:

.. code-block:: console

    $ airflow connections get mysqldb -o json
    [{"id": null, "conn_id": "mysqldb", "conn_type": "mysql", "description": null, "host": "host.com", "schema": "", "login": "myname", "password": "mypassword", "port": null, "is_encrypted": "False", "is_extra_encrypted": "False", "extra_dejson": {"this_param": "some val", "that_param": "other val*"}, "get_uri": "mysql://myname:mypassword@myhost.com/?this_param=some+val&that_param=other+val%2A"}]

Storing and retrieving variables
--------------------------------

To store a variable, you need to `create a secret <https://cloud.yandex.com/docs/lockbox/operations/secret-create>`__
with a name in the following format: ``{variables_prefix}{sep}{variable_name}``.
The payload must contain a text value with any key.

Here is how a variable value may look like: ``some_secret_data``.

Here is an example of creating a secret with the ``yc`` CLI:

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

Storing and retrieving configs
------------------------------

Lockbox Secret Backend is also suitable for storing sensitive configurations.

For example, we will provide you with a secret for ``sentry.sentry_dsn``
and use ``sentry_dsn_value`` as the config value name.

To store a config, you need to `create a secret <https://cloud.yandex.com/docs/lockbox/operations/secret-create>`__
with a name in the following format: ``{config_prefix}{sep}{config_value_name}``.
The payload must contain a text value with any key.

Here is an example of creating a secret with the ``yc`` CLI:

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

Cleaning up your secret
-----------------------

You can easily delete your secret with the ``yc`` CLI:

.. code-block:: console

    $ yc lockbox secret delete --name airflow/connections/mysqldb
    name: airflow/connections/mysqldb
