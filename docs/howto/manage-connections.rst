..  Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

..  Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

Managing Connections
=====================

Airflow needs to know how to connect to your environment. Information
such as hostname, port, login and passwords to other systems and services is
handled in the ``Admin->Connections`` section of the UI. The pipeline code you
will author will reference the 'conn_id' of the Connection objects.

.. image:: ../img/connections.png

Connections can be created and managed using either the UI or environment
variables.

See the :ref:`Connenctions Concepts <concepts-connections>` documentation for
more information.

Creating a Connection with the UI
---------------------------------

Open the ``Admin->Connections`` section of the UI. Click the ``Create`` link
to create a new connection.

.. image:: ../img/connection_create.png

1. Fill in the ``Conn Id`` field with the desired connection ID. It is
   recommended that you use lower-case characters and separate words with
   underscores.
2. Choose the connection type with the ``Conn Type`` field.
3. Fill in the remaining fields. See
   :ref:`manage-connections-connection-types` for a description of the fields
   belonging to the different connection types.
4. Click the ``Save`` button to create the connection.

Editing a Connection with the UI
--------------------------------

Open the ``Admin->Connections`` section of the UI. Click the pencil icon next
to the connection you wish to edit in the connection list.

.. image:: ../img/connection_edit.png

Modify the connection properties and click the ``Save`` button to save your
changes.

Creating a Connection with Environment Variables
------------------------------------------------

Connections in Airflow pipelines can be created using environment variables.
The environment variable needs to have a prefix of ``AIRFLOW_CONN_`` for
Airflow with the value in a URI format to use the connection properly.

When referencing the connection in the Airflow pipeline, the ``conn_id``
should be the name of the variable without the prefix. For example, if the
``conn_id`` is named ``postgres_master`` the environment variable should be
named ``AIRFLOW_CONN_POSTGRES_MASTER`` (note that the environment variable
must be all uppercase). Airflow assumes the value returned from the
environment variable to be in a URI format (e.g.
``postgres://user:password@localhost:5432/master`` or
``s3://accesskey:secretkey@S3``).

.. _manage-connections-connection-types:

Connection Types
----------------

.. _connection-type-GCP:

Google Cloud Platform
~~~~~~~~~~~~~~~~~~~~~

The Google Cloud Platform connection type enables the :ref:`GCP Integrations
<GCP>`.

Authenticating to GCP
'''''''''''''''''''''

There are two ways to connect to GCP using Airflow.

1. Use `Application Default Credentials
   <https://google-auth.readthedocs.io/en/latest/reference/google.auth.html#google.auth.default>`_,
   such as via the metadata server when running on Google Compute Engine.
2. Use a `service account
   <https://cloud.google.com/docs/authentication/#service_accounts>`_ key
   file (JSON format) on disk.

Default Connection IDs
''''''''''''''''''''''

The following connection IDs are used by default.

``bigquery_default``
    Used by the :class:`~airflow.contrib.hooks.bigquery_hook.BigQueryHook`
    hook.

``google_cloud_datastore_default``
    Used by the :class:`~airflow.contrib.hooks.datastore_hook.DatastoreHook`
    hook.

``google_cloud_default``
    Used by the
    :class:`~airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook`,
    :class:`~airflow.contrib.hooks.gcp_dataflow_hook.DataFlowHook`,
    :class:`~airflow.contrib.hooks.gcp_dataproc_hook.DataProcHook`,
    :class:`~airflow.contrib.hooks.gcp_mlengine_hook.MLEngineHook`, and
    :class:`~airflow.contrib.hooks.gcs_hook.GoogleCloudStorageHook` hooks.

Configuring the Connection
''''''''''''''''''''''''''

Project Id (required)
    The Google Cloud project ID to connect to.

Keyfile Path
    Path to a `service account
    <https://cloud.google.com/docs/authentication/#service_accounts>`_ key
    file (JSON format) on disk.

    Not required if using application default credentials.

Keyfile JSON
    Contents of a `service account
    <https://cloud.google.com/docs/authentication/#service_accounts>`_ key
    file (JSON format) on disk. It is recommended to :doc:`Secure your connections <secure-connections>` if using this method to authenticate.

    Not required if using application default credentials.

Scopes (comma separated)
    A list of comma-separated `Google Cloud scopes
    <https://developers.google.com/identity/protocols/googlescopes>`_ to
    authenticate with.

    .. note::
        Scopes are ignored when using application default credentials. See
        issue `AIRFLOW-2522
        <https://issues.apache.org/jira/browse/AIRFLOW-2522>`_.

MySQL
~~~~~
The MySQL connection type provides connection to a MySQL database.

Configuring the Connection
''''''''''''''''''''''''''
Host (required)
    The host to connect to.

Schema (optional)
    Specify the schema name to be used in the database.

Login (required)
    Specify the user name to connect.
    
Password (required)
    Specify the password to connect.    
    
Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in mysql
    connection. The following parameters are supported:

    * **charset**: specify charset of the connection
    * **cursor**: one of "sscursor", "dictcursor, "ssdictcursor" - specifies cursor class to be
      used
    * **local_infile**: controls MySQL's LOCAL capability (permitting local data loading by
      clients). See `MySQLdb docs <https://mysqlclient.readthedocs.io/user_guide.html>`_
      for details.
    * **unix_socket**: UNIX socket used instead of the default socket
    * **ssl**: Dictionary of SSL parameters that control connecting using SSL (those
      parameters are server specific and should contain "ca", "cert", "key", "capath",
      "cipher" parameters. See
      `MySQLdb docs <https://mysqlclient.readthedocs.io/user_guide.html>`_ for details.
      Note that in order to be useful in URL notation, this parameter might also be
      a string where the SSL dictionary is a string-encoded JSON dictionary.

    Example "extras" field:

    .. code-block:: json

       {
          "charset": "utf8",
          "cursorclass": "sscursor",
          "local_infile": true,
          "unix_socket": "/var/socket",
          "ssl": {
            "cert": "/tmp/client-cert.pem",
            "ca": "/tmp/server-ca.pem'",
            "key": "/tmp/client-key.pem"
          }
       }

    or

    .. code-block:: json

       {
          "charset": "utf8",
          "cursorclass": "sscursor",
          "local_infile": true,
          "unix_socket": "/var/socket",
          "ssl": "{\"cert\": \"/tmp/client-cert.pem\", \"ca\": \"/tmp/server-ca.pem\", \"key\": \"/tmp/client-key.pem\"}"
       }

    When specifying the connection as URI (in AIRFLOW_CONN_* variable) you should specify it
    following the standard syntax of DB connections, where extras are passed as parameters
    of the URI (note that all components of the URI should be URL-encoded).

    For example:

    .. code-block:: bash

       mysql://mysql_user:XXXXXXXXXXXX@1.1.1.1:3306/mysqldb?ssl=%7B%22cert%22%3A+%22%2Ftmp%2Fclient-cert.pem%22%2C+%22ca%22%3A+%22%2Ftmp%2Fserver-ca.pem%22%2C+%22key%22%3A+%22%2Ftmp%2Fclient-key.pem%22%7D

    .. note::
        If encounter UnicodeDecodeError while working with MySQL connection check
        the charset defined is matched to the database charset.

Postgres
~~~~~~~~
The Postgres connection type provides connection to a Postgres database.

Configuring the Connection
''''''''''''''''''''''''''
Host (required)
    The host to connect to.

Schema (optional)
    Specify the schema name to be used in the database.

Login (required)
    Specify the user name to connect.

Password (required)
    Specify the password to connect.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in postgres
    connection. The following parameters out of the standard python parameters
    are supported:

    * **sslmode** - This option determines whether or with what priority a secure SSL
      TCP/IP connection will be negotiated with the server. There are six modes:
      'disable', 'allow', 'prefer', 'require', 'verify-ca', 'verify-full'.
    * **sslcert** - This parameter specifies the file name of the client SSL certificate,
      replacing the default.
    * **sslkey** - This parameter specifies the file name of the client SSL key,
      replacing the default.
    * **sslrootcert** - This parameter specifies the name of a file containing SSL
      certificate authority (CA) certificate(s).
    * **sslcrl** - This parameter specifies the file name of the SSL certificate
      revocation list (CRL).
    * **application_name** - Specifies a value for the application_name
      configuration parameter.
    * **keepalives_idle** - Controls the number of seconds of inactivity after which TCP
      should send a keepalive message to the server.

    More details on all Postgres parameters supported can be found in
    `Postgres documentation <https://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-CONNSTRING>`_

    Example "extras" field:

    .. code-block:: json

       {
          "sslmode": "verify-ca",
          "sslcert": "/tmp/client-cert.pem",
          "sslca": "/tmp/server-ca.pem",
          "sslkey": "/tmp/client-key.pem"
       }

    When specifying the connection as URI (in AIRFLOW_CONN_* variable) you should specify it
    following the standard syntax of DB connections, where extras are passed as parameters
    of the URI (note that all components of the URI should be URL-encoded).

    For example:

    .. code-block:: bash

        postgresql://postgres_user:XXXXXXXXXXXX@1.1.1.1:5432/postgresdb?sslmode=verify-ca&sslcert=%2Ftmp%2Fclient-cert.pem&sslkey=%2Ftmp%2Fclient-key.pem&sslrootcert=%2Ftmp%2Fserver-ca.pem

Cloudsql
~~~~~~~~
The gcpcloudsql:// connection is used by
:class:`airflow.contrib.operators.gcp_sql_operator.CloudSqlQueryOperator` to perform query
on a Google Cloud SQL database. Google Cloud SQL database can be either
Postgres or MySQL, so this is a "meta" connection type - it introduces common schema
for both MySQL and Postgres, including what kind of connectivity should be used.
Google Cloud SQL supports connecting via public IP or via Cloud Sql Proxy
and in the latter case the
:class:`~airflow.contrib.hooks.gcp_sql_hook.CloudSqlDatabaseHook` uses
:class:`~airflow.contrib.hooks.gcp_sql_hook.CloudSqlProxyRunner` to automatically prepare
and use temporary Postgres or MySQL connection that will use the proxy to connect
(either via TCP or UNIX socket)

Configuring the Connection
''''''''''''''''''''''''''
Host (required)
    The host to connect to.

Schema (optional)
    Specify the schema name to be used in the database.

Login (required)
    Specify the user name to connect.

Password (required)
    Specify the password to connect.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in gcpcloudsql
    connection.

    Details of all the parameters supported in extra field can be found in
    :class:`~airflow.contrib.hooks.gcp_sql_hook.CloudSqlDatabaseHook`

    Example "extras" field:

    .. code-block:: json

       {
          "database_type": "mysql",
          "project_id": "example-project",
          "location": "europe-west1",
          "instance": "testinstance",
          "use_proxy": true,
          "sql_proxy_use_tcp": false
       }

    When specifying the connection as URI (in AIRFLOW_CONN_* variable) you should specify it
    following the standard syntax of DB connections, where extras are passed as parameters
    of the URI (note that all components of the URI should be URL-encoded).

    For example:

    .. code-block:: bash

        gcpcloudsql://user:XXXXXXXXX@1.1.1.1:3306/mydb?database_type=mysql&project_id=example-project&location=europe-west1&instance=testinstance&use_proxy=True&sql_proxy_use_tcp=False

SSH
~~~
The SSH connection type provides connection to use :class:`~airflow.contrib.hooks.ssh_hook.SSHHook` to run commands on a remote server using :class:`~airflow.contrib.operators.ssh_operator.SSHOperator` or transfer file from/to the remote server using :class:`~airflow.contrib.operators.ssh_operator.SFTPOperator`.

Configuring the Connection
''''''''''''''''''''''''''
Host (required)
    The Remote host to connect.

Username (optional)
    The Username to connect to the remote_host.

Password (optional)
    Specify the password of the username to connect to the remote_host.

Port (optional)
    Port of remote host to connect. Default is 22.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in ssh
    connection. The following parameters out of the standard python parameters
    are supported:

    * **timeout** - An optional timeout (in seconds) for the TCP connect. Default is ``10``.
    * **compress** - ``true`` to ask the remote client/server to compress traffic; `false` to refuse compression. Default is ``true``.
    * **no_host_key_check** - Set to ``false`` to restrict connecting to hosts with no entries in ``~/.ssh/known_hosts`` (Hosts file). This provides maximum protection against trojan horse attacks, but can be troublesome when the ``/etc/ssh/ssh_known_hosts`` file is poorly maintained or connections to new hosts are frequently made. This option forces the user to manually add all new hosts. Default is ``true``, ssh will automatically add new host keys to the user known hosts files.
    * **allow_host_key_change** - Set to ``true`` if you want to allow connecting to hosts that has host key changed or when you get 'REMOTE HOST IDENTIFICATION HAS CHANGED' error.  This wont protect against Man-In-The-Middle attacks. Other possible solution is to remove the host entry from ``~/.ssh/known_hosts`` file. Default is ``false``.

    Example "extras" field:

    .. code-block:: json

       {
          "timeout": "10",
          "compress": "false",
          "no_host_key_check": "false",
          "allow_host_key_change": "false"
       }

    When specifying the connection as URI (in AIRFLOW_CONN_* variable) you should specify it
    following the standard syntax of connections, where extras are passed as parameters
    of the URI (note that all components of the URI should be URL-encoded).

    For example:

    .. code-block:: bash

        ssh://user:pass@localhost:22?timeout=10&compress=false&no_host_key_check=false&allow_host_key_change=true
