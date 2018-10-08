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
~~~~~~~~~~~~~~~~~~~~~
The MySQL connect type allows to connect with MySQL database.

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
    Specify the charset. Example: {"charset": "utf8"}
    
    .. note::
        If encounter UnicodeDecodeError while working with MySQL connection check the charset defined is matched to the database charset.
