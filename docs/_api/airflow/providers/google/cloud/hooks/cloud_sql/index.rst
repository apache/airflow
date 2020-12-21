:mod:`airflow.providers.google.cloud.hooks.cloud_sql`
=====================================================

.. py:module:: airflow.providers.google.cloud.hooks.cloud_sql

.. autoapi-nested-parse::

   This module contains a Google Cloud SQL Hook.



Module Contents
---------------

.. data:: UNIX_PATH_MAX
   :annotation: = 108

   

.. data:: TIME_TO_SLEEP_IN_SECONDS
   :annotation: = 20

   

.. py:class:: CloudSqlOperationStatus

   Helper class with operation statuses.

   .. attribute:: PENDING
      :annotation: = PENDING

      

   .. attribute:: RUNNING
      :annotation: = RUNNING

      

   .. attribute:: DONE
      :annotation: = DONE

      

   .. attribute:: UNKNOWN
      :annotation: = UNKNOWN

      


.. py:class:: CloudSQLHook(api_version: str, gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None)

   Bases: :class:`airflow.providers.google.common.hooks.base_google.GoogleBaseHook`

   Hook for Google Cloud SQL APIs.

   All the methods in the hook where project_id is used must be called with
   keyword arguments rather than positional.

   
   .. method:: get_conn(self)

      Retrieves connection to Cloud SQL.

      :return: Google Cloud SQL services object.
      :rtype: dict



   
   .. method:: get_instance(self, instance: str, project_id: str)

      Retrieves a resource containing information about a Cloud SQL instance.

      :param instance: Database instance ID. This does not include the project ID.
      :type instance: str
      :param project_id: Project ID of the project that contains the instance. If set
          to None or missing, the default project_id from the Google Cloud connection is used.
      :type project_id: str
      :return: A Cloud SQL instance resource.
      :rtype: dict



   
   .. method:: create_instance(self, body: Dict, project_id: str)

      Creates a new Cloud SQL instance.

      :param body: Body required by the Cloud SQL insert API, as described in
          https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/instances/insert#request-body.
      :type body: dict
      :param project_id: Project ID of the project that contains the instance. If set
          to None or missing, the default project_id from the Google Cloud connection is used.
      :type project_id: str
      :return: None



   
   .. method:: patch_instance(self, body: dict, instance: str, project_id: str)

      Updates settings of a Cloud SQL instance.

      Caution: This is not a partial update, so you must include values for
      all the settings that you want to retain.

      :param body: Body required by the Cloud SQL patch API, as described in
          https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/instances/patch#request-body.
      :type body: dict
      :param instance: Cloud SQL instance ID. This does not include the project ID.
      :type instance: str
      :param project_id: Project ID of the project that contains the instance. If set
          to None or missing, the default project_id from the Google Cloud connection is used.
      :type project_id: str
      :return: None



   
   .. method:: delete_instance(self, instance: str, project_id: str)

      Deletes a Cloud SQL instance.

      :param project_id: Project ID of the project that contains the instance. If set
          to None or missing, the default project_id from the Google Cloud connection is used.
      :type project_id: str
      :param instance: Cloud SQL instance ID. This does not include the project ID.
      :type instance: str
      :return: None



   
   .. method:: get_database(self, instance: str, database: str, project_id: str)

      Retrieves a database resource from a Cloud SQL instance.

      :param instance: Database instance ID. This does not include the project ID.
      :type instance: str
      :param database: Name of the database in the instance.
      :type database: str
      :param project_id: Project ID of the project that contains the instance. If set
          to None or missing, the default project_id from the Google Cloud connection is used.
      :type project_id: str
      :return: A Cloud SQL database resource, as described in
          https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/databases#resource.
      :rtype: dict



   
   .. method:: create_database(self, instance: str, body: Dict, project_id: str)

      Creates a new database inside a Cloud SQL instance.

      :param instance: Database instance ID. This does not include the project ID.
      :type instance: str
      :param body: The request body, as described in
          https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/databases/insert#request-body.
      :type body: dict
      :param project_id: Project ID of the project that contains the instance. If set
          to None or missing, the default project_id from the Google Cloud connection is used.
      :type project_id: str
      :return: None



   
   .. method:: patch_database(self, instance: str, database: str, body: Dict, project_id: str)

      Updates a database resource inside a Cloud SQL instance.

      This method supports patch semantics.
      See https://cloud.google.com/sql/docs/mysql/admin-api/how-tos/performance#patch.

      :param instance: Database instance ID. This does not include the project ID.
      :type instance: str
      :param database: Name of the database to be updated in the instance.
      :type database: str
      :param body: The request body, as described in
          https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/databases/insert#request-body.
      :type body: dict
      :param project_id: Project ID of the project that contains the instance. If set
          to None or missing, the default project_id from the Google Cloud connection is used.
      :type project_id: str
      :return: None



   
   .. method:: delete_database(self, instance: str, database: str, project_id: str)

      Deletes a database from a Cloud SQL instance.

      :param instance: Database instance ID. This does not include the project ID.
      :type instance: str
      :param database: Name of the database to be deleted in the instance.
      :type database: str
      :param project_id: Project ID of the project that contains the instance. If set
          to None or missing, the default project_id from the Google Cloud connection is used.
      :type project_id: str
      :return: None



   
   .. method:: export_instance(self, instance: str, body: Dict, project_id: str)

      Exports data from a Cloud SQL instance to a Cloud Storage bucket as a SQL dump
      or CSV file.

      :param instance: Database instance ID of the Cloud SQL instance. This does not include the
          project ID.
      :type instance: str
      :param body: The request body, as described in
          https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/instances/export#request-body
      :type body: dict
      :param project_id: Project ID of the project that contains the instance. If set
          to None or missing, the default project_id from the Google Cloud connection is used.
      :type project_id: str
      :return: None



   
   .. method:: import_instance(self, instance: str, body: Dict, project_id: str)

      Imports data into a Cloud SQL instance from a SQL dump or CSV file in
      Cloud Storage.

      :param instance: Database instance ID. This does not include the
          project ID.
      :type instance: str
      :param body: The request body, as described in
          https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/instances/export#request-body
      :type body: dict
      :param project_id: Project ID of the project that contains the instance. If set
          to None or missing, the default project_id from the Google Cloud connection is used.
      :type project_id: str
      :return: None



   
   .. method:: _wait_for_operation_to_complete(self, project_id: str, operation_name: str)

      Waits for the named operation to complete - checks status of the
      asynchronous call.

      :param project_id: Project ID of the project that contains the instance.
      :type project_id: str
      :param operation_name: Name of the operation.
      :type operation_name: str
      :return: None




.. data:: CLOUD_SQL_PROXY_DOWNLOAD_URL
   :annotation: = https://dl.google.com/cloudsql/cloud_sql_proxy.{}.{}

   

.. data:: CLOUD_SQL_PROXY_VERSION_DOWNLOAD_URL
   :annotation: = https://storage.googleapis.com/cloudsql-proxy/{}/cloud_sql_proxy.{}.{}

   

.. data:: GCP_CREDENTIALS_KEY_PATH
   :annotation: = extra__google_cloud_platform__key_path

   

.. data:: GCP_CREDENTIALS_KEYFILE_DICT
   :annotation: = extra__google_cloud_platform__keyfile_dict

   

.. py:class:: CloudSqlProxyRunner(path_prefix: str, instance_specification: str, gcp_conn_id: str = 'google_cloud_default', project_id: Optional[str] = None, sql_proxy_version: Optional[str] = None, sql_proxy_binary_path: Optional[str] = None)

   Bases: :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   Downloads and runs cloud-sql-proxy as subprocess of the Python process.

   The cloud-sql-proxy needs to be downloaded and started before we can connect
   to the Google Cloud SQL instance via database connection. It establishes
   secure tunnel connection to the database. It authorizes using the
   Google Cloud credentials that are passed by the configuration.

   More details about the proxy can be found here:
   https://cloud.google.com/sql/docs/mysql/sql-proxy

   :param path_prefix: Unique path prefix where proxy will be downloaded and
       directories created for unix sockets.
   :type path_prefix: str
   :param instance_specification: Specification of the instance to connect the
       proxy to. It should be specified in the form that is described in
       https://cloud.google.com/sql/docs/mysql/sql-proxy#multiple-instances in
       -instances parameter (typically in the form of ``<project>:<region>:<instance>``
       for UNIX socket connections and in the form of
       ``<project>:<region>:<instance>=tcp:<port>`` for TCP connections.
   :type instance_specification: str
   :param gcp_conn_id: Id of Google Cloud connection to use for
       authentication
   :type gcp_conn_id: str
   :param project_id: Optional id of the Google Cloud project to connect to - it overwrites
       default project id taken from the Google Cloud connection.
   :type project_id: str
   :param sql_proxy_version: Specific version of SQL proxy to download
       (for example 'v1.13'). By default latest version is downloaded.
   :type sql_proxy_version: str
   :param sql_proxy_binary_path: If specified, then proxy will be
       used from the path specified rather than dynamically generated. This means
       that if the binary is not present in that path it will also be downloaded.
   :type sql_proxy_binary_path: str

   
   .. method:: _build_command_line_parameters(self)



   
   .. staticmethod:: _is_os_64bit()



   
   .. method:: _download_sql_proxy_if_needed(self)



   
   .. method:: _get_credential_parameters(self, session: Session)



   
   .. method:: start_proxy(self)

      Starts Cloud SQL Proxy.

      You have to remember to stop the proxy if you started it!



   
   .. method:: stop_proxy(self)

      Stops running proxy.

      You should stop the proxy after you stop using it.



   
   .. method:: get_proxy_version(self)

      Returns version of the Cloud SQL Proxy.



   
   .. method:: get_socket_path(self)

      Retrieves UNIX socket path used by Cloud SQL Proxy.

      :return: The dynamically generated path for the socket created by the proxy.
      :rtype: str




.. data:: CONNECTION_URIS
   :annotation: :Dict[str, Dict[str, Dict[str, str]]]

   

.. data:: CLOUD_SQL_VALID_DATABASE_TYPES
   :annotation: = ['postgres', 'mysql']

   

.. py:class:: CloudSQLDatabaseHook(gcp_cloudsql_conn_id: str = 'google_cloud_sql_default', gcp_conn_id: str = 'google_cloud_default', default_gcp_project_id: Optional[str] = None)

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   Serves DB connection configuration for Google Cloud SQL (Connections
   of *gcpcloudsql://* type).

   The hook is a "meta" one. It does not perform an actual connection.
   It is there to retrieve all the parameters configured in gcpcloudsql:// connection,
   start/stop Cloud SQL Proxy if needed, dynamically generate Postgres or MySQL
   connection in the database and return an actual Postgres or MySQL hook.
   The returned Postgres/MySQL hooks are using direct connection or Cloud SQL
   Proxy socket/TCP as configured.

   Main parameters of the hook are retrieved from the standard URI components:

   * **user** - User name to authenticate to the database (from login of the URI).
   * **password** - Password to authenticate to the database (from password of the URI).
   * **public_ip** - IP to connect to for public connection (from host of the URI).
   * **public_port** - Port to connect to for public connection (from port of the URI).
   * **database** - Database to connect to (from schema of the URI).

   Remaining parameters are retrieved from the extras (URI query parameters):

   * **project_id** - Optional, Google Cloud project where the Cloud SQL
      instance exists. If missing, default project id passed is used.
   * **instance** -  Name of the instance of the Cloud SQL database instance.
   * **location** - The location of the Cloud SQL instance (for example europe-west1).
   * **database_type** - The type of the database instance (MySQL or Postgres).
   * **use_proxy** - (default False) Whether SQL proxy should be used to connect to Cloud
     SQL DB.
   * **use_ssl** - (default False) Whether SSL should be used to connect to Cloud SQL DB.
     You cannot use proxy and SSL together.
   * **sql_proxy_use_tcp** - (default False) If set to true, TCP is used to connect via
     proxy, otherwise UNIX sockets are used.
   * **sql_proxy_binary_path** - Optional path to Cloud SQL Proxy binary. If the binary
     is not specified or the binary is not present, it is automatically downloaded.
   * **sql_proxy_version** -  Specific version of the proxy to download (for example
     v1.13). If not specified, the latest version is downloaded.
   * **sslcert** - Path to client certificate to authenticate when SSL is used.
   * **sslkey** - Path to client private key to authenticate when SSL is used.
   * **sslrootcert** - Path to server's certificate to authenticate when SSL is used.

   :param gcp_cloudsql_conn_id: URL of the connection
   :type gcp_cloudsql_conn_id: str
   :param gcp_conn_id: The connection ID used to connect to Google Cloud for
       cloud-sql-proxy authentication.
   :type gcp_conn_id: str
   :param default_gcp_project_id: Default project id used if project_id not specified
          in the connection URL
   :type default_gcp_project_id: str

   .. attribute:: _conn
      :annotation: :Optional[Any]

      

   
   .. staticmethod:: _get_bool(val: Any)



   
   .. staticmethod:: _check_ssl_file(file_to_check, name)



   
   .. method:: _validate_inputs(self)



   
   .. method:: validate_ssl_certs(self)

      SSL certificates validator.

      :return: None



   
   .. method:: validate_socket_path_length(self)

      Validates sockets path length.

      :return: None or rises AirflowException



   
   .. staticmethod:: _generate_unique_path()

      We are not using mkdtemp here as the path generated with mkdtemp
      can be close to 60 characters and there is a limitation in
      length of socket path to around 100 characters in total.
      We append project/location/instance to it later and postgres
      appends its own prefix, so we chose a shorter "/tmp/[8 random characters]"



   
   .. staticmethod:: _quote(value)



   
   .. method:: _generate_connection_uri(self)



   
   .. method:: _get_instance_socket_name(self)



   
   .. method:: _get_sqlproxy_instance_specification(self)



   
   .. method:: create_connection(self)

      Create Connection object, according to whether it uses proxy, TCP, UNIX sockets, SSL.
      Connection ID will be randomly generated.



   
   .. method:: get_sqlproxy_runner(self)

      Retrieve Cloud SQL Proxy runner. It is used to manage the proxy
      lifecycle per task.

      :return: The Cloud SQL Proxy runner.
      :rtype: CloudSqlProxyRunner



   
   .. method:: get_database_hook(self, connection: Connection)

      Retrieve database hook. This is the actual Postgres or MySQL database hook
      that uses proxy or connects directly to the Google Cloud SQL database.



   
   .. method:: cleanup_database_hook(self)

      Clean up database hook after it was used.



   
   .. method:: reserve_free_tcp_port(self)

      Reserve free TCP port to be used by Cloud SQL Proxy



   
   .. method:: free_reserved_port(self)

      Free TCP port. Makes it immediately ready to be used by Cloud SQL Proxy.




