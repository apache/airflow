:mod:`airflow.providers.google.cloud.operators.cloud_sql`
=========================================================

.. py:module:: airflow.providers.google.cloud.operators.cloud_sql

.. autoapi-nested-parse::

   This module contains Google Cloud SQL operators.



Module Contents
---------------

.. data:: SETTINGS
   :annotation: = settings

   

.. data:: SETTINGS_VERSION
   :annotation: = settingsVersion

   

.. data:: CLOUD_SQL_CREATE_VALIDATION
   

   

.. data:: CLOUD_SQL_EXPORT_VALIDATION
   

   

.. data:: CLOUD_SQL_IMPORT_VALIDATION
   

   

.. data:: CLOUD_SQL_DATABASE_CREATE_VALIDATION
   

   

.. data:: CLOUD_SQL_DATABASE_PATCH_VALIDATION
   

   

.. py:class:: CloudSQLBaseOperator(*, instance: str, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', api_version: str = 'v1beta4', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Abstract base operator for Google Cloud SQL operators to inherit from.

   :param instance: Cloud SQL instance ID. This does not include the project ID.
   :type instance: str
   :param project_id: Optional, Google Cloud Project ID.  f set to None or missing,
           the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param gcp_conn_id: The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param api_version: API version used (e.g. v1beta4).
   :type api_version: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   
   .. method:: _validate_inputs(self)



   
   .. method:: _check_if_instance_exists(self, instance, hook: CloudSQLHook)



   
   .. method:: _check_if_db_exists(self, db_name, hook: CloudSQLHook)



   
   .. method:: execute(self, context)



   
   .. staticmethod:: _get_settings_version(instance)




.. py:class:: CloudSQLCreateInstanceOperator(*, body: dict, instance: str, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', api_version: str = 'v1beta4', validate_body: bool = True, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.providers.google.cloud.operators.cloud_sql.CloudSQLBaseOperator`

   Creates a new Cloud SQL instance.
   If an instance with the same name exists, no action will be taken and
   the operator will succeed.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudSQLCreateInstanceOperator`

   :param body: Body required by the Cloud SQL insert API, as described in
       https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/instances/insert
       #request-body
   :type body: dict
   :param instance: Cloud SQL instance ID. This does not include the project ID.
   :type instance: str
   :param project_id: Optional, Google Cloud Project ID. If set to None or missing,
           the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param gcp_conn_id: The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param api_version: API version used (e.g. v1beta4).
   :type api_version: str
   :param validate_body: True if body should be validated, False otherwise.
   :type validate_body: bool
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['project_id', 'instance', 'body', 'gcp_conn_id', 'api_version', 'impersonation_chain']

      

   
   .. method:: _validate_inputs(self)



   
   .. method:: _validate_body_fields(self)



   
   .. method:: execute(self, context)




.. py:class:: CloudSQLInstancePatchOperator(*, body: dict, instance: str, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', api_version: str = 'v1beta4', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.providers.google.cloud.operators.cloud_sql.CloudSQLBaseOperator`

   Updates settings of a Cloud SQL instance.

   Caution: This is a partial update, so only included values for the settings will be
   updated.

   In the request body, supply the relevant portions of an instance resource, according
   to the rules of patch semantics.
   https://cloud.google.com/sql/docs/mysql/admin-api/how-tos/performance#patch

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudSQLInstancePatchOperator`

   :param body: Body required by the Cloud SQL patch API, as described in
       https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/instances/patch#request-body
   :type body: dict
   :param instance: Cloud SQL instance ID. This does not include the project ID.
   :type instance: str
   :param project_id: Optional, Google Cloud Project ID.  If set to None or missing,
           the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param gcp_conn_id: The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param api_version: API version used (e.g. v1beta4).
   :type api_version: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['project_id', 'instance', 'body', 'gcp_conn_id', 'api_version', 'impersonation_chain']

      

   
   .. method:: _validate_inputs(self)



   
   .. method:: execute(self, context)




.. py:class:: CloudSQLDeleteInstanceOperator(*, instance: str, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', api_version: str = 'v1beta4', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.providers.google.cloud.operators.cloud_sql.CloudSQLBaseOperator`

   Deletes a Cloud SQL instance.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudSQLDeleteInstanceOperator`

   :param instance: Cloud SQL instance ID. This does not include the project ID.
   :type instance: str
   :param project_id: Optional, Google Cloud Project ID. If set to None or missing,
           the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param gcp_conn_id: The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param api_version: API version used (e.g. v1beta4).
   :type api_version: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['project_id', 'instance', 'gcp_conn_id', 'api_version', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudSQLCreateInstanceDatabaseOperator(*, instance: str, body: dict, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', api_version: str = 'v1beta4', validate_body: bool = True, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.providers.google.cloud.operators.cloud_sql.CloudSQLBaseOperator`

   Creates a new database inside a Cloud SQL instance.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudSQLCreateInstanceDatabaseOperator`

   :param instance: Database instance ID. This does not include the project ID.
   :type instance: str
   :param body: The request body, as described in
       https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/databases/insert#request-body
   :type body: dict
   :param project_id: Optional, Google Cloud Project ID. If set to None or missing,
           the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param gcp_conn_id: The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param api_version: API version used (e.g. v1beta4).
   :type api_version: str
   :param validate_body: Whether the body should be validated. Defaults to True.
   :type validate_body: bool
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['project_id', 'instance', 'body', 'gcp_conn_id', 'api_version', 'impersonation_chain']

      

   
   .. method:: _validate_inputs(self)



   
   .. method:: _validate_body_fields(self)



   
   .. method:: execute(self, context)




.. py:class:: CloudSQLPatchInstanceDatabaseOperator(*, instance: str, database: str, body: dict, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', api_version: str = 'v1beta4', validate_body: bool = True, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.providers.google.cloud.operators.cloud_sql.CloudSQLBaseOperator`

   Updates a resource containing information about a database inside a Cloud SQL
   instance using patch semantics.
   See: https://cloud.google.com/sql/docs/mysql/admin-api/how-tos/performance#patch

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudSQLPatchInstanceDatabaseOperator`

   :param instance: Database instance ID. This does not include the project ID.
   :type instance: str
   :param database: Name of the database to be updated in the instance.
   :type database: str
   :param body: The request body, as described in
       https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/databases/patch#request-body
   :type body: dict
   :param project_id: Optional, Google Cloud Project ID.
   :type project_id: str
   :param gcp_conn_id: The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param api_version: API version used (e.g. v1beta4).
   :type api_version: str
   :param validate_body: Whether the body should be validated. Defaults to True.
   :type validate_body: bool
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['project_id', 'instance', 'body', 'database', 'gcp_conn_id', 'api_version', 'impersonation_chain']

      

   
   .. method:: _validate_inputs(self)



   
   .. method:: _validate_body_fields(self)



   
   .. method:: execute(self, context)




.. py:class:: CloudSQLDeleteInstanceDatabaseOperator(*, instance: str, database: str, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', api_version: str = 'v1beta4', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.providers.google.cloud.operators.cloud_sql.CloudSQLBaseOperator`

   Deletes a database from a Cloud SQL instance.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudSQLDeleteInstanceDatabaseOperator`

   :param instance: Database instance ID. This does not include the project ID.
   :type instance: str
   :param database: Name of the database to be deleted in the instance.
   :type database: str
   :param project_id: Optional, Google Cloud Project ID. If set to None or missing,
           the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param gcp_conn_id: The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param api_version: API version used (e.g. v1beta4).
   :type api_version: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['project_id', 'instance', 'database', 'gcp_conn_id', 'api_version', 'impersonation_chain']

      

   
   .. method:: _validate_inputs(self)



   
   .. method:: execute(self, context)




.. py:class:: CloudSQLExportInstanceOperator(*, instance: str, body: dict, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', api_version: str = 'v1beta4', validate_body: bool = True, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.providers.google.cloud.operators.cloud_sql.CloudSQLBaseOperator`

   Exports data from a Cloud SQL instance to a Cloud Storage bucket as a SQL dump
   or CSV file.

   Note: This operator is idempotent. If executed multiple times with the same
   export file URI, the export file in GCS will simply be overridden.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudSQLExportInstanceOperator`

   :param instance: Cloud SQL instance ID. This does not include the project ID.
   :type instance: str
   :param body: The request body, as described in
       https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/instances/export#request-body
   :type body: dict
   :param project_id: Optional, Google Cloud Project ID. If set to None or missing,
           the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param gcp_conn_id: The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param api_version: API version used (e.g. v1beta4).
   :type api_version: str
   :param validate_body: Whether the body should be validated. Defaults to True.
   :type validate_body: bool
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['project_id', 'instance', 'body', 'gcp_conn_id', 'api_version', 'impersonation_chain']

      

   
   .. method:: _validate_inputs(self)



   
   .. method:: _validate_body_fields(self)



   
   .. method:: execute(self, context)




.. py:class:: CloudSQLImportInstanceOperator(*, instance: str, body: dict, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', api_version: str = 'v1beta4', validate_body: bool = True, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.providers.google.cloud.operators.cloud_sql.CloudSQLBaseOperator`

   Imports data into a Cloud SQL instance from a SQL dump or CSV file in Cloud Storage.

   CSV IMPORT:

   This operator is NOT idempotent for a CSV import. If the same file is imported
   multiple times, the imported data will be duplicated in the database.
   Moreover, if there are any unique constraints the duplicate import may result in an
   error.

   SQL IMPORT:

   This operator is idempotent for a SQL import if it was also exported by Cloud SQL.
   The exported SQL contains 'DROP TABLE IF EXISTS' statements for all tables
   to be imported.

   If the import file was generated in a different way, idempotence is not guaranteed.
   It has to be ensured on the SQL file level.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudSQLImportInstanceOperator`

   :param instance: Cloud SQL instance ID. This does not include the project ID.
   :type instance: str
   :param body: The request body, as described in
       https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/instances/export#request-body
   :type body: dict
   :param project_id: Optional, Google Cloud Project ID. If set to None or missing,
           the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param gcp_conn_id: The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param api_version: API version used (e.g. v1beta4).
   :type api_version: str
   :param validate_body: Whether the body should be validated. Defaults to True.
   :type validate_body: bool
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['project_id', 'instance', 'body', 'gcp_conn_id', 'api_version', 'impersonation_chain']

      

   
   .. method:: _validate_inputs(self)



   
   .. method:: _validate_body_fields(self)



   
   .. method:: execute(self, context)




.. py:class:: CloudSQLExecuteQueryOperator(*, sql: Union[List[str], str], autocommit: bool = False, parameters: Optional[Union[Dict, Iterable]] = None, gcp_conn_id: str = 'google_cloud_default', gcp_cloudsql_conn_id: str = 'google_cloud_sql_default', **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Performs DML or DDL query on an existing Cloud Sql instance. It optionally uses
   cloud-sql-proxy to establish secure connection with the database.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudSQLExecuteQueryOperator`

   :param sql: SQL query or list of queries to run (should be DML or DDL query -
       this operator does not return any data from the database,
       so it is useless to pass it DQL queries. Note that it is responsibility of the
       author of the queries to make sure that the queries are idempotent. For example
       you can use CREATE TABLE IF NOT EXISTS to create a table.
   :type sql: str or list[str]
   :param parameters: (optional) the parameters to render the SQL query with.
   :type parameters: dict or iterable
   :param autocommit: if True, each command is automatically committed.
       (default value: False)
   :type autocommit: bool
   :param gcp_conn_id: The connection ID used to connect to Google Cloud for
       cloud-sql-proxy authentication.
   :type gcp_conn_id: str
   :param gcp_cloudsql_conn_id: The connection ID used to connect to Google Cloud SQL
      its schema should be gcpcloudsql://.
      See :class:`~airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook` for
      details on how to define gcpcloudsql:// connection.
   :type gcp_cloudsql_conn_id: str

   .. attribute:: template_fields
      :annotation: = ['sql', 'gcp_cloudsql_conn_id', 'gcp_conn_id']

      

   .. attribute:: template_ext
      :annotation: = ['.sql']

      

   
   .. method:: _execute_query(self, hook: CloudSQLDatabaseHook, database_hook: Union[PostgresHook, MySqlHook])



   
   .. method:: execute(self, context)




