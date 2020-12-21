:mod:`airflow.providers.google.cloud.operators.spanner`
=======================================================

.. py:module:: airflow.providers.google.cloud.operators.spanner

.. autoapi-nested-parse::

   This module contains Google Spanner operators.



Module Contents
---------------

.. py:class:: SpannerDeployInstanceOperator(*, instance_id: str, configuration_name: str, node_count: int, display_name: str, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Creates a new Cloud Spanner instance, or if an instance with the same instance_id
   exists in the specified project, updates the Cloud Spanner instance.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:SpannerDeployInstanceOperator`

   :param instance_id: Cloud Spanner instance ID.
   :type instance_id: str
   :param configuration_name:  The name of the Cloud Spanner instance configuration
     defining how the instance will be created. Required for
     instances that do not yet exist.
   :type configuration_name: str
   :param node_count: (Optional) The number of nodes allocated to the Cloud Spanner
     instance.
   :type node_count: int
   :param display_name: (Optional) The display name for the Cloud Spanner  instance in
     the Google Cloud Console. (Must be between 4 and 30 characters.) If this value is not set
     in the constructor, the name is the same as the instance ID.
   :type display_name: str
   :param project_id: Optional, the ID of the project which owns the Cloud Spanner
       Database.  If set to None or missing, the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param gcp_conn_id: The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
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
      :annotation: = ['project_id', 'instance_id', 'configuration_name', 'display_name', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: _validate_inputs(self)



   
   .. method:: execute(self, context)




.. py:class:: SpannerDeleteInstanceOperator(*, instance_id: str, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Deletes a Cloud Spanner instance. If an instance does not exist,
   no action is taken and the operator succeeds.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:SpannerDeleteInstanceOperator`

   :param instance_id: The Cloud Spanner instance ID.
   :type instance_id: str
   :param project_id: Optional, the ID of the project that owns the Cloud Spanner
       Database.  If set to None or missing, the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param gcp_conn_id: The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
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
      :annotation: = ['project_id', 'instance_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: _validate_inputs(self)



   
   .. method:: execute(self, context)




.. py:class:: SpannerQueryDatabaseInstanceOperator(*, instance_id: str, database_id: str, query: Union[str, List[str]], project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Executes an arbitrary DML query (INSERT, UPDATE, DELETE).

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:SpannerQueryDatabaseInstanceOperator`

   :param instance_id: The Cloud Spanner instance ID.
   :type instance_id: str
   :param database_id: The Cloud Spanner database ID.
   :type database_id: str
   :param query: The query or list of queries to be executed. Can be a path to a SQL
      file.
   :type query: str or list
   :param project_id: Optional, the ID of the project that owns the Cloud Spanner
       Database.  If set to None or missing, the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param gcp_conn_id: The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
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
      :annotation: = ['project_id', 'instance_id', 'database_id', 'query', 'gcp_conn_id', 'impersonation_chain']

      

   .. attribute:: template_ext
      :annotation: = ['.sql']

      

   
   .. method:: _validate_inputs(self)



   
   .. method:: execute(self, context)



   
   .. staticmethod:: sanitize_queries(queries: List[str])

      Drops empty query in queries.

      :param queries: queries
      :type queries: List[str]
      :rtype: None




.. py:class:: SpannerDeployDatabaseInstanceOperator(*, instance_id: str, database_id: str, ddl_statements: List[str], project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Creates a new Cloud Spanner database, or if database exists,
   the operator does nothing.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:SpannerDeployDatabaseInstanceOperator`

   :param instance_id: The Cloud Spanner instance ID.
   :type instance_id: str
   :param database_id: The Cloud Spanner database ID.
   :type database_id: str
   :param ddl_statements: The string list containing DDL for the new database.
   :type ddl_statements: list[str]
   :param project_id: Optional, the ID of the project that owns the Cloud Spanner
       Database.  If set to None or missing, the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param gcp_conn_id: The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
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
      :annotation: = ['project_id', 'instance_id', 'database_id', 'ddl_statements', 'gcp_conn_id', 'impersonation_chain']

      

   .. attribute:: template_ext
      :annotation: = ['.sql']

      

   
   .. method:: _validate_inputs(self)



   
   .. method:: execute(self, context)




.. py:class:: SpannerUpdateDatabaseInstanceOperator(*, instance_id: str, database_id: str, ddl_statements: List[str], project_id: Optional[str] = None, operation_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Updates a Cloud Spanner database with the specified DDL statement.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:SpannerUpdateDatabaseInstanceOperator`

   :param instance_id: The Cloud Spanner instance ID.
   :type instance_id: str
   :param database_id: The Cloud Spanner database ID.
   :type database_id: str
   :param ddl_statements: The string list containing DDL to apply to the database.
   :type ddl_statements: list[str]
   :param project_id: Optional, the ID of the project that owns the Cloud Spanner
       Database.  If set to None or missing, the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param operation_id: (Optional) Unique per database operation id that can
          be specified to implement idempotency check.
   :type operation_id: str
   :param gcp_conn_id: The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
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
      :annotation: = ['project_id', 'instance_id', 'database_id', 'ddl_statements', 'gcp_conn_id', 'impersonation_chain']

      

   .. attribute:: template_ext
      :annotation: = ['.sql']

      

   
   .. method:: _validate_inputs(self)



   
   .. method:: execute(self, context)




.. py:class:: SpannerDeleteDatabaseInstanceOperator(*, instance_id: str, database_id: str, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Deletes a Cloud Spanner database.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:SpannerDeleteDatabaseInstanceOperator`

   :param instance_id: Cloud Spanner instance ID.
   :type instance_id: str
   :param database_id: Cloud Spanner database ID.
   :type database_id: str
   :param project_id: Optional, the ID of the project that owns the Cloud Spanner
       Database.  If set to None or missing, the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param gcp_conn_id: The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
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
      :annotation: = ['project_id', 'instance_id', 'database_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: _validate_inputs(self)



   
   .. method:: execute(self, context)




