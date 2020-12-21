:mod:`airflow.providers.google.cloud.hooks.spanner`
===================================================

.. py:module:: airflow.providers.google.cloud.hooks.spanner

.. autoapi-nested-parse::

   This module contains a Google Cloud Spanner Hook.



Module Contents
---------------

.. py:class:: SpannerHook(gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None)

   Bases: :class:`airflow.providers.google.common.hooks.base_google.GoogleBaseHook`

   Hook for Google Cloud Spanner APIs.

   All the methods in the hook where project_id is used must be called with
   keyword arguments rather than positional.

   
   .. method:: _get_client(self, project_id: str)

      Provides a client for interacting with the Cloud Spanner API.

      :param project_id: The ID of the Google Cloud project.
      :type project_id: str
      :return: Client
      :rtype: google.cloud.spanner_v1.client.Client



   
   .. method:: get_instance(self, instance_id: str, project_id: str)

      Gets information about a particular instance.

      :param project_id: Optional, The ID of the Google Cloud project that owns the Cloud Spanner
          database. If set to None or missing, the default project_id from the Google Cloud connection
          is used.
      :type project_id: str
      :param instance_id: The ID of the Cloud Spanner instance.
      :type instance_id: str
      :return: Spanner instance
      :rtype: google.cloud.spanner_v1.instance.Instance



   
   .. method:: _apply_to_instance(self, project_id: str, instance_id: str, configuration_name: str, node_count: int, display_name: str, func: Callable[[Instance], Operation])

      Invokes a method on a given instance by applying a specified Callable.

      :param project_id: The ID of the Google Cloud project that owns the Cloud Spanner database.
      :type project_id: str
      :param instance_id: The ID of the instance.
      :type instance_id: str
      :param configuration_name: Name of the instance configuration defining how the
          instance will be created. Required for instances which do not yet exist.
      :type configuration_name: str
      :param node_count: (Optional) Number of nodes allocated to the instance.
      :type node_count: int
      :param display_name: (Optional) The display name for the instance in the Cloud
          Console UI. (Must be between 4 and 30 characters.) If this value is not set
          in the constructor, will fall back to the instance ID.
      :type display_name: str
      :param func: Method of the instance to be called.
      :type func: Callable[google.cloud.spanner_v1.instance.Instance]



   
   .. method:: create_instance(self, instance_id: str, configuration_name: str, node_count: int, display_name: str, project_id: str)

      Creates a new Cloud Spanner instance.

      :param instance_id: The ID of the Cloud Spanner instance.
      :type instance_id: str
      :param configuration_name: The name of the instance configuration defining how the
          instance will be created. Possible configuration values can be retrieved via
          https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instanceConfigs/list
      :type configuration_name: str
      :param node_count: (Optional) The number of nodes allocated to the Cloud Spanner
          instance.
      :type node_count: int
      :param display_name: (Optional) The display name for the instance in the Google Cloud Console.
          Must be between 4 and 30 characters. If this value is not passed, the name falls back
          to the instance ID.
      :type display_name: str
      :param project_id: Optional, the ID of the Google Cloud project that owns the Cloud Spanner
          database. If set to None or missing, the default project_id from the Google Cloud connection
          is used.
      :type project_id: str
      :return: None



   
   .. method:: update_instance(self, instance_id: str, configuration_name: str, node_count: int, display_name: str, project_id: str)

      Updates an existing Cloud Spanner instance.

      :param instance_id: The ID of the Cloud Spanner instance.
      :type instance_id: str
      :param configuration_name: The name of the instance configuration defining how the
          instance will be created. Possible configuration values can be retrieved via
          https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instanceConfigs/list
      :type configuration_name: str
      :param node_count: (Optional) The number of nodes allocated to the Cloud Spanner
          instance.
      :type node_count: int
      :param display_name: (Optional) The display name for the instance in the Google Cloud
          Console. Must be between 4 and 30 characters. If this value is not set in
          the constructor, the name falls back to the instance ID.
      :type display_name: str
      :param project_id: Optional, the ID of the Google Cloud project that owns the Cloud Spanner
          database. If set to None or missing, the default project_id from the Google Cloud connection
          is used.
      :type project_id: str
      :return: None



   
   .. method:: delete_instance(self, instance_id: str, project_id: str)

      Deletes an existing Cloud Spanner instance.

      :param instance_id: The ID of the Cloud Spanner instance.
      :type instance_id: str
      :param project_id: Optional, the ID of the Google Cloud project that owns the Cloud Spanner
          database. If set to None or missing, the default project_id from the Google Cloud connection
          is used.
      :type project_id: str
      :return: None



   
   .. method:: get_database(self, instance_id: str, database_id: str, project_id: str)

      Retrieves a database in Cloud Spanner. If the database does not exist
      in the specified instance, it returns None.

      :param instance_id: The ID of the Cloud Spanner instance.
      :type instance_id: str
      :param database_id: The ID of the database in Cloud Spanner.
      :type database_id: str
      :param project_id: Optional, the ID of the Google Cloud project that owns the Cloud Spanner
          database. If set to None or missing, the default project_id from the Google Cloud connection
          is used.
      :type project_id: str
      :return: Database object or None if database does not exist
      :rtype: google.cloud.spanner_v1.database.Database or None



   
   .. method:: create_database(self, instance_id: str, database_id: str, ddl_statements: List[str], project_id: str)

      Creates a new database in Cloud Spanner.

      :type project_id: str
      :param instance_id: The ID of the Cloud Spanner instance.
      :type instance_id: str
      :param database_id: The ID of the database to create in Cloud Spanner.
      :type database_id: str
      :param ddl_statements: The string list containing DDL for the new database.
      :type ddl_statements: list[str]
      :param project_id: Optional, the ID of the Google Cloud project that owns the Cloud Spanner
          database. If set to None or missing, the default project_id from the Google Cloud connection
          is used.
      :return: None



   
   .. method:: update_database(self, instance_id: str, database_id: str, ddl_statements: List[str], project_id: str, operation_id: Optional[str] = None)

      Updates DDL of a database in Cloud Spanner.

      :type project_id: str
      :param instance_id: The ID of the Cloud Spanner instance.
      :type instance_id: str
      :param database_id: The ID of the database in Cloud Spanner.
      :type database_id: str
      :param ddl_statements: The string list containing DDL for the new database.
      :type ddl_statements: list[str]
      :param project_id: Optional, the ID of the Google Cloud project that owns the Cloud Spanner
          database. If set to None or missing, the default project_id from the Google Cloud connection
          is used.
      :param operation_id: (Optional) The unique per database operation ID that can be
          specified to implement idempotency check.
      :type operation_id: str
      :return: None



   
   .. method:: delete_database(self, instance_id: str, database_id, project_id: str)

      Drops a database in Cloud Spanner.

      :type project_id: str
      :param instance_id: The ID of the Cloud Spanner instance.
      :type instance_id: str
      :param database_id: The ID of the database in Cloud Spanner.
      :type database_id: str
      :param project_id: Optional, the ID of the Google Cloud project that owns the Cloud Spanner
          database. If set to None or missing, the default project_id from the Google Cloud connection
          is used.
      :return: True if everything succeeded
      :rtype: bool



   
   .. method:: execute_dml(self, instance_id: str, database_id: str, queries: List[str], project_id: str)

      Executes an arbitrary DML query (INSERT, UPDATE, DELETE).

      :param instance_id: The ID of the Cloud Spanner instance.
      :type instance_id: str
      :param database_id: The ID of the database in Cloud Spanner.
      :type database_id: str
      :param queries: The queries to execute.
      :type queries: List[str]
      :param project_id: Optional, the ID of the Google Cloud project that owns the Cloud Spanner
          database. If set to None or missing, the default project_id from the Google Cloud connection
          is used.
      :type project_id: str



   
   .. staticmethod:: _execute_sql_in_transaction(transaction: Transaction, queries: List[str])




