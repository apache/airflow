# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from google.api_core.exceptions import GoogleAPICallError, AlreadyExists
from google.cloud.spanner_v1.client import Client
from google.longrunning.operations_grpc_pb2 import Operation  # noqa: F401

from airflow import AirflowException
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook


class CloudSpannerHook(GoogleCloudBaseHook):
    """
    Hook for Google Cloud Spanner APIs.
    """
    _client = None

    def __init__(self,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None):
        super(CloudSpannerHook, self).__init__(gcp_conn_id, delegate_to)

    def _get_client(self, project_id):
        """
        Provides a client for interacting with the Cloud Spanner API.

        :param project_id: The ID of the  GCP project.
        :type project_id: str
        :return: Client for interacting with the Cloud Spanner API. See:
            https://googleapis.github.io/google-cloud-python/latest/spanner/client-api.html#google.cloud.spanner_v1.client.Client
        :rtype: object
        """
        if not self._client:
            self._client = Client(project=project_id, credentials=self._get_credentials())
        return self._client

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def get_instance(self, instance_id, project_id=None):
        """
        Gets information about a particular instance.

        :param project_id: Optional, The ID of the  GCP project that owns the Cloud Spanner
            database.  If set to None or missing, the default project_id from the GCP connection is used.
        :type project_id: str
        :param instance_id: The ID of the Cloud Spanner instance.
        :type instance_id: str
        :return: Representation of a Cloud Spanner Instance. See:
            https://googleapis.github.io/google-cloud-python/latest/spanner/instance-api.html#google.cloud.spanner_v1.instance.Instance
        :rtype: object
        """
        instance = self._get_client(project_id=project_id).instance(instance_id=instance_id)
        if not instance.exists():
            return None
        return instance

    def _apply_to_instance(self, project_id, instance_id, configuration_name, node_count,
                           display_name, func):
        """
        Invokes a method on a given instance by applying a specified Callable.

        :param project_id: The ID of the  GCP project that owns the Cloud Spanner
            database.
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
        :type func: Callable
        """
        # noinspection PyUnresolvedReferences
        instance = self._get_client(project_id=project_id).instance(
            instance_id=instance_id, configuration_name=configuration_name,
            node_count=node_count, display_name=display_name)
        try:
            operation = func(instance)  # type: Operation
        except GoogleAPICallError as e:
            self.log.error('An error occurred: %s. Exiting.', e.message)
            raise e

        if operation:
            result = operation.result()
            self.log.info(result)

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def create_instance(self, instance_id, configuration_name, node_count,
                        display_name, project_id=None):
        """
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
        :param display_name: (Optional) The display name for the instance in the GCP
            Console. Must be between 4 and 30 characters.  If this value is not set in
            the constructor, the name falls back to the instance ID.
        :type display_name: str
        :param project_id: Optional, the ID of the  GCP project that owns the Cloud Spanner
            database. If set to None or missing, the default project_id from the GCP connection is used.
        :type project_id: str
        :return: None
        """
        self._apply_to_instance(project_id, instance_id, configuration_name,
                                node_count, display_name, lambda x: x.create())

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def update_instance(self, instance_id, configuration_name, node_count,
                        display_name, project_id=None):
        """
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
        :param display_name: (Optional) The display name for the instance in the GCP
            Console. Must be between 4 and 30 characters. If this value is not set in
            the constructor, the name falls back to the instance ID.
        :type display_name: str
        :param project_id: Optional, the ID of the  GCP project that owns the Cloud Spanner
            database. If set to None or missing, the default project_id from the GCP connection is used.
        :type project_id: str
        :return: None
        """
        return self._apply_to_instance(project_id, instance_id, configuration_name,
                                       node_count, display_name, lambda x: x.update())

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def delete_instance(self, instance_id, project_id=None):
        """
        Deletes an existing Cloud Spanner instance.

        :param instance_id:  The ID of the Cloud Spanner instance.
        :type instance_id: str
        :param project_id: Optional, the ID of the  GCP project that owns the Cloud Spanner
            database. If set to None or missing, the default project_id from the GCP connection is used.
        :type project_id: str
        :return None
        """
        instance = self._get_client(project_id=project_id).instance(instance_id)
        try:
            instance.delete()
            return
        except GoogleAPICallError as e:
            self.log.error('An error occurred: %s. Exiting.', e.message)
            raise e

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def get_database(self, instance_id, database_id, project_id=None):
        """
        Retrieves a database in Cloud Spanner. If the database does not exist
        in the specified instance, it returns None.

        :param instance_id: The ID of the Cloud Spanner instance.
        :type instance_id: str
        :param database_id: The ID of the database in Cloud Spanner.
        :type database_id: str
        :param project_id: Optional, the ID of the  GCP project that owns the Cloud Spanner
            database. If set to None or missing, the default project_id from the GCP connection is used.
        :type project_id: str
        :return: Database object or None if database does not exist
        :rtype: Union[Database, None]
        """

        instance = self._get_client(project_id=project_id).instance(
            instance_id=instance_id)
        if not instance.exists():
            raise AirflowException("The instance {} does not exist in project {} !".
                                   format(instance_id, project_id))
        database = instance.database(database_id=database_id)
        if not database.exists():
            return None
        else:
            return database

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def create_database(self, instance_id, database_id, ddl_statements, project_id=None):
        """
        Creates a new database in Cloud Spanner.

        :type project_id: str
        :param instance_id: The ID of the Cloud Spanner instance.
        :type instance_id: str
        :param database_id: The ID of the database to create in Cloud Spanner.
        :type database_id: str
        :param ddl_statements: The string list containing DDL for the new database.
        :type ddl_statements: list[str]
        :param project_id: Optional, the ID of the  GCP project that owns the Cloud Spanner
            database. If set to None or missing, the default project_id from the GCP connection is used.
        :return: None
        """

        instance = self._get_client(project_id=project_id).instance(
            instance_id=instance_id)
        if not instance.exists():
            raise AirflowException("The instance {} does not exist in project {} !".
                                   format(instance_id, project_id))
        database = instance.database(database_id=database_id,
                                     ddl_statements=ddl_statements)
        try:
            operation = database.create()  # type: Operation
        except GoogleAPICallError as e:
            self.log.error('An error occurred: %s. Exiting.', e.message)
            raise e

        if operation:
            result = operation.result()
            self.log.info(result)
        return

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def update_database(self, instance_id, database_id, ddl_statements,
                        project_id=None,
                        operation_id=None):
        """
        Updates DDL of a database in Cloud Spanner.

        :type project_id: str
        :param instance_id: The ID of the Cloud Spanner instance.
        :type instance_id: str
        :param database_id: The ID of the database in Cloud Spanner.
        :type database_id: str
        :param ddl_statements: The string list containing DDL for the new database.
        :type ddl_statements: list[str]
        :param project_id: Optional, the ID of the GCP project that owns the Cloud Spanner
            database. If set to None or missing, the default project_id from the GCP connection is used.
        :param operation_id: (Optional) The unique per database operation ID that can be
            specified to implement idempotency check.
        :type operation_id: str
        :return: None
        """

        instance = self._get_client(project_id=project_id).instance(
            instance_id=instance_id)
        if not instance.exists():
            raise AirflowException("The instance {} does not exist in project {} !".
                                   format(instance_id, project_id))
        database = instance.database(database_id=database_id)
        try:
            operation = database.update_ddl(
                ddl_statements=ddl_statements, operation_id=operation_id)
            if operation:
                result = operation.result()
                self.log.info(result)
            return
        except AlreadyExists as e:
            if e.code == 409 and operation_id in e.message:
                self.log.info("Replayed update_ddl message - the operation id %s "
                              "was already done before.", operation_id)
                return
        except GoogleAPICallError as e:
            self.log.error('An error occurred: %s. Exiting.', e.message)
            raise e

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def delete_database(self, instance_id, database_id, project_id=None):
        """
        Drops a database in Cloud Spanner.

        :type project_id: str
        :param instance_id: The ID of the Cloud Spanner instance.
        :type instance_id: str
        :param database_id: The ID of the database in Cloud Spanner.
        :type database_id: str
        :param project_id: Optional, the ID of the  GCP project that owns the Cloud Spanner
            database. If set to None or missing, the default project_id from the GCP connection is used.
        :return: True if everything succeeded
        :rtype: bool
        """

        instance = self._get_client(project_id=project_id).\
            instance(instance_id=instance_id)
        if not instance.exists():
            raise AirflowException("The instance {} does not exist in project {} !".
                                   format(instance_id, project_id))
        database = instance.database(database_id=database_id)
        if not database.exists():
            self.log.info("The database {} is already deleted from instance {}. "
                          "Exiting.".format(database_id, instance_id))
            return
        try:
            operation = database.drop()  # type: Operation
        except GoogleAPICallError as e:
            self.log.error('An error occurred: %s. Exiting.', e.message)
            raise e

        if operation:
            result = operation.result()
            self.log.info(result)
        return

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def execute_dml(self, instance_id, database_id, queries, project_id=None):
        """
        Executes an arbitrary DML query (INSERT, UPDATE, DELETE).

        :param instance_id: The ID of the Cloud Spanner instance.
        :type instance_id: str
        :param database_id: The ID of the database in Cloud Spanner.
        :type database_id: str
        :param queries: The queries to execute.
        :type queries: str
        :param project_id: Optional, the ID of the  GCP project that owns the Cloud Spanner
            database. If set to None or missing, the default project_id from the GCP connection is used.
        :type project_id: str
        """
        self._get_client(project_id=project_id).instance(instance_id=instance_id).\
            database(database_id=database_id).run_in_transaction(
            lambda transaction: self._execute_sql_in_transaction(transaction, queries))

    @staticmethod
    def _execute_sql_in_transaction(transaction, queries):
        for sql in queries:
            transaction.execute_update(sql)
