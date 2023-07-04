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
"""This module contains a Google Cloud Spanner Hook."""
from __future__ import annotations

from typing import Callable, Sequence

from google.api_core.exceptions import AlreadyExists, GoogleAPICallError
from google.cloud.spanner_v1.client import Client
from google.cloud.spanner_v1.database import Database
from google.cloud.spanner_v1.instance import Instance
from google.cloud.spanner_v1.transaction import Transaction
from google.longrunning.operations_grpc_pb2 import Operation

from airflow.exceptions import AirflowException
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class SpannerHook(GoogleBaseHook):
    """
    Hook for Google Cloud Spanner APIs.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        if kwargs.get("delegate_to") is not None:
            raise RuntimeError(
                "The `delegate_to` parameter has been deprecated before and finally removed in this version"
                " of Google Provider. You MUST convert it to `impersonate_chain`"
            )
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
        )
        self._client: Client | None = None

    def _get_client(self, project_id: str) -> Client:
        """
        Provides a client for interacting with the Cloud Spanner API.

        :param project_id: The ID of the Google Cloud project.
        :return: Client
        """
        if not self._client:
            self._client = Client(
                project=project_id, credentials=self.get_credentials(), client_info=CLIENT_INFO
            )
        return self._client

    @GoogleBaseHook.fallback_to_default_project_id
    def get_instance(
        self,
        instance_id: str,
        project_id: str,
    ) -> Instance | None:
        """
        Gets information about a particular instance.

        :param project_id: Optional, The ID of the Google Cloud project that owns the Cloud Spanner
            database. If set to None or missing, the default project_id from the Google Cloud connection
            is used.
        :param instance_id: The ID of the Cloud Spanner instance.
        :return: Spanner instance
        """
        instance = self._get_client(project_id=project_id).instance(instance_id=instance_id)
        if not instance.exists():
            return None
        return instance

    def _apply_to_instance(
        self,
        project_id: str,
        instance_id: str,
        configuration_name: str,
        node_count: int,
        display_name: str,
        func: Callable[[Instance], Operation],
    ) -> None:
        """
        Invokes a method on a given instance by applying a specified Callable.

        :param project_id: The ID of the Google Cloud project that owns the Cloud Spanner database.
        :param instance_id: The ID of the instance.
        :param configuration_name: Name of the instance configuration defining how the
            instance will be created. Required for instances which do not yet exist.
        :param node_count: (Optional) Number of nodes allocated to the instance.
        :param display_name: (Optional) The display name for the instance in the Cloud
            Console UI. (Must be between 4 and 30 characters.) If this value is not set
            in the constructor, will fall back to the instance ID.
        :param func: Method of the instance to be called.
        """
        instance = self._get_client(project_id=project_id).instance(
            instance_id=instance_id,
            configuration_name=configuration_name,
            node_count=node_count,
            display_name=display_name,
        )
        try:
            operation: Operation = func(instance)
        except GoogleAPICallError as e:
            self.log.error("An error occurred: %s. Exiting.", e.message)
            raise e

        if operation:
            result = operation.result()
            self.log.info(result)

    @GoogleBaseHook.fallback_to_default_project_id
    def create_instance(
        self,
        instance_id: str,
        configuration_name: str,
        node_count: int,
        display_name: str,
        project_id: str,
    ) -> None:
        """
        Creates a new Cloud Spanner instance.

        :param instance_id: The ID of the Cloud Spanner instance.
        :param configuration_name: The name of the instance configuration defining how the
            instance will be created. Possible configuration values can be retrieved via
            https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instanceConfigs/list
        :param node_count: (Optional) The number of nodes allocated to the Cloud Spanner
            instance.
        :param display_name: (Optional) The display name for the instance in the Google Cloud Console.
            Must be between 4 and 30 characters. If this value is not passed, the name falls back
            to the instance ID.
        :param project_id: Optional, the ID of the Google Cloud project that owns the Cloud Spanner
            database. If set to None or missing, the default project_id from the Google Cloud connection
            is used.
        :return: None
        """
        self._apply_to_instance(
            project_id, instance_id, configuration_name, node_count, display_name, lambda x: x.create()
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def update_instance(
        self,
        instance_id: str,
        configuration_name: str,
        node_count: int,
        display_name: str,
        project_id: str,
    ) -> None:
        """
        Updates an existing Cloud Spanner instance.

        :param instance_id: The ID of the Cloud Spanner instance.
        :param configuration_name: The name of the instance configuration defining how the
            instance will be created. Possible configuration values can be retrieved via
            https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instanceConfigs/list
        :param node_count: (Optional) The number of nodes allocated to the Cloud Spanner
            instance.
        :param display_name: (Optional) The display name for the instance in the Google Cloud
            Console. Must be between 4 and 30 characters. If this value is not set in
            the constructor, the name falls back to the instance ID.
        :param project_id: Optional, the ID of the Google Cloud project that owns the Cloud Spanner
            database. If set to None or missing, the default project_id from the Google Cloud connection
            is used.
        :return: None
        """
        self._apply_to_instance(
            project_id, instance_id, configuration_name, node_count, display_name, lambda x: x.update()
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_instance(self, instance_id: str, project_id: str) -> None:
        """
        Deletes an existing Cloud Spanner instance.

        :param instance_id: The ID of the Cloud Spanner instance.
        :param project_id: Optional, the ID of the Google Cloud project that owns the Cloud Spanner
            database. If set to None or missing, the default project_id from the Google Cloud connection
            is used.
        :return: None
        """
        instance = self._get_client(project_id=project_id).instance(instance_id)
        try:
            instance.delete()
            return
        except GoogleAPICallError as e:
            self.log.error("An error occurred: %s. Exiting.", e.message)
            raise e

    @GoogleBaseHook.fallback_to_default_project_id
    def get_database(
        self,
        instance_id: str,
        database_id: str,
        project_id: str,
    ) -> Database | None:
        """
        Retrieves a database in Cloud Spanner. If the database does not exist
        in the specified instance, it returns None.

        :param instance_id: The ID of the Cloud Spanner instance.
        :param database_id: The ID of the database in Cloud Spanner.
        :param project_id: Optional, the ID of the Google Cloud project that owns the Cloud Spanner
            database. If set to None or missing, the default project_id from the Google Cloud connection
            is used.
        :return: Database object or None if database does not exist
        """
        instance = self._get_client(project_id=project_id).instance(instance_id=instance_id)
        if not instance.exists():
            raise AirflowException(f"The instance {instance_id} does not exist in project {project_id} !")
        database = instance.database(database_id=database_id)
        if not database.exists():
            return None

        return database

    @GoogleBaseHook.fallback_to_default_project_id
    def create_database(
        self,
        instance_id: str,
        database_id: str,
        ddl_statements: list[str],
        project_id: str,
    ) -> None:
        """
        Creates a new database in Cloud Spanner.

        :param instance_id: The ID of the Cloud Spanner instance.
        :param database_id: The ID of the database to create in Cloud Spanner.
        :param ddl_statements: The string list containing DDL for the new database.
        :param project_id: Optional, the ID of the Google Cloud project that owns the Cloud Spanner
            database. If set to None or missing, the default project_id from the Google Cloud connection
            is used.
        :return: None
        """
        instance = self._get_client(project_id=project_id).instance(instance_id=instance_id)
        if not instance.exists():
            raise AirflowException(f"The instance {instance_id} does not exist in project {project_id} !")
        database = instance.database(database_id=database_id, ddl_statements=ddl_statements)
        try:
            operation: Operation = database.create()
        except GoogleAPICallError as e:
            self.log.error("An error occurred: %s. Exiting.", e.message)
            raise e

        if operation:
            result = operation.result()
            self.log.info(result)

    @GoogleBaseHook.fallback_to_default_project_id
    def update_database(
        self,
        instance_id: str,
        database_id: str,
        ddl_statements: list[str],
        project_id: str,
        operation_id: str | None = None,
    ) -> None:
        """
        Updates DDL of a database in Cloud Spanner.

        :param instance_id: The ID of the Cloud Spanner instance.
        :param database_id: The ID of the database in Cloud Spanner.
        :param ddl_statements: The string list containing DDL for the new database.
        :param project_id: Optional, the ID of the Google Cloud project that owns the Cloud Spanner
            database. If set to None or missing, the default project_id from the Google Cloud connection
            is used.
        :param operation_id: (Optional) The unique per database operation ID that can be
            specified to implement idempotency check.
        :return: None
        """
        instance = self._get_client(project_id=project_id).instance(instance_id=instance_id)
        if not instance.exists():
            raise AirflowException(f"The instance {instance_id} does not exist in project {project_id} !")
        database = instance.database(database_id=database_id)
        try:
            operation = database.update_ddl(ddl_statements=ddl_statements, operation_id=operation_id)
            if operation:
                result = operation.result()
                self.log.info(result)
            return
        except AlreadyExists as e:
            if e.code == 409 and operation_id in e.message:
                self.log.info(
                    "Replayed update_ddl message - the operation id %s was already done before.",
                    operation_id,
                )
                return
        except GoogleAPICallError as e:
            self.log.error("An error occurred: %s. Exiting.", e.message)
            raise e

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_database(self, instance_id: str, database_id, project_id: str) -> bool:
        """
        Drops a database in Cloud Spanner.

        :param instance_id: The ID of the Cloud Spanner instance.
        :param database_id: The ID of the database in Cloud Spanner.
        :param project_id: Optional, the ID of the Google Cloud project that owns the Cloud Spanner
            database. If set to None or missing, the default project_id from the Google Cloud connection
            is used.
        :return: True if everything succeeded
        """
        instance = self._get_client(project_id=project_id).instance(instance_id=instance_id)
        if not instance.exists():
            raise AirflowException(f"The instance {instance_id} does not exist in project {project_id} !")
        database = instance.database(database_id=database_id)
        if not database.exists():
            self.log.info(
                "The database %s is already deleted from instance %s. Exiting.", database_id, instance_id
            )
            return False
        try:
            database.drop()
        except GoogleAPICallError as e:
            self.log.error("An error occurred: %s. Exiting.", e.message)
            raise e

        return True

    @GoogleBaseHook.fallback_to_default_project_id
    def execute_dml(
        self,
        instance_id: str,
        database_id: str,
        queries: list[str],
        project_id: str,
    ) -> None:
        """
        Executes an arbitrary DML query (INSERT, UPDATE, DELETE).

        :param instance_id: The ID of the Cloud Spanner instance.
        :param database_id: The ID of the database in Cloud Spanner.
        :param queries: The queries to execute.
        :param project_id: Optional, the ID of the Google Cloud project that owns the Cloud Spanner
            database. If set to None or missing, the default project_id from the Google Cloud connection
            is used.
        """
        self._get_client(project_id=project_id).instance(instance_id=instance_id).database(
            database_id=database_id
        ).run_in_transaction(lambda transaction: self._execute_sql_in_transaction(transaction, queries))

    @staticmethod
    def _execute_sql_in_transaction(transaction: Transaction, queries: list[str]):
        for sql in queries:
            transaction.execute_update(sql)
