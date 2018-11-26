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
from google.api_core.exceptions import GoogleAPICallError
from google.cloud.spanner_v1.client import Client
from google.cloud.spanner_v1.database import Database
from google.cloud.spanner_v1.instance import Instance  # noqa: F401
from google.longrunning.operations_grpc_pb2 import Operation  # noqa: F401
from typing import Optional, Callable  # noqa: F401

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook


# noinspection PyAbstractClass
class CloudSpannerHook(GoogleCloudBaseHook):
    """
    Hook for Google Cloud Spanner APIs.
    """
    _client = None

    def __init__(self,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None):
        super(CloudSpannerHook, self).__init__(gcp_conn_id, delegate_to)

    def get_client(self, project_id):
        # type: (str) -> Client
        """
        Provides a client for interacting with Cloud Spanner API.

        :param project_id: The ID of the project which owns the instances, tables and data.
        :type project_id: str
        :return: Client for interacting with Cloud Spanner API. See:
            https://googleapis.github.io/google-cloud-python/latest/spanner/client-api.html#google.cloud.spanner_v1.client.Client
        :rtype: object
        """
        if not self._client:
            self._client = Client(project=project_id, credentials=self._get_credentials())
        return self._client

    def get_instance(self, project_id, instance_id):
        # type: (str, str) -> Optional[Instance]
        """
        Gets information about a particular instance.

        :param project_id: The ID of the project which owns the instances, tables and data.
        :type project_id: str
        :param instance_id: The ID of the instance.
        :type instance_id: str
        :return: Representation of a Cloud Spanner Instance. See:
            https://googleapis.github.io/google-cloud-python/latest/spanner/instance-api.html#google.cloud.spanner_v1.instance.Instance
        :rtype: object
        """
        client = self.get_client(project_id)
        instance = client.instance(instance_id)
        if not instance.exists():
            return None
        return instance

    def create_instance(self, project_id, instance_id, configuration_name, node_count,
                        display_name):
        # type: (str, str, str, int, str) -> bool
        """
        Creates a new Cloud Spanner instance.

        :param project_id: The ID of the project which owns the instances, tables and
            data.
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
        :return: True if the operation succeeded, raises an exception otherwise.
        :rtype: bool
        """
        return self._apply_to_instance(project_id, instance_id, configuration_name,
                                       node_count, display_name, lambda x: x.create())

    def update_instance(self, project_id, instance_id, configuration_name, node_count,
                        display_name):
        # type: (str, str, str, int, str) -> bool
        """
        Updates an existing Cloud Spanner instance.

        :param project_id: The ID of the project which owns the instances, tables and
            data.
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
        :return: True if the operation succeeded, raises an exception otherwise.
        :rtype: bool
        """
        return self._apply_to_instance(project_id, instance_id, configuration_name,
                                       node_count, display_name, lambda x: x.update())

    def _apply_to_instance(self, project_id, instance_id, configuration_name, node_count,
                           display_name, func):
        # type: (str, str, str, int, str, Callable[[Instance], Operation]) -> bool
        """
        Invokes a method on a given instance by applying a specified Callable.

        :param project_id: The ID of the project which owns the instances, tables and
            data.
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
        client = self.get_client(project_id)
        instance = client.instance(instance_id,
                                   configuration_name=configuration_name,
                                   node_count=node_count,
                                   display_name=display_name)
        try:
            operation = func(instance)  # type: Operation
        except GoogleAPICallError as e:
            self.log.error('An error occurred: %s. Aborting.', e.message)
            raise e

        if operation:
            result = operation.result()
            self.log.info(result)
        return True

    def delete_instance(self, project_id, instance_id):
        # type: (str, str) -> bool
        """
        Deletes an existing Cloud Spanner instance.

        :param project_id: The ID of the project which owns the instances, tables and data.
        :type project_id: str
        :param instance_id: The ID of the instance.
        :type instance_id: str
        """
        client = self.get_client(project_id)
        instance = client.instance(instance_id)
        try:
            instance.delete()
            return True
        except GoogleAPICallError as e:
            self.log.error('An error occurred: %s. Aborting.', e.message)
            raise e

    def execute_dml(self, project_id, instance_id, database_id, queries):
        # type: (str, str, str, str) -> None
        """
        Executes an arbitrary DML query (INSERT, UPDATE, DELETE).

        :param project_id: The ID of the project which owns the instances, tables and data.
        :type project_id: str
        :param instance_id: The ID of the instance.
        :type instance_id: str
        :param database_id: The ID of the database.
        :type database_id: str
        :param queries: The queries to be executed.
        :type queries: str
        """
        client = self.get_client(project_id)
        instance = client.instance(instance_id)
        database = Database(database_id, instance)
        database.run_in_transaction(lambda transaction:
                                    self._execute_sql_in_transaction(transaction, queries))

    @staticmethod
    def _execute_sql_in_transaction(transaction, queries):
        for sql in queries:
            transaction.execute_update(sql)
