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
from __future__ import annotations

from google.api_core import operations_v1
from google.api_core.future import polling
from google.api_core.operation import Operation
from google.protobuf.empty_pb2 import Empty
from googleapiclient.discovery import Resource, build_from_document

from airflow.exceptions import AirflowException
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class FinancialServicesHook(GoogleBaseHook):
    """
    Hook for interacting with the Google Financial Services API.

    :param discovery_doc: Discovery document for building the Financial Services API
        as described `here <https://cloud.google.com/financial-services/anti-money-laundering/docs/reference/rest#discovery-document>`__
    :param gcp_conn_id: Identifier of connection to Google Cloud Platform.
        Defaults to "google_cloud_default".
    """

    connection: Resource | None = None

    def __init__(
        self,
        discovery_doc: dict,
        gcp_conn_id: str = "google_cloud_default",
        **kwargs,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=None,
        )
        self.discovery_doc = discovery_doc

    def get_operations_client(self):
        if not self.operations_client:
            credentials = self.get_credentials()
            self.operations_transport = operations_v1.OperationsRestTransport(
                host="financialservices.googleapis.com", credentials=credentials
            )
            self.operations_client = operations_v1.AbstractOperationsClient(
                transport=self.operations_transport
            )

        return self.operations_client

    def get_conn(self) -> Resource:
        """
        Establish a connection to the Google Financial Services API.

        :return: A Google Cloud Financial Services API service resource.
        """
        if not self.connection:
            self.connection = build_from_document(self.discovery_doc)

        return self.connection

    def wait_for_operation(self, operation: Operation, timeout: float | None = None):
        """Wait for long-lasting operation to complete."""
        try:
            return operation.result(timeout=timeout)
        except Exception:
            error = operation.exception(timeout=timeout)
            raise AirflowException(error)

    def get_instance(self, project_id: str, region: str, instance_id: str) -> dict:
        """
        Get a Financial Services Anti-Money Laundering AI instance.

        :param project_id:  Required. The ID of the Google Cloud project that the service belongs to.
        :param region:  Required. The ID of the Google Cloud region that the service belongs to.
        :param instance_id:  Required. The ID of the instance, which is used as the final component of the
            instances's name.

        :returns: A dictionary containing the instance metadata
        """
        conn = self.get_conn()
        name = f"projects/{project_id}/locations/{region}/instances/{instance_id}"
        response = conn.projects().locations().instances().get(name=name).execute()
        return response

    def create_instance(
        self, project_id: str, region: str, instance_id: str, kms_key_ring_id: str, kms_key_id: str
    ) -> Operation:
        """
        Create a Financial Services Anti-Money Laundering AI instance.

        :param project_id:  Required. The ID of the Google Cloud project that the service belongs to.
        :param region:  Required. The ID of the Google Cloud region that the service belongs to.
        :param instance_id:  Required. The ID of the instance, which is used as the final component of the
            instances's name.
        :param kms_key_ring_id:  Required. The ID of the Google Cloud KMS key ring containing the key to
            use for instance encryption
        :param kms_key_id:  Required. The ID of the Google Cloud KMS key to use for instance encryption

        :returns: A dictionary containing metadata for the create instance operation
        """
        conn = self.get_conn()
        operations_client = self.get_operations_client()

        parent = f"projects/{project_id}/locations/{region}"
        kms_key = (
            f"projects/{project_id}/locations/{region}/keyRings/{kms_key_ring_id}/cryptoKeys{kms_key_id}"
        )
        operation_json = (
            conn.projects()
            .locations()
            .instances()
            .create(
                parent=parent,
                instanceId=instance_id,
                body={"kmsKey": kms_key},
            )
            .execute()
        )
        return Operation(
            operation=operation_json,
            refresh=lambda: operations_client.get_operation(operation_json["name"]),
            cancel=lambda: operations_client.cancel_operation(operation_json["name"]),
            result_type=Empty,
            # metadata_type=OperationMetadata,
            polling=polling.DEFAULT_POLLING.with_timeout(timeout=21600),
        )

    def delete_instance(self, project_id: str, region: str, instance_id: str) -> Operation:
        """
        Delete a Financial Services Anti-Money Laundering AI instance.

        :param project_id:  Required. The ID of the Google Cloud project that the service belongs to.
        :param region:  Required. The ID of the Google Cloud region that the service belongs to.
        :param instance_id:  Required. The ID of the instance, which is used as the final component of the
            instances's name.

        :returns: A dictionary containing metadata for the delete instance
                operation
        """
        conn = self.get_conn()
        operations_client = self.get_operations_client()

        name = f"projects/{project_id}/locations/{region}/instances/{instance_id}"
        operation_json = conn.projects().locations().instances().delete(name=name).execute()

        return Operation(
            operation=operation_json,
            refresh=lambda: operations_client.get_operation(operation_json["name"]),
            cancel=lambda: operations_client.cancel_operation(operation_json["name"]),
            result_type=Empty,
            # metadata_type=OperationMetadata,
            polling=polling.DEFAULT_POLLING.with_timeout(timeout=21600),
        )

    def get_operation(self, project_id: str, region: str, operation_id: str) -> dict:
        """
        Get a Financial Services Anti-Money Laundering AI operation.

        :param project_id:  Required. The ID of the Google Cloud project that the service belongs to.
        :param region:  Required. The ID of the Google Cloud region that the service belongs to.
        :param operation_id:  Required. The ID of the operation, which is used as the final component of the
            operation's name.

        :return: A dictionary containing metadata for the operation
        """
        conn = self.get_conn()
        name = f"projects/{project_id}/locations/{region}/operations/{operation_id}"
        response = conn.projects().locations().operations().get(name=name).execute()
        return response
