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

import json
from pathlib import PurePath

from google.api_core.future import polling
from google.api_core.operation import Operation
from google.longrunning import operations_pb2
from google.protobuf.empty_pb2 import Empty
from google.protobuf.json_format import ParseDict
from googleapiclient.discovery import Resource, build, build_from_document

from airflow.exceptions import AirflowException
from airflow.models import Variable
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
        gcp_conn_id: str = "google_cloud_default",
        **kwargs,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=None,
        )

    def _get_developer_key(self) -> str | None:
        return Variable.get("financial_services_api_key", default_var=None)

    def _get_discovery_doc(self) -> dict | None:
        discovery_doc_path = Variable.get("financial_services_discovery_doc_path", default_var=None)
        if not discovery_doc_path:
            discovery_doc = None
        else:
            with open(discovery_doc_path) as file:
                discovery_doc = json.load(file)
        return discovery_doc

    def get_conn(self) -> Resource:
        """
        Establish a connection to the Google Financial Services API.

        :return: A Google Cloud Financial Services API service resource.
        """
        if not self.connection:
            developer_key = self._get_developer_key()
            discovery_doc = self._get_discovery_doc()

            if developer_key:
                credentials = self.get_credentials()
                self.connection = build(
                    serviceName="financialservices",
                    version="v1",
                    developerKey=developer_key,
                    discoveryServiceUrl="https://financialservices.googleapis.com/$discovery/rest?version=v1",
                    credentials=credentials,
                )
            elif discovery_doc:
                self.connection = build_from_document(discovery_doc)
            else:
                raise AirflowException(
                    "Connecting to financialservices.googleapis.com requires either 'financial_services_api_key' or "
                    "'financial_services_discovery_doc_path' to be set in Airflow variables. Use 'airflow variables set financial_services_api_key <API KEY VALUE>' "
                    "to set the variable"
                )

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

        :returns: The create instance operation.
        """
        conn = self.get_conn()

        parent = f"projects/{project_id}/locations/{region}"
        kms_key = (
            f"projects/{project_id}/locations/{region}/keyRings/{kms_key_ring_id}/cryptoKeys/{kms_key_id}"
        )
        response = (
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
        operation_id, operation_proto = self._parse_operation_proto(response)

        return Operation(
            operation=operation_proto,
            refresh=lambda: self._get_operation(project_id, region, operation_id),
            cancel=lambda: self._cancel_operation(project_id, region, operation_id),
            result_type=Empty,
            metadata_type=Empty,
            polling=polling.DEFAULT_POLLING,
        )

    def delete_instance(self, project_id: str, region: str, instance_id: str) -> Operation:
        """
        Delete a Financial Services Anti-Money Laundering AI instance.

        :param project_id:  Required. The ID of the Google Cloud project that the service belongs to.
        :param region:  Required. The ID of the Google Cloud region that the service belongs to.
        :param instance_id:  Required. The ID of the instance, which is used as the final component of the
            instances's name.

        :returns: The delete instance operation.
        """
        conn = self.get_conn()

        name = f"projects/{project_id}/locations/{region}/instances/{instance_id}"
        response = conn.projects().locations().instances().delete(name=name).execute()
        operation_id, operation_proto = self._parse_operation_proto(response)

        return Operation(
            operation=operation_proto,
            refresh=lambda: self._get_operation(project_id, region, operation_id),
            cancel=lambda: self._cancel_operation(project_id, region, operation_id),
            result_type=Empty,
            metadata_type=Empty,
            polling=polling.DEFAULT_POLLING,
        )

    def _parse_operation_proto(self, json_response: dict) -> tuple[str, operations_pb2.Operation]:
        """
        Parse an operation response from a Financial Services API call using operations_pb2.Operation.

        :param json_response: Required. Long-running operation data returned from the Financial Services API in JSON format.

        :returns: Tuple containing the operation ID and a parsed operations_pb2.Operation.
        """
        # Can not find message descriptor by type_url: type.googleapis.com/google.cloud.financialservices.v1.OperationMetadata
        # replace operation metadata protobuf with Empty
        json_response["metadata"] = {"@type": "type.googleapis.com/google.protobuf.Empty"}

        if "response" in json_response.keys():
            # Can not find message descriptor by type_url: type.googleapis.com/google.cloud.financialservices.v1.Instance
            # replace instance protobuf with Empty; response can be parsed, but no instance data can be returned
            json_response["response"] = {"@type": "type.googleapis.com/google.protobuf.Empty"}

        operation_proto = ParseDict(js_dict=json_response, message=operations_pb2.Operation())
        operation_id = PurePath(operation_proto.name).name
        return operation_id, operation_proto

    def _get_operation(self, project_id: str, region: str, operation_id: str) -> operations_pb2.Operation:
        """
        Get a long-running operation.

        :param project_id:  Required. The ID of the Google Cloud project that the service belongs to.
        :param region:  Required. The ID of the Google Cloud region that the service belongs to.
        :param operation_id:  Required. The ID of the long-running operation.

        :returns: The parsed operations_pb2.Operation.
        """
        conn = self.get_conn()
        name = f"projects/{project_id}/locations/{region}/operations/{operation_id}"
        response = conn.projects().locations().operations().get(name=name).execute()
        _, operation_proto = self._parse_operation_proto(response)
        return operation_proto

    def _cancel_operation(self, project_id: str, region: str, operation_id: str):
        """
        Cancel a long-running operation.

        :param project_id:  Required. The ID of the Google Cloud project that the service belongs to.
        :param region:  Required. The ID of the Google Cloud region that the service belongs to.
        :param operation_id:  Required. The ID of the long-running operation.
        """
        conn = self.get_conn()
        name = f"projects/{project_id}/locations/{region}/operations/{operation_id}"
        conn.projects().locations().operations().cancel(name=name).execute()
