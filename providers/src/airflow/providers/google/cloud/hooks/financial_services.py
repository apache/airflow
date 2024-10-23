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

import importlib.resources
import json

from googleapiclient.discovery import Resource, build_from_document

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class FinancialServicesHook(GoogleBaseHook):
    """
    Hook for interacting with the Google Financial Services API.

    :param gcp_conn_id: Identifier of connection to Google Cloud Platform.
        Defaults to "google_cloud_default".
    :param api_version: API version for the Financial Services API.
        Defaults to "v1".
    :param dev_key_var: Airflow variable name for accessing/saving the
        developer key. If key is not provided, secret value will be stored in a
        variable with the default name. Defaults to "AMLAI_API_KEY".
    :param dev_key_secret_uri: URI for the GCP secret (Secrets
        Manager) containing the developer key. Secret will only be accessed if
        dev_key_var does not exist. Defaults to None.
    """

    connection: Resource | None = None

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        api_version: str = "v1",
        dev_key_var: str = "AMLAI_API_KEY",
        dev_key_secret_uri: str | None = None,
        **kwargs,
    ) -> None:
        if kwargs.get("delegate_to") is not None:
            raise RuntimeError(
                "The `delegate_to` parameter has been deprecated before and finally"
                " removed in this version"
                " of Google Provider. You MUST convert it to `impersonate_chain`"
            )
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=None,
        )
        self.dev_key_var = dev_key_var
        self.dev_key_secret_uri = dev_key_secret_uri
        self.api_version = api_version

    def get_conn(self) -> Resource:
        """
        Establish a connection to the Google Financial Services API.

        :return: a Google Cloud Financial Services service object.
        """
        if not self.connection:
            api_doc_res = importlib.resources.files("airflow.providers.google.cloud.hooks").joinpath(
                "financial_services_discovery.json"
            )
            with importlib.resources.as_file(api_doc_res) as file:
                self.connection = build_from_document(json.loads(file.read_text()))

        return self.connection

    def get_instance(self, instance_resource_uri: str) -> dict:
        """
        Get an AML AI instance.

        :param instance_resource_uri: URI of the instance to get (format:
            'projects/<Project ID>/locations/<Location>/instances/<Instance>)

        :returns: A dictionary containing the instance metadata
        """
        conn = self.get_conn()
        response = conn.projects().locations().instances().get(name=instance_resource_uri).execute()
        return response

    def create_instance(self, instance_id: str, kms_key_uri: str, location_resource_uri: str) -> dict:
        """
        Create an AML AI instance.

        :param instance_id: Identifier for the instance to create
        :param kms_key: URI of the KMS key to that will be used for instance encryption
            (format: 'projects/<Project ID>/locations/<Location>/keyRings/<Key Ring>/
            cryptoKeys/<Key>')

        :returns: A dictionary containing metadata for the create instance operation
        """
        conn = self.get_conn()
        response = (
            conn.projects()
            .locations()
            .instances()
            .create(
                parent=location_resource_uri,
                instanceId=instance_id,
                body={"kmsKey": kms_key_uri},
            )
            .execute()
        )
        return response

    def delete_instance(self, instance_resource_uri: str) -> dict:
        """
        Delete an AML AI instance.

        :param instance_resource_uri: URI of the instance to delete (format:
                'projects/<Project ID>/locations/<Location>/instances/<Instance>)

        :returns: A dictionary containing metadata for the delete instance
                operation
        """
        conn = self.get_conn()
        response = conn.projects().locations().instances().delete(name=instance_resource_uri).execute()
        return response

    def get_operation(self, operation_resource_uri: str) -> dict:
        """
        Get an operation.

        :param operation_resource_uri: URI of the operation
        :return: A dictionary containing metadata for the operation
        """
        conn = self.get_conn()
        response = conn.projects().locations().operations().get(name=operation_resource_uri).execute()
        return response
