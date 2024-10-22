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

from typing import TYPE_CHECKING, Sequence

from airflow.providers.google.cloud.hooks.financial_services import FinancialServicesHook
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class FinancialServicesCreateInstanceOperator(GoogleCloudBaseOperator):
    """
    Create a Financial Services AML AI Instance.

    :param instance_id: Identifier for the instance to create
    :param location_resource_uri: URI of the location to create the instance in
        (format: 'projects/<Project ID>/locations/<Location>)
    :param kms_key_uri: URI of the KMS key to that will be used for instance
        encryption (format: 'projects/<Project ID>/locations/<Location>/keyRings/
        <Key Ring>/cryptoKeys/<Key>')
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

    template_fields: Sequence[str] = ("instance_id", "location_resource_uri", "kms_key_uri")

    def __init__(
        self,
        instance_id: str,
        location_resource_uri: str,
        kms_key_uri: str,
        gcp_conn_id: str = "google_cloud_default",
        api_version: str = "v1",
        dev_key_var: str = "AMLAI_API_KEY",
        dev_key_secret_uri: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.instance_id = instance_id
        self.location_resource_uri = location_resource_uri
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self.dev_key_var = dev_key_var
        self.dev_key_secret_uri = dev_key_secret_uri
        self.kms_key_uri = kms_key_uri

    def execute(self, context: Context):
        super().execute(context)
        hook = FinancialServicesHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            dev_key_var=self.dev_key_var,
            dev_key_secret_uri=self.dev_key_secret_uri,
        )
        response = hook.create_instance(
            instance_id=self.instance_id,
            kms_key_uri=self.kms_key_uri,
            location_resource_uri=self.location_resource_uri,
        )
        return response["name"]


class FinancialServicesDeleteInstanceOperator(GoogleCloudBaseOperator):
    """
    Delete a Financial Services AML AI Instance.

    :param instance_resource_uri: URI of the instance to delete (format:
        'projects/<Project ID>/locations/<Location>/instances/<Instance>)
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

    template_fields: Sequence[str] = "instance_resource_uri"

    def __init__(
        self,
        instance_resource_uri: str,
        gcp_conn_id: str = "google_cloud_default",
        api_version: str = "v1",
        dev_key_var: str = "AMLAI_API_KEY",
        dev_key_secret_uri: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.instance_resource_uri = instance_resource_uri
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self.dev_key_var = dev_key_var
        self.dev_key_secret_uri = dev_key_secret_uri

    def execute(self, context: Context):
        super().execute(context)
        hook = FinancialServicesHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            dev_key_var=self.dev_key_var,
            dev_key_secret_uri=self.dev_key_secret_uri,
        )
        response = hook.delete_instance(
            instance_resource_uri=self.instance_resource_uri,
        )
        return response["name"]


class FinancialServicesGetInstanceOperator(GoogleCloudBaseOperator):
    """
    Get a Financial Services AML AI Instance.

    :param instance_resource_uri: URI of the instance to get (format:
        'projects/<Project ID>/locations/<Location>/instances/<Instance>)
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

    template_fields: Sequence[str] = "instance_resource_uri"

    def __init__(
        self,
        instance_resource_uri: str,
        gcp_conn_id: str = "google_cloud_default",
        api_version: str = "v1",
        dev_key_var: str = "AMLAI_API_KEY",
        dev_key_secret_uri: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.instance_resource_uri = instance_resource_uri
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self.dev_key_var = dev_key_var
        self.dev_key_secret_uri = dev_key_secret_uri

    def execute(self, context: Context):
        super().execute(context)
        hook = FinancialServicesHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            dev_key_var=self.dev_key_var,
            dev_key_secret_uri=self.dev_key_secret_uri,
        )
        response = hook.get_instance(
            instance_resource_uri=self.instance_resource_uri,
        )
        return response
