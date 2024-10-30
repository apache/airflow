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
    Create a Financial Services Anti-Money Laundering AI instance.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:FinancialServicesCreateInstanceOperator`

    :param instance_id: Identifier for the instance to create
    :param location_resource_uri: URI of the location to create the instance in
        (format: 'projects/<Project ID>/locations/<Location>)
    :param kms_key_uri: URI of the KMS key to that will be used for instance
        encryption (format: 'projects/<Project ID>/locations/<Location>/keyRings/
        <Key Ring>/cryptoKeys/<Key>')
    :param discovery_doc: Discovery document for building the Financial Services API
        as described `here <https://cloud.google.com/financial-services/anti-money-laundering/docs/reference/rest#discovery-document>`__
    :param gcp_conn_id: Identifier of connection to Google Cloud Platform.
        Defaults to "google_cloud_default".
    """

    # [START howto_operator_financial_services_create_instance_template_fields]
    template_fields: Sequence[str] = (
        "instance_id",
        "location_resource_uri",
        "kms_key_uri",
        "discovery_doc",
        "gcp_conn_id",
    )
    # [END howto_operator_financial_services_create_instance_template_fields]

    def __init__(
        self,
        instance_id: str,
        location_resource_uri: str,
        kms_key_uri: str,
        discovery_doc: dict,
        gcp_conn_id: str = "google_cloud_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.instance_id = instance_id
        self.location_resource_uri = location_resource_uri
        self.kms_key_uri = kms_key_uri
        self.discovery_doc = discovery_doc
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Context):
        hook = FinancialServicesHook(
            discovery_doc=self.discovery_doc,
            gcp_conn_id=self.gcp_conn_id,
        )
        response = hook.create_instance(
            instance_id=self.instance_id,
            kms_key_uri=self.kms_key_uri,
            location_resource_uri=self.location_resource_uri,
        )
        return response["name"]


class FinancialServicesDeleteInstanceOperator(GoogleCloudBaseOperator):
    """
    Delete a Financial Services Anti-Money Laundering AI instance.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:FinancialServicesDeleteInstanceOperator`

    :param instance_resource_uri: URI of the instance to delete (format:
        'projects/<Project ID>/locations/<Location>/instances/<Instance ID>)
    :param discovery_doc: Discovery document for building the Financial Services API
        as described `here <https://cloud.google.com/financial-services/anti-money-laundering/docs/reference/rest#discovery-document>`__
    :param gcp_conn_id: Identifier of connection to Google Cloud Platform.
        Defaults to "google_cloud_default".
    """

    # [START howto_operator_financial_services_get_instance_template_fields]
    template_fields: Sequence[str] = ("instance_resource_uri", "discovery_doc", "gcp_conn_id")
    # [END howto_operator_financial_services_get_instance_template_fields]

    def __init__(
        self,
        instance_resource_uri: str,
        discovery_doc: dict,
        gcp_conn_id: str = "google_cloud_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.instance_resource_uri = instance_resource_uri
        self.discovery_doc = discovery_doc
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Context):
        hook = FinancialServicesHook(
            discovery_doc=self.discovery_doc,
            gcp_conn_id=self.gcp_conn_id,
        )
        response = hook.delete_instance(
            instance_resource_uri=self.instance_resource_uri,
        )
        return response["name"]


class FinancialServicesGetInstanceOperator(GoogleCloudBaseOperator):
    """
    Get a Financial Services Anti-Money Laundering AI instance.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:FinancialServicesGetInstanceOperator`

    :param instance_resource_uri: URI of the instance to get (format:
        'projects/<Project ID>/locations/<Location>/instances/<Instance ID>)
    :param discovery_doc: Discovery document for building the Financial Services API
        as described `here <https://cloud.google.com/financial-services/anti-money-laundering/docs/reference/rest#discovery-document>`__
    :param gcp_conn_id: Identifier of connection to Google Cloud Platform.
        Defaults to "google_cloud_default".
    """

    # [START howto_operator_financial_services_delete_instance_template_fields]
    template_fields: Sequence[str] = ("instance_resource_uri", "discovery_doc", "gcp_conn_id")
    # [END howto_operator_financial_services_delete_instance_template_fields]

    def __init__(
        self,
        instance_resource_uri: str,
        discovery_doc: dict,
        gcp_conn_id: str = "google_cloud_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.instance_resource_uri = instance_resource_uri
        self.discovery_doc = discovery_doc
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Context):
        hook = FinancialServicesHook(
            discovery_doc=self.discovery_doc,
            gcp_conn_id=self.gcp_conn_id,
        )
        response = hook.get_instance(
            instance_resource_uri=self.instance_resource_uri,
        )
        return response
