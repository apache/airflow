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

    :param project_id:  Required. The ID of the Google Cloud project that the service belongs to.
    :param region:  Required. The ID of the Google Cloud region that the service belongs to.
    :param instance_id:  Required. The ID of the instance, which is used as the final component of the
        instances's name.
    :param kms_key_ring_id:  Required. The ID of the Google Cloud KMS key ring containing the key to
        use for instance encryption
    :param kms_key_id:  Required. The ID of the Google Cloud KMS key to use for instance encryption
    :param discovery_doc: Discovery document for building the Financial Services API
        as described `here <https://cloud.google.com/financial-services/anti-money-laundering/docs/reference/rest#discovery-document>`__
    :param gcp_conn_id: Identifier of connection to Google Cloud Platform.
        Defaults to "google_cloud_default".
    """

    # [START howto_operator_financial_services_create_instance_template_fields]
    template_fields: Sequence[str] = (
        "project_id",
        "region",
        "instance_id",
        "kms_key_ring_id",
        "kms_key_id",
        "discovery_doc",
        "gcp_conn_id",
    )
    # [END howto_operator_financial_services_create_instance_template_fields]

    def __init__(
        self,
        project_id: str,
        region: str,
        instance_id: str,
        kms_key_ring_id: str,
        kms_key_id: str,
        discovery_doc: dict,
        gcp_conn_id: str = "google_cloud_default",
        timeout: float = 43200.0,  # 12hr
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.instance_id = instance_id
        self.kms_key_ring_id = kms_key_ring_id
        self.kms_key_id = kms_key_id
        self.discovery_doc = discovery_doc
        self.gcp_conn_id = gcp_conn_id
        self.timeout = timeout

    def execute(self, context: Context):
        hook = FinancialServicesHook(
            discovery_doc=self.discovery_doc,
            gcp_conn_id=self.gcp_conn_id,
        )
        self.log.info("Creating Financial Services instance: %s", self.instance_id)

        operation = hook.create_instance(
            project_id=self.project_id,
            region=self.region,
            instance_id=self.instance_id,
            kms_key_ring_id=self.kms_key_ring_id,
            kms_key_id=self.kms_key_id,
        )
        hook.wait_for_operation(
            operation=operation,
            timeout=self.timeout,
        )


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
    template_fields: Sequence[str] = ("project_id", "region", "instance_id", "discovery_doc", "gcp_conn_id")
    # [END howto_operator_financial_services_get_instance_template_fields]

    def __init__(
        self,
        project_id: str,
        region: str,
        instance_id: str,
        discovery_doc: dict,
        gcp_conn_id: str = "google_cloud_default",
        timeout: float = 43200.0,  # 12hr
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.instance_id = instance_id
        self.discovery_doc = discovery_doc
        self.gcp_conn_id = gcp_conn_id
        self.timeout = timeout

    def execute(self, context: Context):
        hook = FinancialServicesHook(
            discovery_doc=self.discovery_doc,
            gcp_conn_id=self.gcp_conn_id,
        )
        self.log.info("Deleting Financial Services instance: %s", self.instance_id)

        operation = hook.delete_instance(
            project_id=self.project_id,
            region=self.region,
            instance_id=self.instance_id,
        )
        hook.wait_for_operation(operation=operation, timeout=self.timeout)


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
    template_fields: Sequence[str] = ("project_id", "region", "instance_id", "discovery_doc", "gcp_conn_id")
    # [END howto_operator_financial_services_delete_instance_template_fields]

    def __init__(
        self,
        project_id: str,
        region: str,
        instance_id: str,
        discovery_doc: dict,
        gcp_conn_id: str = "google_cloud_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.instance_id = instance_id
        self.discovery_doc = discovery_doc
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Context):
        hook = FinancialServicesHook(
            discovery_doc=self.discovery_doc,
            gcp_conn_id=self.gcp_conn_id,
        )
        self.log.info("Fetching Financial Services instance: %s", self.instance_id)

        response = hook.get_instance(
            project_id=self.project_id, region=self.region, instance_id=self.instance_id
        )
        return response
