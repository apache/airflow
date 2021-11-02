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
"""This module contains a Google Cloud Data Fusion sensors."""
from typing import Optional, Sequence, Set, Union

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.datafusion import DataFusionHook
from airflow.sensors.base import BaseSensorOperator


class CloudDataFusionPipelineStateSensor(BaseSensorOperator):
    """
    Check the status of the pipeline in the Google Cloud Data Fusion

    :param pipeline_name: Your pipeline name.
    :type pipeline_name: str
    :param pipeline_id: Your pipeline ID.
    :type pipeline_name: str
    :param expected_statuses: State that is expected
    :type expected_statuses: set[str]
    :param failure_statuses: State that will terminate the sensor with an exception
    :type failure_statuses: set[str]
    :param instance_name: The name of the instance.
    :type instance_name: str
    :param location: The Cloud Data Fusion location in which to handle the request.
    :type location: str
    :param project_id: The ID of the Google Cloud project that the instance belongs to.
    :type project_id: str
    :param namespace: If your pipeline belongs to a Basic edition instance, the namespace ID
        is always default. If your pipeline belongs to an Enterprise edition instance, you
        can create a namespace.
    :type namespace: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]

    """

    template_fields = ['pipeline_id']

    def __init__(
        self,
        pipeline_name: str,
        pipeline_id: str,
        expected_statuses: Set[str],
        instance_name: str,
        location: str,
        failure_statuses: Set[str] = None,
        project_id: Optional[str] = None,
        namespace: str = "default",
        gcp_conn_id: str = 'google_cloud_default',
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.pipeline_name = pipeline_name
        self.pipeline_id = pipeline_id
        self.expected_statuses = expected_statuses
        self.failure_statuses = failure_statuses
        self.instance_name = instance_name
        self.location = location
        self.project_id = project_id
        self.namespace = namespace
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def poke(self, context: dict) -> bool:
        self.log.info(
            "Waiting for pipeline %s to be in one of the states: %s.",
            self.pipeline_id,
            ", ".join(self.expected_statuses),
        )
        hook = DataFusionHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        instance = hook.get_instance(
            instance_name=self.instance_name,
            location=self.location,
            project_id=self.project_id,
        )
        api_url = instance["apiEndpoint"]
        pipeline_status = None
        try:
            pipeline_workflow = hook.get_pipeline_workflow(
                pipeline_name=self.pipeline_name,
                instance_url=api_url,
                pipeline_id=self.pipeline_id,
                namespace=self.namespace,
            )
            pipeline_status = pipeline_workflow["status"]
        except AirflowException:
            pass  # Because the pipeline may not be visible in system yet

        if self.failure_statuses and pipeline_status in self.failure_statuses:
            raise AirflowException(
                f"Pipeline with id '{self.pipeline_id}' state is: {pipeline_status}. "
                f"Terminating sensor..."
            )

        self.log.debug(
            "Current status of the pipeline workflow for %s: %s.", self.pipeline_id, pipeline_status
        )
        return pipeline_status in self.expected_statuses
