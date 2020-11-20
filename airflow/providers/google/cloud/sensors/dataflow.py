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
"""This module contains a Google Cloud Dataflow sensor."""
from typing import Callable, Optional, Sequence, Set, Union

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.dataflow import (
    DEFAULT_DATAFLOW_LOCATION,
    DataflowHook,
    DataflowJobStatus,
)
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class DataflowJobStatusSensor(BaseSensorOperator):
    """
    Checks for the status of a job in Google Cloud Dataflow.

    :param job_id: ID of the job to be checked.
    :type job_id: str
    :param expected_statuses: The expected state of the operation.
        See:
        https://cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.jobs#Job.JobState
    :type expected_statuses: Union[Set[str], str]
    :param project_id: Optional, the Google Cloud project ID in which to start a job.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :type project_id: str
    :param location: The location of the Dataflow job (for example europe-west1). See:
        https://cloud.google.com/dataflow/docs/concepts/regional-endpoints
    :type location: str
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled. See:
        https://developers.google.com/identity/protocols/oauth2/service-account#delegatingauthority
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

    template_fields = ['job_id']

    @apply_defaults
    def __init__(
        self,
        *,
        job_id: str,
        expected_statuses: Union[Set[str], str],
        project_id: Optional[str] = None,
        location: str = DEFAULT_DATAFLOW_LOCATION,
        gcp_conn_id: str = 'google_cloud_default',
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.job_id = job_id
        self.expected_statuses = (
            {expected_statuses} if isinstance(expected_statuses, str) else expected_statuses
        )
        self.project_id = project_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        self.hook: Optional[DataflowHook] = None

    def poke(self, context: dict) -> bool:
        self.log.info(
            "Waiting for job %s to be in one of the states: %s.",
            self.job_id,
            ", ".join(self.expected_statuses),
        )
        self.hook = DataflowHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        job = self.hook.get_job(
            job_id=self.job_id,
            project_id=self.project_id,
            location=self.location,
        )

        job_status = job["currentState"]
        self.log.debug("Current job status for job %s: %s.", self.job_id, job_status)

        if job_status in self.expected_statuses:
            return True
        elif job_status in DataflowJobStatus.TERMINAL_STATES:
            raise AirflowException(f"Job with id '{self.job_id}' is already in terminal state: {job_status}")

        return False


class DataflowJobMetricsSensor(BaseSensorOperator):
    """
    Checks the metrics of a job in Google Cloud Dataflow.

    :param job_id: ID of the job to be checked.
    :type job_id: str
    :param callback: callback which is called with list of read job metrics
        See:
        https://cloud.google.com/dataflow/docs/reference/rest/v1b3/MetricUpdate
    :type callback: callable
    :param project_id: Optional, the Google Cloud project ID in which to start a job.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :type project_id: str
    :param location: The location of the Dataflow job (for example europe-west1). See:
        https://cloud.google.com/dataflow/docs/concepts/regional-endpoints
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
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

    template_fields = ['job_id']

    @apply_defaults
    def __init__(
        self,
        *,
        job_id: str,
        callback: Callable[[dict], bool],
        project_id: Optional[str] = None,
        location: str = DEFAULT_DATAFLOW_LOCATION,
        gcp_conn_id: str = 'google_cloud_default',
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.job_id = job_id
        self.project_id = project_id
        self.callback = callback
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        self.hook: Optional[DataflowHook] = None

    def poke(self, context: dict) -> bool:
        self.hook = DataflowHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        result = self.hook.fetch_job_metrics_by_id(
            job_id=self.job_id,
            project_id=self.project_id,
            location=self.location,
        )

        return self.callback(result["metrics"])
