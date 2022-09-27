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
"""This module contains Google BigQuery sensors."""
from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, Any, Sequence

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.triggers.bigquery import BigQueryTableExistenceTrigger
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class BigQueryTableExistenceSensor(BaseSensorOperator):
    """
    Checks for the existence of a table in Google Bigquery.

    :param project_id: The Google cloud project in which to look for the table.
        The connection supplied to the hook must provide
        access to the specified project.
    :param dataset_id: The name of the dataset in which to look for the table.
        storage bucket.
    :param table_id: The name of the table to check the existence of.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        'project_id',
        'dataset_id',
        'table_id',
        'impersonation_chain',
    )
    ui_color = '#f0eee4'

    def __init__(
        self,
        *,
        project_id: str,
        dataset_id: str,
        table_id: str,
        gcp_conn_id: str = 'google_cloud_default',
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def poke(self, context: Context) -> bool:
        table_uri = f'{self.project_id}:{self.dataset_id}.{self.table_id}'
        self.log.info('Sensor checks existence of table: %s', table_uri)
        hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        return hook.table_exists(
            project_id=self.project_id, dataset_id=self.dataset_id, table_id=self.table_id
        )


class BigQueryTablePartitionExistenceSensor(BaseSensorOperator):
    """
    Checks for the existence of a partition within a table in Google Bigquery.

    :param project_id: The Google cloud project in which to look for the table.
        The connection supplied to the hook must provide
        access to the specified project.
    :param dataset_id: The name of the dataset in which to look for the table.
        storage bucket.
    :param table_id: The name of the table to check the existence of.
    :param partition_id: The name of the partition to check the existence of.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must
        have domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        'project_id',
        'dataset_id',
        'table_id',
        'partition_id',
        'impersonation_chain',
    )
    ui_color = '#f0eee4'

    def __init__(
        self,
        *,
        project_id: str,
        dataset_id: str,
        table_id: str,
        partition_id: str,
        gcp_conn_id: str = 'google_cloud_default',
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.partition_id = partition_id
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def poke(self, context: Context) -> bool:
        table_uri = f'{self.project_id}:{self.dataset_id}.{self.table_id}'
        self.log.info('Sensor checks existence of partition: "%s" in table: %s', self.partition_id, table_uri)
        hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        return hook.table_partition_exists(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            table_id=self.table_id,
            partition_id=self.partition_id,
        )


class BigQueryTableExistenceAsyncSensor(BigQueryTableExistenceSensor):
    """
    Checks for the existence of a table in Google Big Query.

    :param project_id: The Google cloud project in which to look for the table.
       The connection supplied to the hook must provide
       access to the specified project.
    :param dataset_id: The name of the dataset in which to look for the table.
       storage bucket.
    :param table_id: The name of the table to check the existence of.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param bigquery_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
       This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
    :param polling_interval: The interval in seconds to wait between checks table existence.
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        polling_interval: float = 5.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.polling_interval = polling_interval
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Context) -> None:
        """Airflow runs this method on the worker and defers using the trigger."""
        self.defer(
            timeout=timedelta(seconds=self.timeout),
            trigger=BigQueryTableExistenceTrigger(
                dataset_id=self.dataset_id,
                table_id=self.table_id,
                project_id=self.project_id,
                poll_interval=self.polling_interval,
                gcp_conn_id=self.gcp_conn_id,
                hook_params={
                    "delegate_to": self.delegate_to,
                    "impersonation_chain": self.impersonation_chain,
                },
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: dict[str, Any], event: dict[str, str] | None = None) -> str:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        table_uri = f"{self.project_id}:{self.dataset_id}.{self.table_id}"
        self.log.info("Sensor checks existence of table: %s", table_uri)
        if event:
            if event["status"] == "success":
                return event["message"]
            raise AirflowException(event["message"])
        raise AirflowException("No event received in trigger callback")
