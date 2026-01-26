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

import warnings
from collections.abc import Sequence
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from airflow.configuration import conf
from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.common.compat.sdk import AirflowException, BaseSensorOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.triggers.bigquery import (
    BigQueryTableExistenceTrigger,
    BigQueryTablePartitionExistenceTrigger,
    BigQueryStreamingBufferEmptyTrigger,
)

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


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
        "project_id",
        "dataset_id",
        "table_id",
        "impersonation_chain",
    )
    ui_color = "#f0eee4"

    def __init__(
        self,
        *,
        project_id: str,
        dataset_id: str,
        table_id: str,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        if deferrable and "poke_interval" not in kwargs:
            # TODO: Remove once deprecated
            if "polling_interval" in kwargs:
                kwargs["poke_interval"] = kwargs["polling_interval"]
                warnings.warn(
                    "Argument `poll_interval` is deprecated and will be removed "
                    "in a future release.  Please use `poke_interval` instead.",
                    AirflowProviderDeprecationWarning,
                    stacklevel=2,
                )
            else:
                kwargs["poke_interval"] = 5

        super().__init__(**kwargs)

        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

        self.deferrable = deferrable

    def poke(self, context: Context) -> bool:
        table_uri = f"{self.project_id}:{self.dataset_id}.{self.table_id}"
        self.log.info("Sensor checks existence of table: %s", table_uri)
        hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        return hook.table_exists(
            project_id=self.project_id, dataset_id=self.dataset_id, table_id=self.table_id
        )

    def execute(self, context: Context) -> None:
        """Airflow runs this method on the worker and defers using the trigger."""
        if not self.deferrable:
            super().execute(context)
        else:
            if not self.poke(context=context):
                self.defer(
                    timeout=timedelta(seconds=self.timeout),
                    trigger=BigQueryTableExistenceTrigger(
                        dataset_id=self.dataset_id,
                        table_id=self.table_id,
                        project_id=self.project_id,
                        poll_interval=self.poke_interval,
                        gcp_conn_id=self.gcp_conn_id,
                        hook_params={
                            "impersonation_chain": self.impersonation_chain,
                        },
                    ),
                    method_name="execute_complete",
                )

    def execute_complete(self, context: dict[str, Any], event: dict[str, str] | None = None) -> str:
        """
        Act as a callback for when the trigger fires - returns immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was successful.
        """
        table_uri = f"{self.project_id}:{self.dataset_id}.{self.table_id}"
        self.log.info("Sensor checks existence of table: %s", table_uri)
        if event:
            if event["status"] == "success":
                return event["message"]
            raise AirflowException(event["message"])

        message = "No event received in trigger callback"
        raise AirflowException(message)


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
        "project_id",
        "dataset_id",
        "table_id",
        "partition_id",
        "impersonation_chain",
    )
    ui_color = "#f0eee4"

    def __init__(
        self,
        *,
        project_id: str,
        dataset_id: str,
        table_id: str,
        partition_id: str,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        if deferrable and "poke_interval" not in kwargs:
            kwargs["poke_interval"] = 5
        super().__init__(**kwargs)

        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.partition_id = partition_id
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

        self.deferrable = deferrable

    def poke(self, context: Context) -> bool:
        table_uri = f"{self.project_id}:{self.dataset_id}.{self.table_id}"
        self.log.info('Sensor checks existence of partition: "%s" in table: %s', self.partition_id, table_uri)
        hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        return hook.table_partition_exists(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            table_id=self.table_id,
            partition_id=self.partition_id,
        )

    def execute(self, context: Context) -> None:
        """Airflow runs this method on the worker and defers using the triggers if deferrable is True."""
        if not self.deferrable:
            super().execute(context)
        else:
            if not self.poke(context=context):
                self.defer(
                    timeout=timedelta(seconds=self.timeout),
                    trigger=BigQueryTablePartitionExistenceTrigger(
                        dataset_id=self.dataset_id,
                        table_id=self.table_id,
                        project_id=self.project_id,
                        partition_id=self.partition_id,
                        poll_interval=self.poke_interval,
                        gcp_conn_id=self.gcp_conn_id,
                        hook_params={
                            "impersonation_chain": self.impersonation_chain,
                        },
                    ),
                    method_name="execute_complete",
                )

    def execute_complete(self, context: dict[str, Any], event: dict[str, str] | None = None) -> str:
        """
        Act as a callback for when the trigger fires - returns immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was successful.
        """
        table_uri = f"{self.project_id}:{self.dataset_id}.{self.table_id}"
        self.log.info('Sensor checks existence of partition: "%s" in table: %s', self.partition_id, table_uri)
        if event:
            if event["status"] == "success":
                return event["message"]

            raise AirflowException(event["message"])

        message = "No event received in trigger callback"
        raise AirflowException(message)


class BigQueryStreamingBufferEmptySensor(BaseSensorOperator):
    """
    Sensor for checking whether the streaming buffer in a BigQuery table is empty.

        The BigQueryStreamingBufferEmptySensor waits for the streaming buffer in a specified
        BigQuery table to be empty before proceeding. It can be used in ETL pipelines to ensure
        that recent streamed data has been processed before continuing downstream tasks.

        :ivar template_fields: Fields that can be templated in this operator.
        :type template_fields: Sequence[str]
        :ivar ui_color: Color of the operator in the Airflow UI.
        :type ui_color: Str
        :ivar project_id: The Google Cloud project ID where the BigQuery table resides.
        :type project_id: Str
        :ivar dataset_id: The ID of the dataset containing the BigQuery table.
        :type dataset_id: Str
        :ivar table_id: The ID of the BigQuery table to monitor.
        :type table_id: Str
        :ivar gcp_conn_id: The Airflow connection ID for GCP. Defaults to "google_cloud_default".
        :type gcp_conn_id: Str
        :ivar impersonation_chain: Optional array or string of service accounts to impersonate using short-term
                                   credentials. If multiple accounts are provided, the service account must
                                   grant the role `roles/iam.serviceAccountTokenCreator` on the next account
                                   in the chain.
        :type impersonation_chain: Str | Sequence[str] | None
        :ivar deferrable: Indicates whether the operator supports deferrable execution. If True, the sensor
                          can defer instead of polling, leading to reduced resource use.
        :type deferrable: Bool
    """

    template_fields: Sequence[str] = (
        "project_id",
        "dataset_id",
        "table_id",
        "impersonation_chain",
    )

    ui_color = "#f0eee4"

    def __init__(
        self,
        *,
        project_id: str,
        dataset_id: str,
        table_id: str,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        if deferrable and "poke_interval" not in kwargs:
            kwargs["poke_interval"] = 30

        super().__init__(**kwargs)

        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.deferrable = deferrable
    def execute(self, context: Context) -> None:
        """
        Executes the operator logic taking into account the `deferrable` attribute.
        If not deferrable, it uses the base class `execute` method; otherwise, it
        sets up deferral with a specific trigger to wait until the BigQuery Streaming
        Buffer is empty.

        :param context: The execution context provided by Airflow. It allows
            access to metadata and runtime information of the task instance.
        :type context: Context
        :return: None. The method does not return anything but performs actions
            relevant to the operator's execution.
        """
        if not self.deferrable:
            super().execute(context)
        else:
            if not self.poke(context=context):
                self.defer(
                    timeout=timedelta(seconds=self.timeout),
                    trigger=BigQueryStreamingBufferEmptyTrigger(
                        project_id=self.project_id,
                        dataset_id=self.dataset_id,
                        table_id=self.table_id,
                        gcp_conn_id=self.gcp_conn_id,
                        impersonation_chain=self.impersonation_chain,
                    ),
                    method_name="execute_complete",
                )

    def poke(self, context: Context) -> bool:
        """
        Check if the BigQuery streaming buffer is empty for the specified table.

        This method periodically checks the status of the BigQuery table's streaming buffer
        to determine if it is empty. It is useful for ensuring that recent streamed data
        has been fully processed before continuing with downstream tasks.
        """
        table_uri = f"{self.project_id}:{self.dataset_id}.{self.table_id}"
        self.log.info("Checking streaming buffer state for table: %s", table_uri)

        hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        try:
            table = hook.get_table(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                table_id=self.table_id,
            )
            return table.get("streamingBuffer") is None
        except Exception as err:
            if "not found" in str(err):
                raise AirflowException(f"Table {self.project_id}.{self.dataset_id}.{self.table_id} not found") from err
            raise err



