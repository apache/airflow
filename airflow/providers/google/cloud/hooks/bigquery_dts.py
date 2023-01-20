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
"""This module contains a BigQuery Hook."""
from __future__ import annotations

from copy import copy
from typing import Sequence

from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.api_core.retry import Retry
from google.cloud.bigquery_datatransfer_v1 import DataTransferServiceAsyncClient, DataTransferServiceClient
from google.cloud.bigquery_datatransfer_v1.types import (
    StartManualTransferRunsResponse,
    TransferConfig,
    TransferRun,
)
from googleapiclient.discovery import Resource

from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import (
    PROVIDE_PROJECT_ID,
    GoogleBaseAsyncHook,
    GoogleBaseHook,
)


def get_object_id(obj: dict) -> str:
    """Returns unique id of the object."""
    return obj["name"].rpartition("/")[-1]


class BiqQueryDataTransferServiceHook(GoogleBaseHook):
    """
    Hook for Google Bigquery Transfer API.

    All the methods in the hook where ``project_id`` is used must be called with
    keyword arguments rather than positional.
    """

    _conn: Resource | None = None

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        location: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )
        self.location = location

    @staticmethod
    def _disable_auto_scheduling(config: dict | TransferConfig) -> TransferConfig:
        """
        In the case of Airflow, the customer needs to create a transfer config
        with the automatic scheduling disabled (UI, CLI or an Airflow operator) and
        then trigger a transfer run using a specialized Airflow operator that will
        call start_manual_transfer_runs.

        :param config: Data transfer configuration to create.
        """
        config = TransferConfig.to_dict(config) if isinstance(config, TransferConfig) else config
        new_config = copy(config)
        schedule_options = new_config.get("schedule_options")
        if schedule_options:
            disable_auto_scheduling = schedule_options.get("disable_auto_scheduling", None)
            if disable_auto_scheduling is None:
                schedule_options["disable_auto_scheduling"] = True
        else:
            new_config["schedule_options"] = {"disable_auto_scheduling": True}

        return TransferConfig(**new_config)

    def get_conn(self) -> DataTransferServiceClient:
        """
        Retrieves connection to Google Bigquery.

        :return: Google Bigquery API client
        """
        if not self._conn:
            self._conn = DataTransferServiceClient(
                credentials=self.get_credentials(), client_info=CLIENT_INFO
            )
        return self._conn

    @GoogleBaseHook.fallback_to_default_project_id
    def create_transfer_config(
        self,
        transfer_config: dict | TransferConfig,
        project_id: str = PROVIDE_PROJECT_ID,
        authorization_code: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> TransferConfig:
        """
        Creates a new data transfer configuration.

        :param transfer_config: Data transfer configuration to create.
        :param project_id: The BigQuery project id where the transfer configuration should be
            created. If set to None or missing, the default project_id from the Google Cloud connection
            is used.
        :param authorization_code: authorization code to use with this transfer configuration.
            This is required if new credentials are needed.
        :param retry: A retry object used to retry requests. If `None` is
            specified, requests will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request to
            complete. Note that if retry is specified, the timeout applies to each individual
            attempt.
        :param metadata: Additional metadata that is provided to the method.
        :return: A ``google.cloud.bigquery_datatransfer_v1.types.TransferConfig`` instance.
        """
        client = self.get_conn()
        parent = f"projects/{project_id}"
        if self.location:
            parent = f"{parent}/locations/{self.location}"

        return client.create_transfer_config(
            request={
                "parent": parent,
                "transfer_config": self._disable_auto_scheduling(transfer_config),
                "authorization_code": authorization_code,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_transfer_config(
        self,
        transfer_config_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        """
        Deletes transfer configuration.

        :param transfer_config_id: Id of transfer config to be used.
        :param project_id: The BigQuery project id where the transfer configuration should be
            created. If set to None or missing, the default project_id from the Google Cloud connection
            is used.
        :param retry: A retry object used to retry requests. If `None` is
            specified, requests will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request to
            complete. Note that if retry is specified, the timeout applies to each individual
            attempt.
        :param metadata: Additional metadata that is provided to the method.
        :return: None
        """
        client = self.get_conn()
        project = f"projects/{project_id}"
        if self.location:
            project = f"{project}/locations/{self.location}"

        name = f"{project}/transferConfigs/{transfer_config_id}"
        return client.delete_transfer_config(
            request={"name": name}, retry=retry, timeout=timeout, metadata=metadata or ()
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def start_manual_transfer_runs(
        self,
        transfer_config_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
        requested_time_range: dict | None = None,
        requested_run_time: dict | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> StartManualTransferRunsResponse:
        """
        Start manual transfer runs to be executed now with schedule_time equal
        to current time. The transfer runs can be created for a time range where
        the run_time is between start_time (inclusive) and end_time
        (exclusive), or for a specific run_time.

        :param transfer_config_id: Id of transfer config to be used.
        :param requested_time_range: Time range for the transfer runs that should be started.
            If a dict is provided, it must be of the same form as the protobuf
            message `~google.cloud.bigquery_datatransfer_v1.types.TimeRange`
        :param requested_run_time: Specific run_time for a transfer run to be started. The
            requested_run_time must not be in the future.  If a dict is provided, it
            must be of the same form as the protobuf message
            `~google.cloud.bigquery_datatransfer_v1.types.Timestamp`
        :param project_id: The BigQuery project id where the transfer configuration should be
            created. If set to None or missing, the default project_id from the Google Cloud connection
            is used.
        :param retry: A retry object used to retry requests. If `None` is
            specified, requests will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request to
            complete. Note that if retry is specified, the timeout applies to each individual
            attempt.
        :param metadata: Additional metadata that is provided to the method.
        :return: An ``google.cloud.bigquery_datatransfer_v1.types.StartManualTransferRunsResponse`` instance.
        """
        client = self.get_conn()
        project = f"projects/{project_id}"
        if self.location:
            project = f"{project}/locations/{self.location}"

        parent = f"{project}/transferConfigs/{transfer_config_id}"
        return client.start_manual_transfer_runs(
            request={
                "parent": parent,
                "requested_time_range": requested_time_range,
                "requested_run_time": requested_run_time,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def get_transfer_run(
        self,
        run_id: str,
        transfer_config_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> TransferRun:
        """
        Returns information about the particular transfer run.

        :param run_id: ID of the transfer run.
        :param transfer_config_id: ID of transfer config to be used.
        :param project_id: The BigQuery project id where the transfer configuration should be
            created. If set to None or missing, the default project_id from the Google Cloud connection
            is used.
        :param retry: A retry object used to retry requests. If `None` is
            specified, requests will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request to
            complete. Note that if retry is specified, the timeout applies to each individual
            attempt.
        :param metadata: Additional metadata that is provided to the method.
        :return: An ``google.cloud.bigquery_datatransfer_v1.types.TransferRun`` instance.
        """
        client = self.get_conn()
        project = f"projects/{project_id}"
        if self.location:
            project = f"{project}/locations/{self.location}"

        name = f"{project}/transferConfigs/{transfer_config_id}/runs/{run_id}"
        return client.get_transfer_run(
            request={"name": name}, retry=retry, timeout=timeout, metadata=metadata or ()
        )


class AsyncBiqQueryDataTransferServiceHook(GoogleBaseAsyncHook):
    """Hook of the BigQuery service to be used with async client of the Google library."""

    sync_hook_class = BiqQueryDataTransferServiceHook

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        location: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
    ):
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            location=location,
            impersonation_chain=impersonation_chain,
        )
        self._conn: DataTransferServiceAsyncClient | None = None

    async def _get_conn(self) -> DataTransferServiceAsyncClient:
        if not self._conn:
            credentials = (await self.get_sync_hook()).get_credentials()
            self._conn = DataTransferServiceAsyncClient(credentials=credentials, client_info=CLIENT_INFO)
        return self._conn

    async def _get_project_id(self) -> str:
        sync_hook = await self.get_sync_hook()
        return sync_hook.project_id

    async def get_transfer_run(
        self,
        config_id: str,
        run_id: str,
        project_id: str | None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Returns information about the particular transfer run.

        :param run_id: ID of the transfer run.
        :param config_id: ID of transfer config to be used.
        :param project_id: The BigQuery project id where the transfer configuration should be
            created. If set to None or missing, the default project_id from the Google Cloud connection
            is used.
        :param retry: A retry object used to retry requests. If `None` is
            specified, requests will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request to
            complete. Note that if retry is specified, the timeout applies to each individual
            attempt.
        :param metadata: Additional metadata that is provided to the method.
        :return: An ``google.cloud.bigquery_datatransfer_v1.types.TransferRun`` instance.
        """
        project_id = project_id or (await self._get_project_id())
        client = await self._get_conn()
        name = f"projects/{project_id}/transferConfigs/{config_id}/runs/{run_id}"
        transfer_run = await client.get_transfer_run(
            name=name,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return transfer_run
