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
#
"""
This module contains a BigQuery Hook.
"""
from copy import copy
from typing import Optional, Sequence, Tuple, Union

from google.api_core.retry import Retry
from google.cloud.bigquery_datatransfer_v1 import DataTransferServiceClient
from google.cloud.bigquery_datatransfer_v1.types import (
    StartManualTransferRunsResponse,
    TransferConfig,
    TransferRun,
)
from google.protobuf.json_format import MessageToDict, ParseDict
from googleapiclient.discovery import Resource

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


def get_object_id(obj: dict) -> str:
    """
    Returns unique id of the object.
    """
    return obj["name"].rpartition("/")[-1]


class BiqQueryDataTransferServiceHook(GoogleBaseHook):
    """
    Hook for Google Bigquery Transfer API.

    All the methods in the hook where ``project_id`` is used must be called with
    keyword arguments rather than positional.
    """

    _conn = None  # type: Optional[Resource]

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )

    @staticmethod
    def _disable_auto_scheduling(config: Union[dict, TransferConfig]) -> TransferConfig:
        """
        In the case of Airflow, the customer needs to create a transfer config
        with the automatic scheduling disabled (UI, CLI or an Airflow operator) and
        then trigger a transfer run using a specialized Airflow operator that will
        call start_manual_transfer_runs.

        :param config: Data transfer configuration to create.
        :type config: Union[dict, google.cloud.bigquery_datatransfer_v1.types.TransferConfig]
        """
        config = MessageToDict(config) if isinstance(config, TransferConfig) else config
        new_config = copy(config)
        schedule_options = new_config.get("schedule_options")
        if schedule_options:
            disable_auto_scheduling = schedule_options.get("disable_auto_scheduling", None)
            if disable_auto_scheduling is None:
                schedule_options["disable_auto_scheduling"] = True
        else:
            new_config["schedule_options"] = {"disable_auto_scheduling": True}
        return ParseDict(new_config, TransferConfig())

    def get_conn(self) -> DataTransferServiceClient:
        """
        Retrieves connection to Google Bigquery.

        :return: Google Bigquery API client
        :rtype: google.cloud.bigquery_datatransfer_v1.DataTransferServiceClient
        """
        if not self._conn:
            self._conn = DataTransferServiceClient(
                credentials=self._get_credentials(), client_info=self.client_info
            )
        return self._conn

    @GoogleBaseHook.fallback_to_default_project_id
    def create_transfer_config(
        self,
        transfer_config: Union[dict, TransferConfig],
        project_id: str,
        authorization_code: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> TransferConfig:
        """
        Creates a new data transfer configuration.

        :param transfer_config: Data transfer configuration to create.
        :type transfer_config: Union[dict, google.cloud.bigquery_datatransfer_v1.types.TransferConfig]
        :param project_id: The BigQuery project id where the transfer configuration should be
            created. If set to None or missing, the default project_id from the Google Cloud connection
            is used.
        :type project_id: str
        :param authorization_code: authorization code to use with this transfer configuration.
            This is required if new credentials are needed.
        :type authorization_code: Optional[str]
        :param retry: A retry object used to retry requests. If `None` is
            specified, requests will not be retried.
        :type retry: Optional[google.api_core.retry.Retry]
        :param timeout: The amount of time, in seconds, to wait for the request to
            complete. Note that if retry is specified, the timeout applies to each individual
            attempt.
        :type timeout: Optional[float]
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Optional[Sequence[Tuple[str, str]]]
        :return: A ``google.cloud.bigquery_datatransfer_v1.types.TransferConfig`` instance.
        """
        client = self.get_conn()
        parent = client.project_path(project_id)
        return client.create_transfer_config(
            parent=parent,
            transfer_config=self._disable_auto_scheduling(transfer_config),
            authorization_code=authorization_code,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_transfer_config(
        self,
        transfer_config_id: str,
        project_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> None:
        """
        Deletes transfer configuration.

        :param transfer_config_id: Id of transfer config to be used.
        :type transfer_config_id: str
        :param project_id: The BigQuery project id where the transfer configuration should be
            created. If set to None or missing, the default project_id from the Google Cloud connection
            is used.
        :type project_id: str
        :param retry: A retry object used to retry requests. If `None` is
            specified, requests will not be retried.
        :type retry: Optional[google.api_core.retry.Retry]
        :param timeout: The amount of time, in seconds, to wait for the request to
            complete. Note that if retry is specified, the timeout applies to each individual
            attempt.
        :type timeout: Optional[float]
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Optional[Sequence[Tuple[str, str]]]
        :return: None
        """
        client = self.get_conn()
        name = client.project_transfer_config_path(project=project_id, transfer_config=transfer_config_id)
        return client.delete_transfer_config(name=name, retry=retry, timeout=timeout, metadata=metadata)

    @GoogleBaseHook.fallback_to_default_project_id
    def start_manual_transfer_runs(
        self,
        transfer_config_id: str,
        project_id: str,
        requested_time_range: Optional[dict] = None,
        requested_run_time: Optional[dict] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> StartManualTransferRunsResponse:
        """
        Start manual transfer runs to be executed now with schedule_time equal
        to current time. The transfer runs can be created for a time range where
        the run_time is between start_time (inclusive) and end_time
        (exclusive), or for a specific run_time.

        :param transfer_config_id: Id of transfer config to be used.
        :type transfer_config_id: str
        :param requested_time_range: Time range for the transfer runs that should be started.
            If a dict is provided, it must be of the same form as the protobuf
            message `~google.cloud.bigquery_datatransfer_v1.types.TimeRange`
        :type requested_time_range: Union[dict, ~google.cloud.bigquery_datatransfer_v1.types.TimeRange]
        :param requested_run_time: Specific run_time for a transfer run to be started. The
            requested_run_time must not be in the future.  If a dict is provided, it
            must be of the same form as the protobuf message
            `~google.cloud.bigquery_datatransfer_v1.types.Timestamp`
        :type requested_run_time: Union[dict, ~google.cloud.bigquery_datatransfer_v1.types.Timestamp]
        :param project_id: The BigQuery project id where the transfer configuration should be
            created. If set to None or missing, the default project_id from the Google Cloud connection
            is used.
        :type project_id: str
        :param retry: A retry object used to retry requests. If `None` is
            specified, requests will not be retried.
        :type retry: Optional[google.api_core.retry.Retry]
        :param timeout: The amount of time, in seconds, to wait for the request to
            complete. Note that if retry is specified, the timeout applies to each individual
            attempt.
        :type timeout: Optional[float]
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Optional[Sequence[Tuple[str, str]]]
        :return: An ``google.cloud.bigquery_datatransfer_v1.types.StartManualTransferRunsResponse`` instance.
        """
        client = self.get_conn()
        parent = client.project_transfer_config_path(project=project_id, transfer_config=transfer_config_id)
        return client.start_manual_transfer_runs(
            parent=parent,
            requested_time_range=requested_time_range,
            requested_run_time=requested_run_time,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def get_transfer_run(
        self,
        run_id: str,
        transfer_config_id: str,
        project_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> TransferRun:
        """
        Returns information about the particular transfer run.

        :param run_id: ID of the transfer run.
        :type run_id: str
        :param transfer_config_id: ID of transfer config to be used.
        :type transfer_config_id: str
        :param project_id: The BigQuery project id where the transfer configuration should be
            created. If set to None or missing, the default project_id from the Google Cloud connection
            is used.
        :type project_id: str
        :param retry: A retry object used to retry requests. If `None` is
            specified, requests will not be retried.
        :type retry: Optional[google.api_core.retry.Retry]
        :param timeout: The amount of time, in seconds, to wait for the request to
            complete. Note that if retry is specified, the timeout applies to each individual
            attempt.
        :type timeout: Optional[float]
        :param metadata: Additional metadata that is provided to the method.
        :type metadata: Optional[Sequence[Tuple[str, str]]]
        :return: An ``google.cloud.bigquery_datatransfer_v1.types.TransferRun`` instance.
        """
        client = self.get_conn()
        name = client.project_run_path(project=project_id, transfer_config=transfer_config_id, run=run_id)
        return client.get_transfer_run(name=name, retry=retry, timeout=timeout, metadata=metadata)
