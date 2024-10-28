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
"""This module contains Google Dataplex hook."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Sequence

from google.api_core.client_options import ClientOptions
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.cloud.dataplex_v1 import (
    DataplexServiceClient,
    DataScanServiceAsyncClient,
    DataScanServiceClient,
)
from google.cloud.dataplex_v1.types import (
    Asset,
    DataScan,
    DataScanJob,
    Lake,
    Task,
    Zone,
)
from google.protobuf.field_mask_pb2 import FieldMask

from airflow.exceptions import AirflowException
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import (
    PROVIDE_PROJECT_ID,
    GoogleBaseAsyncHook,
    GoogleBaseHook,
)

if TYPE_CHECKING:
    from google.api_core.operation import Operation
    from google.api_core.retry import Retry
    from google.api_core.retry_async import AsyncRetry
    from googleapiclient.discovery import Resource

PATH_DATA_SCAN = "projects/{project_id}/locations/{region}/dataScans/{data_scan_id}"


class AirflowDataQualityScanException(AirflowException):
    """Raised when data quality scan rules fail."""


class AirflowDataQualityScanResultTimeoutException(AirflowException):
    """Raised when no result found after specified amount of seconds."""


class DataplexHook(GoogleBaseHook):
    """
    Hook for Google Dataplex.

    :param api_version: The version of the api that will be requested for example 'v3'.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    _conn: Resource | None = None

    def __init__(
        self,
        api_version: str = "v1",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        location: str | None = None,
        **kwargs,
    ) -> None:
        if kwargs.get("delegate_to") is not None:
            raise RuntimeError(
                "The `delegate_to` parameter has been deprecated before and finally removed in this version"
                " of Google Provider. You MUST convert it to `impersonate_chain`"
            )
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
        )
        self.api_version = api_version
        self.location = location

    def get_dataplex_client(self) -> DataplexServiceClient:
        """Return DataplexServiceClient."""
        client_options = ClientOptions(api_endpoint="dataplex.googleapis.com:443")

        return DataplexServiceClient(
            credentials=self.get_credentials(),
            client_info=CLIENT_INFO,
            client_options=client_options,
        )

    def get_dataplex_data_scan_client(self) -> DataScanServiceClient:
        """Return DataScanServiceClient."""
        client_options = ClientOptions(api_endpoint="dataplex.googleapis.com:443")

        return DataScanServiceClient(
            credentials=self.get_credentials(),
            client_info=CLIENT_INFO,
            client_options=client_options,
        )

    def wait_for_operation(self, timeout: float | None, operation: Operation):
        """Wait for long-lasting operation to complete."""
        try:
            return operation.result(timeout=timeout)
        except Exception:
            error = operation.exception(timeout=timeout)
            raise AirflowException(error)

    @GoogleBaseHook.fallback_to_default_project_id
    def create_task(
        self,
        project_id: str,
        region: str,
        lake_id: str,
        body: dict[str, Any] | Task,
        dataplex_task_id: str,
        validate_only: bool | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Any:
        """
        Create a task resource within a lake.

        :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
        :param region: Required. The ID of the Google Cloud region that the task belongs to.
        :param lake_id: Required. The ID of the Google Cloud lake that the task belongs to.
        :param body: Required. The Request body contains an instance of Task.
        :param dataplex_task_id: Required. Task identifier.
        :param validate_only: Optional. Only validate the request, but do not perform mutations.
            The default is false.
        :param retry: A retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        parent = f"projects/{project_id}/locations/{region}/lakes/{lake_id}"

        client = self.get_dataplex_client()
        result = client.create_task(
            request={
                "parent": parent,
                "task_id": dataplex_task_id,
                "task": body,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_task(
        self,
        project_id: str,
        region: str,
        lake_id: str,
        dataplex_task_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Any:
        """
        Delete the task resource.

        :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
        :param region: Required. The ID of the Google Cloud region that the task belongs to.
        :param lake_id: Required. The ID of the Google Cloud lake that the task belongs to.
        :param dataplex_task_id: Required. The ID of the Google Cloud task to be deleted.
        :param retry: A retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        name = f"projects/{project_id}/locations/{region}/lakes/{lake_id}/tasks/{dataplex_task_id}"

        client = self.get_dataplex_client()
        result = client.delete_task(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def list_tasks(
        self,
        project_id: str,
        region: str,
        lake_id: str,
        page_size: int | None = None,
        page_token: str | None = None,
        filter: str | None = None,
        order_by: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Any:
        """
        List tasks under the given lake.

        :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
        :param region: Required. The ID of the Google Cloud region that the task belongs to.
        :param lake_id: Required. The ID of the Google Cloud lake that the task belongs to.
        :param page_size: Optional. Maximum number of tasks to return. The service may return fewer than this
            value. If unspecified, at most 10 tasks will be returned. The maximum value is 1000;
            values above 1000 will be coerced to 1000.
        :param page_token: Optional. Page token received from a previous ListZones call. Provide this to
            retrieve the subsequent page. When paginating, all other parameters provided to ListZones must
            match the call that provided the page token.
        :param filter: Optional. Filter request.
        :param order_by: Optional. Order by fields for the result.
        :param retry: A retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        parent = f"projects/{project_id}/locations/{region}/lakes/{lake_id}"

        client = self.get_dataplex_client()
        result = client.list_tasks(
            request={
                "parent": parent,
                "page_size": page_size,
                "page_token": page_token,
                "filter": filter,
                "order_by": order_by,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def get_task(
        self,
        project_id: str,
        region: str,
        lake_id: str,
        dataplex_task_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Any:
        """
        Get task resource.

        :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
        :param region: Required. The ID of the Google Cloud region that the task belongs to.
        :param lake_id: Required. The ID of the Google Cloud lake that the task belongs to.
        :param dataplex_task_id: Required. The ID of the Google Cloud task to be retrieved.
        :param retry: A retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        name = f"projects/{project_id}/locations/{region}/lakes/{lake_id}/tasks/{dataplex_task_id}"
        client = self.get_dataplex_client()
        result = client.get_task(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_lake(
        self,
        project_id: str,
        region: str,
        lake_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Any:
        """
         Delete the lake resource.

        :param project_id: Required. The ID of the Google Cloud project that the lake belongs to.
        :param region: Required. The ID of the Google Cloud region that the lake belongs to.
        :param lake_id: Required. The ID of the Google Cloud lake to be deleted.
        :param retry: A retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        name = f"projects/{project_id}/locations/{region}/lakes/{lake_id}"

        client = self.get_dataplex_client()
        result = client.delete_lake(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def create_lake(
        self,
        project_id: str,
        region: str,
        lake_id: str,
        body: dict[str, Any] | Lake,
        validate_only: bool | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Any:
        """
        Create a lake resource.

        :param project_id: Required. The ID of the Google Cloud project that the lake belongs to.
        :param region: Required. The ID of the Google Cloud region that the lake belongs to.
        :param lake_id: Required. Lake identifier.
        :param body: Required. The Request body contains an instance of Lake.
        :param validate_only: Optional. Only validate the request, but do not perform mutations.
            The default is false.
        :param retry: A retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        parent = f"projects/{project_id}/locations/{region}"
        client = self.get_dataplex_client()
        result = client.create_lake(
            request={
                "parent": parent,
                "lake_id": lake_id,
                "lake": body,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def get_lake(
        self,
        project_id: str,
        region: str,
        lake_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Any:
        """
        Get lake resource.

        :param project_id: Required. The ID of the Google Cloud project that the lake belongs to.
        :param region: Required. The ID of the Google Cloud region that the lake belongs to.
        :param lake_id: Required. The ID of the Google Cloud lake to be retrieved.
        :param retry: A retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        name = f"projects/{project_id}/locations/{region}/lakes/{lake_id}/"
        client = self.get_dataplex_client()
        result = client.get_lake(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def create_zone(
        self,
        project_id: str,
        region: str,
        lake_id: str,
        zone_id: str,
        body: dict[str, Any] | Zone,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Any:
        """
        Create a zone resource within a lake.

        :param project_id: Required. The ID of the Google Cloud project that the lake belongs to.
        :param region: Required. The ID of the Google Cloud region that the lake belongs to.
        :param lake_id: Required. The ID of the Google Cloud lake to be retrieved.
        :param body: Required. The Request body contains an instance of Zone.
        :param zone_id: Required. Zone identifier.
        :param retry: A retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_dataplex_client()

        name = f"projects/{project_id}/locations/{region}/lakes/{lake_id}"
        result = client.create_zone(
            request={
                "parent": name,
                "zone": body,
                "zone_id": zone_id,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_zone(
        self,
        project_id: str,
        region: str,
        lake_id: str,
        zone_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Any:
        """
        Delete a zone resource. All assets within a zone must be deleted before the zone can be deleted.

        :param project_id: Required. The ID of the Google Cloud project that the lake belongs to.
        :param region: Required. The ID of the Google Cloud region that the lake belongs to.
        :param lake_id: Required. The ID of the Google Cloud lake to be retrieved.
        :param zone_id: Required. Zone identifier.
        :param retry: A retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_dataplex_client()

        name = f"projects/{project_id}/locations/{region}/lakes/{lake_id}/zones/{zone_id}"
        operation = client.delete_zone(
            request={"name": name},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return operation

    @GoogleBaseHook.fallback_to_default_project_id
    def create_asset(
        self,
        project_id: str,
        region: str,
        lake_id: str,
        zone_id: str,
        asset_id: str,
        body: dict[str, Any] | Asset,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Any:
        """
        Create an asset resource.

        :param project_id: Required. The ID of the Google Cloud project that the lake belongs to.
        :param region: Required. The ID of the Google Cloud region that the lake belongs to.
        :param lake_id: Required. The ID of the Google Cloud lake to be retrieved.
        :param zone_id: Required. Zone identifier.
        :param asset_id: Required. Asset identifier.
        :param body: Required. The Request body contains an instance of Asset.
        :param retry: A retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_dataplex_client()

        name = f"projects/{project_id}/locations/{region}/lakes/{lake_id}/zones/{zone_id}"
        result = client.create_asset(
            request={
                "parent": name,
                "asset": body,
                "asset_id": asset_id,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_asset(
        self,
        project_id: str,
        region: str,
        lake_id: str,
        asset_id: str,
        zone_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Any:
        """
        Delete an asset resource.

        :param project_id: Required. The ID of the Google Cloud project that the lake belongs to.
        :param region: Required. The ID of the Google Cloud region that the lake belongs to.
        :param lake_id: Required. The ID of the Google Cloud lake to be retrieved.
        :param zone_id: Required. Zone identifier.
        :param asset_id: Required. Asset identifier.
        :param retry: A retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_dataplex_client()

        name = f"projects/{project_id}/locations/{region}/lakes/{lake_id}/zones/{zone_id}/assets/{asset_id}"
        result = client.delete_asset(
            request={"name": name},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def create_data_scan(
        self,
        project_id: str,
        region: str,
        body: dict[str, Any] | DataScan,
        data_scan_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Any:
        """
        Create a DataScan resource.

        :param project_id: Required. The ID of the Google Cloud project that the lake belongs to.
        :param region: Required. The ID of the Google Cloud region that the lake belongs to.
        :param data_scan_id: Required. Data Quality scan identifier.
        :param body: Required. The Request body contains an instance of DataScan.
        :param retry: A retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_dataplex_data_scan_client()

        parent = f"projects/{project_id}/locations/{region}"
        result = client.create_data_scan(
            request={
                "parent": parent,
                "data_scan": body,
                "data_scan_id": data_scan_id,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def run_data_scan(
        self,
        project_id: str,
        region: str,
        data_scan_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Any:
        """
        Run an on-demand execution of a DataScan.

        :param project_id: Required. The ID of the Google Cloud project that the lake belongs to.
        :param region: Required. The ID of the Google Cloud region that the lake belongs to.
        :param data_scan_id: Required. Data Quality scan identifier.
        :param retry: A retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_dataplex_data_scan_client()

        name = PATH_DATA_SCAN.format(
            project_id=project_id, region=region, data_scan_id=data_scan_id
        )
        result = client.run_data_scan(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def get_data_scan_job(
        self,
        project_id: str,
        region: str,
        data_scan_id: str | None = None,
        job_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Any:
        """
        Get a DataScan Job resource.

        :param project_id: Required. The ID of the Google Cloud project that the lake belongs to.
        :param region: Required. The ID of the Google Cloud region that the lake belongs to.
        :param data_scan_id: Required. Data Quality scan identifier.
        :param job_id: Required. The resource name of the DataScanJob:
            projects/{project_id}/locations/{region}/dataScans/{data_scan_id}/jobs/{data_scan_job_id}
        :param retry: A retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_dataplex_data_scan_client()

        name = f"projects/{project_id}/locations/{region}/dataScans/{data_scan_id}/jobs/{job_id}"
        result = client.get_data_scan_job(
            request={"name": name, "view": "FULL"},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    def wait_for_data_scan_job(
        self,
        data_scan_id: str,
        job_id: str | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
        region: str | None = None,
        wait_time: int = 10,
        result_timeout: float | None = None,
    ) -> Any:
        """
        Wait for Dataplex data scan job.

        :param job_id: Required. The job_id to wait for.
        :param data_scan_id: Required. Data Quality scan identifier.
        :param region: Required. The ID of the Google Cloud region that the lake belongs to.
        :param project_id: Optional. Google Cloud project ID.
        :param wait_time: Number of seconds between checks.
        :param result_timeout: Value in seconds for which operator will wait for the Data Quality scan result.
            Throws exception if there is no result found after specified amount of seconds.
        """
        start = time.monotonic()
        state = None
        while state not in (
            DataScanJob.State.CANCELLED,
            DataScanJob.State.FAILED,
            DataScanJob.State.SUCCEEDED,
        ):
            if result_timeout and start + result_timeout < time.monotonic():
                raise AirflowDataQualityScanResultTimeoutException(
                    f"Timeout: Data Quality scan {job_id} is not ready after {result_timeout}s"
                )
            time.sleep(wait_time)
            try:
                job = self.get_data_scan_job(
                    job_id=job_id,
                    data_scan_id=data_scan_id,
                    project_id=project_id,
                    region=region,
                )
                state = job.state
            except Exception as err:
                self.log.info(
                    "Retrying. Dataplex API returned error when waiting for job: %s", err
                )
        return job

    @GoogleBaseHook.fallback_to_default_project_id
    def get_data_scan(
        self,
        project_id: str,
        region: str,
        data_scan_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Any:
        """
        Get a DataScan resource.

        :param project_id: Required. The ID of the Google Cloud project that the lake belongs to.
        :param region: Required. The ID of the Google Cloud region that the lake belongs to.
        :param data_scan_id: Required. Data Quality scan identifier.
        :param retry: A retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_dataplex_data_scan_client()

        name = PATH_DATA_SCAN.format(
            project_id=project_id, region=region, data_scan_id=data_scan_id
        )
        result = client.get_data_scan(
            request={"name": name, "view": "FULL"},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def update_data_scan(
        self,
        project_id: str,
        region: str,
        data_scan_id: str,
        body: dict[str, Any] | DataScan,
        update_mask: dict | FieldMask | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Any:
        """
        Update a DataScan resource.

        :param project_id: Required. The ID of the Google Cloud project that the lake belongs to.
        :param region: Required. The ID of the Google Cloud region that the lake belongs to.
        :param data_scan_id: Required. Data Quality scan identifier.
        :param body: Required. The Request body contains an instance of DataScan.
        :param update_mask: Required. Mask of fields to update.
        :param retry: A retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_dataplex_data_scan_client()

        full_scan_name = (
            f"projects/{project_id}/locations/{region}/dataScans/{data_scan_id}"
        )

        if body:
            if isinstance(body, DataScan):
                body.name = full_scan_name
            elif isinstance(body, dict):
                body["name"] = full_scan_name
            else:
                raise AirflowException("Unable to set scan_name.")

        if not update_mask:
            update_mask = FieldMask(
                paths=[
                    "data_quality_spec",
                    "labels",
                    "description",
                    "display_name",
                    "execution_spec",
                ]
            )

        result = client.update_data_scan(
            request={
                "data_scan": body,
                "update_mask": update_mask,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_data_scan(
        self,
        project_id: str,
        region: str,
        data_scan_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Any:
        """
        Delete a DataScan resource.

        :param project_id: Required. The ID of the Google Cloud project that the lake belongs to.
        :param region: Required. The ID of the Google Cloud region that the lake belongs to.
        :param data_scan_id: Required. Data Quality scan identifier.
        :param retry: A retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_dataplex_data_scan_client()

        name = PATH_DATA_SCAN.format(
            project_id=project_id, region=region, data_scan_id=data_scan_id
        )
        result = client.delete_data_scan(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def list_data_scan_jobs(
        self,
        project_id: str,
        region: str,
        data_scan_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Any:
        """
        List DataScanJobs under the given DataScan.

        :param project_id: Required. The ID of the Google Cloud project that the lake belongs to.
        :param region: Required. The ID of the Google Cloud region that the lake belongs to.
        :param data_scan_id: Required. Data Quality scan identifier.
        :param retry: A retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_dataplex_data_scan_client()

        name = PATH_DATA_SCAN.format(
            project_id=project_id, region=region, data_scan_id=data_scan_id
        )
        result = client.list_data_scan_jobs(
            request={
                "parent": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result


class DataplexAsyncHook(GoogleBaseAsyncHook):
    """
    Asynchronous Hook for Google Cloud Dataplex APIs.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.
    """

    sync_hook_class = DataplexHook

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(gcp_conn_id=gcp_conn_id, impersonation_chain=impersonation_chain)

    async def get_dataplex_data_scan_client(self) -> DataScanServiceAsyncClient:
        """Return DataScanServiceAsyncClient."""
        client_options = ClientOptions(api_endpoint="dataplex.googleapis.com:443")

        return DataScanServiceAsyncClient(
            credentials=(await self.get_sync_hook()).get_credentials(),
            client_info=CLIENT_INFO,
            client_options=client_options,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    async def get_data_scan_job(
        self,
        project_id: str,
        region: str,
        data_scan_id: str | None = None,
        job_id: str | None = None,
        retry: AsyncRetry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Any:
        """
        Get a DataScan Job resource.

        :param project_id: Required. The ID of the Google Cloud project that the lake belongs to.
        :param region: Required. The ID of the Google Cloud region that the lake belongs to.
        :param data_scan_id: Required. DataScan identifier.
        :param job_id: Required. The resource name of the DataScanJob:
            projects/{project_id}/locations/{region}/dataScans/{data_scan_id}/jobs/{data_scan_job_id}
        :param retry: A retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = await self.get_dataplex_data_scan_client()

        name = f"projects/{project_id}/locations/{region}/dataScans/{data_scan_id}/jobs/{job_id}"
        result = await client.get_data_scan_job(
            request={"name": name, "view": "FULL"},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        return result
