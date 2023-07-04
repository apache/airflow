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

from typing import Any, Sequence

from google.api_core.client_options import ClientOptions
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.api_core.operation import Operation
from google.api_core.retry import Retry
from google.cloud.dataplex_v1 import DataplexServiceClient
from google.cloud.dataplex_v1.types import Lake, Task
from googleapiclient.discovery import Resource

from airflow.exceptions import AirflowException
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


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

    def get_dataplex_client(self) -> DataplexServiceClient:
        """Returns DataplexServiceClient."""
        client_options = ClientOptions(api_endpoint="dataplex.googleapis.com:443")

        return DataplexServiceClient(
            credentials=self.get_credentials(), client_info=CLIENT_INFO, client_options=client_options
        )

    def wait_for_operation(self, timeout: float | None, operation: Operation):
        """Waits for long-lasting operation to complete."""
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
        Creates a task resource within a lake.

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
        Lists tasks under the given lake.

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
        Creates a lake resource.

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
