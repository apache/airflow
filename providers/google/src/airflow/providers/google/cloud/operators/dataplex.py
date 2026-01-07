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
"""This module contains Google Dataplex operators."""

from __future__ import annotations

import time
from collections.abc import MutableSequence, Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any

from google.protobuf.json_format import MessageToDict

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.cloud.triggers.dataplex import (
    DataplexDataProfileJobTrigger,
    DataplexDataQualityJobTrigger,
)

if TYPE_CHECKING:
    from google.protobuf.field_mask_pb2 import FieldMask

    from airflow.providers.common.compat.sdk import Context

from google.api_core.exceptions import AlreadyExists, GoogleAPICallError, NotFound
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.api_core.retry import Retry, exponential_sleep_generator
from google.cloud.dataplex_v1.types import (
    AspectType,
    Asset,
    DataScan,
    DataScanJob,
    Entry,
    EntryGroup,
    EntryType,
    EntryView,
    Lake,
    ListAspectTypesResponse,
    ListEntriesResponse,
    ListEntryGroupsResponse,
    ListEntryTypesResponse,
    SearchEntriesResponse,
    Task,
    Zone,
)
from googleapiclient.errors import HttpError

from airflow.providers.common.compat.sdk import conf
from airflow.providers.google.cloud.hooks.dataplex import AirflowDataQualityScanException, DataplexHook
from airflow.providers.google.cloud.links.dataplex import (
    DataplexCatalogAspectTypeLink,
    DataplexCatalogAspectTypesLink,
    DataplexCatalogEntryGroupLink,
    DataplexCatalogEntryGroupsLink,
    DataplexCatalogEntryLink,
    DataplexCatalogEntryTypeLink,
    DataplexCatalogEntryTypesLink,
    DataplexLakeLink,
    DataplexTaskLink,
    DataplexTasksLink,
)
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator


class DataplexCreateTaskOperator(GoogleCloudBaseOperator):
    """
    Creates a task resource within a lake.

    :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
    :param region: Required. The ID of the Google Cloud region that the task belongs to.
    :param lake_id: Required. The ID of the Google Cloud lake that the task belongs to.
    :param body:  Required. The Request body contains an instance of Task.
    :param dataplex_task_id: Required. Task identifier.
    :param validate_only: Optional. Only validate the request, but do not perform mutations. The default is
        false.
    :param api_version: The version of the api that will be requested for example 'v3'.
    :param retry: A retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param asynchronous: Flag informing should the Dataplex task be created asynchronously.
        This is useful for long running creating tasks and
        waiting on them asynchronously using the DataplexTaskSensor
    """

    template_fields = (
        "project_id",
        "dataplex_task_id",
        "body",
        "validate_only",
        "impersonation_chain",
    )
    template_fields_renderers = {"body": "json"}
    operator_extra_links = (DataplexTaskLink(),)

    def __init__(
        self,
        project_id: str,
        region: str,
        lake_id: str,
        body: dict[str, Any],
        dataplex_task_id: str,
        validate_only: bool | None = None,
        api_version: str = "v1",
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        asynchronous: bool = False,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.region = region
        self.lake_id = lake_id
        self.body = body
        self.dataplex_task_id = dataplex_task_id
        self.validate_only = validate_only
        self.api_version = api_version
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.asynchronous = asynchronous

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            "lake_id": self.lake_id,
            "task_id": self.dataplex_task_id,
            "region": self.region,
            "project_id": self.project_id,
        }

    def execute(self, context: Context) -> dict:
        hook = DataplexHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Creating Dataplex task %s", self.dataplex_task_id)
        DataplexTaskLink.persist(context=context)

        try:
            operation = hook.create_task(
                project_id=self.project_id,
                region=self.region,
                lake_id=self.lake_id,
                body=self.body,
                dataplex_task_id=self.dataplex_task_id,
                validate_only=self.validate_only,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            if not self.asynchronous:
                self.log.info("Waiting for Dataplex task %s to be created", self.dataplex_task_id)
                task = hook.wait_for_operation(timeout=self.timeout, operation=operation)
                self.log.info("Task %s created successfully", self.dataplex_task_id)
            else:
                is_done = operation.done()
                self.log.info("Is operation done already? %s", is_done)
                return is_done
        except HttpError as err:
            if err.resp.status not in (409, "409"):
                raise
            self.log.info("Task %s already exists", self.dataplex_task_id)
            # Wait for task to be ready
            for time_to_wait in exponential_sleep_generator(initial=10, maximum=120):
                task = hook.get_task(
                    project_id=self.project_id,
                    region=self.region,
                    lake_id=self.lake_id,
                    dataplex_task_id=self.dataplex_task_id,
                    retry=self.retry,
                    timeout=self.timeout,
                    metadata=self.metadata,
                )
                if task["state"] != "CREATING":
                    break
                time.sleep(time_to_wait)

        return Task.to_dict(task)


class DataplexDeleteTaskOperator(GoogleCloudBaseOperator):
    """
    Delete the task resource.

    :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
    :param region: Required. The ID of the Google Cloud region that the task belongs to.
    :param lake_id: Required. The ID of the Google Cloud lake that the task belongs to.
    :param dataplex_task_id: Required. Task identifier.
    :param api_version: The version of the api that will be requested for example 'v3'.
    :param retry: A retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
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

    template_fields = ("project_id", "dataplex_task_id", "impersonation_chain")

    def __init__(
        self,
        project_id: str,
        region: str,
        lake_id: str,
        dataplex_task_id: str,
        api_version: str = "v1",
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.region = region
        self.lake_id = lake_id
        self.dataplex_task_id = dataplex_task_id
        self.api_version = api_version
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = DataplexHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Deleting Dataplex task %s", self.dataplex_task_id)

        operation = hook.delete_task(
            project_id=self.project_id,
            region=self.region,
            lake_id=self.lake_id,
            dataplex_task_id=self.dataplex_task_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        hook.wait_for_operation(timeout=self.timeout, operation=operation)
        self.log.info("Dataplex task %s deleted successfully!", self.dataplex_task_id)


class DataplexListTasksOperator(GoogleCloudBaseOperator):
    """
    Lists tasks under the given lake.

    :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
    :param region: Required. The ID of the Google Cloud region that the task belongs to.
    :param lake_id: Required. The ID of the Google Cloud lake that the task belongs to.
    :param page_size: Optional. Maximum number of tasks to return. The service may return fewer than this
        value. If unspecified, at most 10 tasks will be returned. The maximum value is 1000; values above 1000
        will be coerced to 1000.
    :param page_token: Optional. Page token received from a previous ListZones call. Provide this to retrieve
        the subsequent page. When paginating, all other parameters provided to ListZones must match the call
        that provided the page token.
    :param filter: Optional. Filter request.
    :param order_by: Optional. Order by fields for the result.
    :param api_version: The version of the api that will be requested for example 'v3'.
    :param retry: A retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
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

    template_fields = (
        "project_id",
        "page_size",
        "page_token",
        "filter",
        "order_by",
        "impersonation_chain",
    )
    operator_extra_links = (DataplexTasksLink(),)

    def __init__(
        self,
        project_id: str,
        region: str,
        lake_id: str,
        page_size: int | None = None,
        page_token: str | None = None,
        filter: str | None = None,
        order_by: str | None = None,
        api_version: str = "v1",
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.region = region
        self.lake_id = lake_id
        self.page_size = page_size
        self.page_token = page_token
        self.filter = filter
        self.order_by = order_by
        self.api_version = api_version
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            "project_id": self.project_id,
            "lake_id": self.lake_id,
            "region": self.region,
        }

    def execute(self, context: Context) -> list[dict]:
        hook = DataplexHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Listing Dataplex tasks from lake %s", self.lake_id)
        DataplexTasksLink.persist(context=context)

        tasks = hook.list_tasks(
            project_id=self.project_id,
            region=self.region,
            lake_id=self.lake_id,
            page_size=self.page_size,
            page_token=self.page_token,
            filter=self.filter,
            order_by=self.order_by,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return [Task.to_dict(task) for task in tasks]


class DataplexGetTaskOperator(GoogleCloudBaseOperator):
    """
    Get task resource.

    :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
    :param region: Required. The ID of the Google Cloud region that the task belongs to.
    :param lake_id: Required. The ID of the Google Cloud lake that the task belongs to.
    :param dataplex_task_id: Required. Task identifier.
    :param api_version: The version of the api that will be requested for example 'v3'.
    :param retry: A retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
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

    template_fields = ("project_id", "dataplex_task_id", "impersonation_chain")
    operator_extra_links = (DataplexTaskLink(),)

    def __init__(
        self,
        project_id: str,
        region: str,
        lake_id: str,
        dataplex_task_id: str,
        api_version: str = "v1",
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.region = region
        self.lake_id = lake_id
        self.dataplex_task_id = dataplex_task_id
        self.api_version = api_version
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            "lake_id": self.lake_id,
            "task_id": self.dataplex_task_id,
            "region": self.region,
            "project_id": self.project_id,
        }

    def execute(self, context: Context) -> dict:
        hook = DataplexHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Retrieving Dataplex task %s", self.dataplex_task_id)
        DataplexTaskLink.persist(context=context)

        task = hook.get_task(
            project_id=self.project_id,
            region=self.region,
            lake_id=self.lake_id,
            dataplex_task_id=self.dataplex_task_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        DataplexTasksLink.persist(context=context)
        return Task.to_dict(task)


class DataplexCreateLakeOperator(GoogleCloudBaseOperator):
    """
    Creates a lake resource within a lake.

    :param project_id: Required. The ID of the Google Cloud project that the lake belongs to.
    :param region: Required. The ID of the Google Cloud region that the lake belongs to.
    :param lake_id: Required. Lake identifier.
    :param body:  Required. The Request body contains an instance of Lake.
    :param validate_only: Optional. Only validate the request, but do not perform mutations. The default is
        false.
    :param api_version: The version of the api that will be requested for example 'v1'.
    :param retry: A retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param asynchronous: Flag informing should the Dataplex lake be created asynchronously.
        This is useful for long-running creating lakes.
    """

    template_fields = (
        "project_id",
        "lake_id",
        "body",
        "validate_only",
        "impersonation_chain",
    )
    template_fields_renderers = {"body": "json"}
    operator_extra_links = (DataplexLakeLink(),)

    def __init__(
        self,
        project_id: str,
        region: str,
        lake_id: str,
        body: dict[str, Any],
        validate_only: bool | None = None,
        api_version: str = "v1",
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        asynchronous: bool = False,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.region = region
        self.lake_id = lake_id
        self.body = body
        self.validate_only = validate_only
        self.api_version = api_version
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.asynchronous = asynchronous

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            "lake_id": self.lake_id,
            "region": self.region,
            "project_id": self.project_id,
        }

    def execute(self, context: Context) -> dict:
        hook = DataplexHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Creating Dataplex lake %s", self.lake_id)

        try:
            operation = hook.create_lake(
                project_id=self.project_id,
                region=self.region,
                lake_id=self.lake_id,
                body=self.body,
                validate_only=self.validate_only,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            if not self.asynchronous:
                self.log.info("Waiting for Dataplex lake %s to be created", self.lake_id)
                lake = hook.wait_for_operation(timeout=self.timeout, operation=operation)
                self.log.info("Lake %s created successfully", self.lake_id)
            else:
                is_done = operation.done()
                self.log.info("Is operation done already? %s", is_done)
                return is_done
        except HttpError as err:
            if err.resp.status not in (409, "409"):
                raise
            self.log.info("Lake %s already exists", self.lake_id)
            # Wait for lake to be ready
            for time_to_wait in exponential_sleep_generator(initial=10, maximum=120):
                lake = hook.get_lake(
                    project_id=self.project_id,
                    region=self.region,
                    lake_id=self.lake_id,
                    retry=self.retry,
                    timeout=self.timeout,
                    metadata=self.metadata,
                )
                if lake["state"] != "CREATING":
                    break
                time.sleep(time_to_wait)
        DataplexLakeLink.persist(context=context)
        return Lake.to_dict(lake)


class DataplexDeleteLakeOperator(GoogleCloudBaseOperator):
    """
    Delete the lake resource.

    :param project_id: Required. The ID of the Google Cloud project that the lake belongs to.
    :param region: Required. The ID of the Google Cloud region that the lake belongs to.
    :param lake_id: Required. Lake identifier.
    :param api_version: The version of the api that will be requested for example 'v1'.
    :param retry: A retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
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

    template_fields = ("project_id", "lake_id", "impersonation_chain")
    operator_extra_links = (DataplexLakeLink(),)

    def __init__(
        self,
        project_id: str,
        region: str,
        lake_id: str,
        api_version: str = "v1",
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.region = region
        self.lake_id = lake_id
        self.api_version = api_version
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            "lake_id": self.lake_id,
            "region": self.region,
            "project_id": self.project_id,
        }

    def execute(self, context: Context) -> None:
        hook = DataplexHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )

        self.log.info("Deleting Dataplex lake %s", self.lake_id)

        operation = hook.delete_lake(
            project_id=self.project_id,
            region=self.region,
            lake_id=self.lake_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        DataplexLakeLink.persist(context=context)
        hook.wait_for_operation(timeout=self.timeout, operation=operation)
        self.log.info("Dataplex lake %s deleted successfully!", self.lake_id)


class DataplexCreateOrUpdateDataQualityScanOperator(GoogleCloudBaseOperator):
    """
    Creates a DataScan resource.

    :param project_id: Required. The ID of the Google Cloud project that the lake belongs to.
    :param region: Required. The ID of the Google Cloud region that the lake belongs to.
    :param body:  Required. The Request body contains an instance of DataScan.
    :param data_scan_id: Required. Data Quality scan identifier.
    :param update_mask: Mask of fields to update.
    :param api_version: The version of the api that will be requested for example 'v1'.
    :param retry: A retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    :return: Dataplex data scan id
    """

    template_fields = ("project_id", "data_scan_id", "body", "impersonation_chain")
    template_fields_renderers = {"body": "json"}

    def __init__(
        self,
        project_id: str,
        region: str,
        data_scan_id: str,
        body: dict[str, Any] | DataScan,
        api_version: str = "v1",
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        update_mask: dict | FieldMask | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.region = region
        self.data_scan_id = data_scan_id
        self.body = body
        self.update_mask = update_mask
        self.api_version = api_version
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = DataplexHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )

        if self.update_mask is not None:
            self._update_data_scan(hook)
        else:
            self.log.info("Creating Dataplex Data Quality scan %s", self.data_scan_id)
            try:
                operation = hook.create_data_scan(
                    project_id=self.project_id,
                    region=self.region,
                    data_scan_id=self.data_scan_id,
                    body=self.body,
                    retry=self.retry,
                    timeout=self.timeout,
                    metadata=self.metadata,
                )
                hook.wait_for_operation(timeout=self.timeout, operation=operation)
                self.log.info("Dataplex Data Quality scan %s created successfully!", self.data_scan_id)
            except AlreadyExists:
                self._update_data_scan(hook)
            except GoogleAPICallError as e:
                raise AirflowException(f"Error creating Data Quality scan {self.data_scan_id}", e)

        return self.data_scan_id

    def _update_data_scan(self, hook: DataplexHook):
        self.log.info("Dataplex Data Quality scan already exists: %s", {self.data_scan_id})
        operation = hook.update_data_scan(
            project_id=self.project_id,
            region=self.region,
            data_scan_id=self.data_scan_id,
            body=self.body,
            update_mask=self.update_mask,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        hook.wait_for_operation(timeout=self.timeout, operation=operation)
        self.log.info("Dataplex Data Quality scan %s updated successfully!", self.data_scan_id)


class DataplexGetDataQualityScanOperator(GoogleCloudBaseOperator):
    """
    Gets a DataScan resource.

    :param project_id: Required. The ID of the Google Cloud project that the lake belongs to.
    :param region: Required. The ID of the Google Cloud region that the lake belongs to.
    :param data_scan_id: Required. Data Quality scan identifier.
    :param api_version: The version of the api that will be requested for example 'v1'.
    :param retry: A retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    :return: Dataplex data scan
    """

    template_fields = ("project_id", "data_scan_id", "impersonation_chain")

    def __init__(
        self,
        project_id: str,
        region: str,
        data_scan_id: str,
        api_version: str = "v1",
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.region = region
        self.data_scan_id = data_scan_id
        self.api_version = api_version
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = DataplexHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )

        self.log.info("Retrieving the details of Dataplex Data Quality scan %s", self.data_scan_id)
        data_quality_scan = hook.get_data_scan(
            project_id=self.project_id,
            region=self.region,
            data_scan_id=self.data_scan_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )

        return DataScan.to_dict(data_quality_scan)


class DataplexDeleteDataQualityScanOperator(GoogleCloudBaseOperator):
    """
    Deletes a DataScan resource.

    :param project_id: Required. The ID of the Google Cloud project that the lake belongs to.
    :param region: Required. The ID of the Google Cloud region that the lake belongs to.
    :param data_scan_id: Required. Data Quality scan identifier.
    :param api_version: The version of the api that will be requested for example 'v1'.
    :param retry: A retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    :return: None
    """

    template_fields = ("project_id", "data_scan_id", "impersonation_chain")

    def __init__(
        self,
        project_id: str,
        region: str,
        data_scan_id: str,
        api_version: str = "v1",
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.region = region
        self.data_scan_id = data_scan_id
        self.api_version = api_version
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = DataplexHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )

        self.log.info("Deleting Dataplex Data Quality Scan: %s", self.data_scan_id)

        operation = hook.delete_data_scan(
            project_id=self.project_id,
            region=self.region,
            data_scan_id=self.data_scan_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        hook.wait_for_operation(timeout=self.timeout, operation=operation)
        self.log.info("Dataplex Data Quality scan %s deleted successfully!", self.data_scan_id)


class DataplexRunDataQualityScanOperator(GoogleCloudBaseOperator):
    """
    Runs an on-demand execution of a DataScan.

    :param project_id: Required. The ID of the Google Cloud project that the lake belongs to.
    :param region: Required. The ID of the Google Cloud region that the lake belongs to.
    :param data_scan_id: Required. Data Quality scan identifier.
    :param api_version: The version of the api that will be requested for example 'v1'.
    :param retry: A retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param asynchronous: Flag informing that the Dataplex job should be run asynchronously.
        This is useful for submitting long-running jobs and
        waiting on them asynchronously using the DataplexDataQualityJobStatusSensor
    :param fail_on_dq_failure: If set to true and not all Data Quality scan rules have been passed,
        an exception is thrown. If set to false and not all Data Quality scan rules have been passed,
        execution will finish with success.
    :param result_timeout: Value in seconds for which operator will wait for the Data Quality scan result
        when the flag `asynchronous = False`.
        Throws exception if there is no result found after specified amount of seconds.
    :param polling_interval_seconds: time in seconds between polling for job completion.
        The value is considered only when running in deferrable mode. Must be greater than 0.
    :param deferrable: Run operator in the deferrable mode.

    :return: Dataplex Data Quality scan job id.
    """

    template_fields = ("project_id", "data_scan_id", "impersonation_chain")

    def __init__(
        self,
        project_id: str,
        region: str,
        data_scan_id: str,
        api_version: str = "v1",
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        asynchronous: bool = False,
        fail_on_dq_failure: bool = False,
        result_timeout: float = 60.0 * 10,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        polling_interval_seconds: int = 10,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.region = region
        self.data_scan_id = data_scan_id
        self.api_version = api_version
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.asynchronous = asynchronous
        self.fail_on_dq_failure = fail_on_dq_failure
        self.result_timeout = result_timeout
        self.deferrable = deferrable
        self.polling_interval_seconds = polling_interval_seconds

    def execute(self, context: Context) -> str:
        hook = DataplexHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )

        result = hook.run_data_scan(
            project_id=self.project_id,
            region=self.region,
            data_scan_id=self.data_scan_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        job_id = result.job.name.split("/")[-1]

        if self.deferrable:
            if self.asynchronous:
                raise AirflowException(
                    "Both asynchronous and deferrable parameters were passed. Please, provide only one."
                )
            self.defer(
                trigger=DataplexDataQualityJobTrigger(
                    job_id=job_id,
                    data_scan_id=self.data_scan_id,
                    project_id=self.project_id,
                    region=self.region,
                    gcp_conn_id=self.gcp_conn_id,
                    impersonation_chain=self.impersonation_chain,
                    polling_interval_seconds=self.polling_interval_seconds,
                ),
                method_name="execute_complete",
            )
        if not self.asynchronous:
            job = hook.wait_for_data_scan_job(
                job_id=job_id,
                data_scan_id=self.data_scan_id,
                project_id=self.project_id,
                region=self.region,
                result_timeout=self.result_timeout,
            )

            if job.state == DataScanJob.State.FAILED:
                raise AirflowException(f"Data Quality job failed: {job_id}")
            if job.state == DataScanJob.State.SUCCEEDED:
                if not job.data_quality_result.passed:
                    if self.fail_on_dq_failure:
                        raise AirflowDataQualityScanException(
                            f"Data Quality job {job_id} execution failed due to failure of its scanning "
                            f"rules: {self.data_scan_id}"
                        )
                else:
                    self.log.info("Data Quality job executed successfully.")
            else:
                self.log.info("Data Quality job execution returned status: %s", job.status)

        return job_id

    def execute_complete(self, context, event=None) -> None:
        """
        Act as a callback for when the trigger fires - returns immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        job_state = event["job_state"]
        job_id = event["job_id"]
        if job_state == DataScanJob.State.FAILED.name:  # type: ignore
            raise AirflowException(f"Job failed:\n{job_id}")
        if job_state == DataScanJob.State.CANCELLED.name:  # type: ignore
            raise AirflowException(f"Job was cancelled:\n{job_id}")
        if job_state == DataScanJob.State.SUCCEEDED.name:  # type: ignore
            job = event["job"]
            if not job["data_quality_result"]["passed"]:
                if self.fail_on_dq_failure:
                    raise AirflowDataQualityScanException(
                        f"Data Quality job {job_id} execution failed due to failure of its scanning "
                        f"rules: {self.data_scan_id}"
                    )
            else:
                self.log.info("Data Quality job executed successfully.")
        return job_id


class DataplexGetDataQualityScanResultOperator(GoogleCloudBaseOperator):
    """
    Gets a Data Scan Job resource.

    :param project_id: Required. The ID of the Google Cloud project that the lake belongs to.
    :param region: Required. The ID of the Google Cloud region that the lake belongs to.
    :param data_scan_id: Required. Data Quality scan identifier.
    :param job_id: Optional. Data Quality scan job identifier.
    :param api_version: The version of the api that will be requested for example 'v1'.
    :param retry: A retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param fail_on_dq_failure: If set to true and not all Data Quality scan rules have been passed,
        an exception is thrown. If set to false and not all Data Quality scan rules have been passed,
        execution will finish with success.
    :param wait_for_results: Flag indicating whether to wait for the result of a job execution
        or to return the job in its current state.
    :param result_timeout: Value in seconds for which operator will wait for the Data Quality scan result
        when the flag `wait_for_results = True`.
        Throws exception if there is no result found after specified amount of seconds.
    :param polling_interval_seconds: time in seconds between polling for job completion.
        The value is considered only when running in deferrable mode. Must be greater than 0.
    :param deferrable: Run operator in the deferrable mode.

    :return: Dict representing DataScanJob.
        When the job completes with a successful status, information about the Data Quality result
        is available.
    """

    template_fields = ("project_id", "data_scan_id", "impersonation_chain", "job_id")

    def __init__(
        self,
        project_id: str,
        region: str,
        data_scan_id: str,
        job_id: str | None = None,
        api_version: str = "v1",
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        fail_on_dq_failure: bool = False,
        wait_for_results: bool = True,
        result_timeout: float = 60.0 * 10,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        polling_interval_seconds: int = 10,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.region = region
        self.data_scan_id = data_scan_id
        self.job_id = job_id
        self.api_version = api_version
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.fail_on_dq_failure = fail_on_dq_failure
        self.wait_for_results = wait_for_results
        self.result_timeout = result_timeout
        self.deferrable = deferrable
        self.polling_interval_seconds = polling_interval_seconds

    def execute(self, context: Context) -> dict:
        hook = DataplexHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        # fetch the last job
        if not self.job_id:
            jobs = hook.list_data_scan_jobs(
                project_id=self.project_id,
                region=self.region,
                data_scan_id=self.data_scan_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            job_ids = [DataScanJob.to_dict(job) for job in jobs]
            if not job_ids:
                raise AirflowException("There are no jobs, you should create one before.")
            job_id = job_ids[0]["name"]
            self.job_id = job_id.split("/")[-1]

        if self.wait_for_results:
            if self.deferrable:
                self.defer(
                    trigger=DataplexDataQualityJobTrigger(
                        job_id=self.job_id,
                        data_scan_id=self.data_scan_id,
                        project_id=self.project_id,
                        region=self.region,
                        gcp_conn_id=self.gcp_conn_id,
                        impersonation_chain=self.impersonation_chain,
                        polling_interval_seconds=self.polling_interval_seconds,
                    ),
                    method_name="execute_complete",
                )
            else:
                job = hook.wait_for_data_scan_job(
                    job_id=self.job_id,
                    data_scan_id=self.data_scan_id,
                    project_id=self.project_id,
                    region=self.region,
                    result_timeout=self.result_timeout,
                )
        else:
            job = hook.get_data_scan_job(
                project_id=self.project_id,
                region=self.region,
                job_id=self.job_id,
                data_scan_id=self.data_scan_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )

        if job.state == DataScanJob.State.SUCCEEDED:
            if not job.data_quality_result.passed:
                if self.fail_on_dq_failure:
                    raise AirflowDataQualityScanException(
                        f"Data Quality job {self.job_id} execution failed due to failure of its scanning "
                        f"rules: {self.data_scan_id}"
                    )
            else:
                self.log.info("Data Quality job executed successfully")
        else:
            self.log.info("Data Quality job execution returned status: %s", job.state)

        result = DataScanJob.to_dict(job)
        result["state"] = DataScanJob.State(result["state"]).name

        return result

    def execute_complete(self, context, event=None) -> None:
        """
        Act as a callback for when the trigger fires - returns immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        job_state = event["job_state"]
        job_id = event["job_id"]
        job = event["job"]
        if job_state == DataScanJob.State.FAILED.name:  # type: ignore
            raise AirflowException(f"Job failed:\n{job_id}")
        if job_state == DataScanJob.State.CANCELLED.name:  # type: ignore
            raise AirflowException(f"Job was cancelled:\n{job_id}")
        if job_state == DataScanJob.State.SUCCEEDED.name:  # type: ignore
            if not job["data_quality_result"]["passed"]:
                if self.fail_on_dq_failure:
                    raise AirflowDataQualityScanException(
                        f"Data Quality job {self.job_id} execution failed due to failure of its scanning "
                        f"rules: {self.data_scan_id}"
                    )
            else:
                self.log.info("Data Quality job executed successfully")
        else:
            self.log.info("Data Quality job execution returned status: %s", job_state)

        return job


class DataplexCreateOrUpdateDataProfileScanOperator(GoogleCloudBaseOperator):
    """
    Creates a DataScan Data Profile resource.

    :param project_id: Required. The ID of the Google Cloud project that the lake belongs to.
    :param region: Required. The ID of the Google Cloud region that the lake belongs to.
    :param body:  Required. The Request body contains an instance of DataScan.
    :param data_scan_id: Required. Data Profile scan identifier.
    :param update_mask: Mask of fields to update.
    :param api_version: The version of the api that will be requested for example 'v1'.
    :param retry: A retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    :return: Dataplex data profile id
    """

    template_fields = ("project_id", "data_scan_id", "body", "impersonation_chain")
    template_fields_renderers = {"body": "json"}

    def __init__(
        self,
        project_id: str,
        region: str,
        data_scan_id: str,
        body: dict[str, Any] | DataScan,
        api_version: str = "v1",
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        update_mask: dict | FieldMask | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.region = region
        self.data_scan_id = data_scan_id
        self.body = body
        self.update_mask = update_mask
        self.api_version = api_version
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = DataplexHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )

        self.log.info("Creating Dataplex Data Profile scan %s", self.data_scan_id)
        try:
            operation = hook.create_data_scan(
                project_id=self.project_id,
                region=self.region,
                data_scan_id=self.data_scan_id,
                body=self.body,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            hook.wait_for_operation(timeout=self.timeout, operation=operation)
            self.log.info("Dataplex Data Profile scan %s created successfully!", self.data_scan_id)
        except AlreadyExists:
            self.log.info("Dataplex Data Profile scan already exists: %s", {self.data_scan_id})

            operation = hook.update_data_scan(
                project_id=self.project_id,
                region=self.region,
                data_scan_id=self.data_scan_id,
                body=self.body,
                update_mask=self.update_mask,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            hook.wait_for_operation(timeout=self.timeout, operation=operation)
            self.log.info("Dataplex Data Profile scan %s updated successfully!", self.data_scan_id)
        except GoogleAPICallError as e:
            raise AirflowException(f"Error creating Data Profile scan {self.data_scan_id}", e)

        return self.data_scan_id


class DataplexGetDataProfileScanOperator(GoogleCloudBaseOperator):
    """
    Gets a DataScan DataProfile resource.

    :param project_id: Required. The ID of the Google Cloud project that the lake belongs to.
    :param region: Required. The ID of the Google Cloud region that the lake belongs to.
    :param data_scan_id: Required. Data Profile scan identifier.
    :param api_version: The version of the api that will be requested for example 'v1'.
    :param retry: A retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    :return: Dataplex data profile
    """

    template_fields = ("project_id", "data_scan_id", "impersonation_chain")

    def __init__(
        self,
        project_id: str,
        region: str,
        data_scan_id: str,
        api_version: str = "v1",
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.region = region
        self.data_scan_id = data_scan_id
        self.api_version = api_version
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = DataplexHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )

        self.log.info("Retrieving the details of Dataplex Data Profile scan %s", self.data_scan_id)
        data_profile_scan = hook.get_data_scan(
            project_id=self.project_id,
            region=self.region,
            data_scan_id=self.data_scan_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )

        return DataScan.to_dict(data_profile_scan)


class DataplexDeleteDataProfileScanOperator(GoogleCloudBaseOperator):
    """
    Deletes a DataScan DataProfile resource.

    :param project_id: Required. The ID of the Google Cloud project that the lake belongs to.
    :param region: Required. The ID of the Google Cloud region that the lake belongs to.
    :param data_scan_id: Required. Data Profile scan identifier.
    :param api_version: The version of the api that will be requested for example 'v1'.
    :param retry: A retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :return: None
    """

    template_fields = ("project_id", "data_scan_id", "impersonation_chain")

    def __init__(
        self,
        project_id: str,
        region: str,
        data_scan_id: str,
        api_version: str = "v1",
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.region = region
        self.data_scan_id = data_scan_id
        self.api_version = api_version
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = DataplexHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )

        self.log.info("Deleting Dataplex Data Profile Scan: %s", self.data_scan_id)

        operation = hook.delete_data_scan(
            project_id=self.project_id,
            region=self.region,
            data_scan_id=self.data_scan_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        hook.wait_for_operation(timeout=self.timeout, operation=operation)
        self.log.info("Dataplex Data Profile scan %s deleted successfully!", self.data_scan_id)


class DataplexRunDataProfileScanOperator(GoogleCloudBaseOperator):
    """
    Runs an on-demand execution of a DataScan Data Profile Scan.

    :param project_id: Required. The ID of the Google Cloud project that the lake belongs to.
    :param region: Required. The ID of the Google Cloud region that the lake belongs to.
    :param data_scan_id: Required. Data Profile scan identifier.
    :param api_version: The version of the api that will be requested for example 'v1'.
    :param retry: A retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param asynchronous: Flag informing that the Dataplex job should be run asynchronously.
        This is useful for submitting long-running jobs and
        waiting on them asynchronously using the DataplexDataProfileJobStatusSensor
    :param result_timeout: Value in seconds for which operator will wait for the Data Profile scan result
        when the flag `asynchronous = False`.
        Throws exception if there is no result found after specified amount of seconds.
    :param polling_interval_seconds: time in seconds between polling for job completion.
        The value is considered only when running in deferrable mode. Must be greater than 0.
    :param deferrable: Run operator in the deferrable mode.

    :return: Dataplex Data Profile scan job id.
    """

    template_fields = ("project_id", "data_scan_id", "impersonation_chain")

    def __init__(
        self,
        project_id: str,
        region: str,
        data_scan_id: str,
        api_version: str = "v1",
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        asynchronous: bool = False,
        result_timeout: float = 60.0 * 10,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        polling_interval_seconds: int = 10,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.region = region
        self.data_scan_id = data_scan_id
        self.api_version = api_version
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.asynchronous = asynchronous
        self.result_timeout = result_timeout
        self.deferrable = deferrable
        self.polling_interval_seconds = polling_interval_seconds

    def execute(self, context: Context) -> dict:
        hook = DataplexHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )

        result = hook.run_data_scan(
            project_id=self.project_id,
            region=self.region,
            data_scan_id=self.data_scan_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        job_id = result.job.name.split("/")[-1]

        if self.deferrable:
            if self.asynchronous:
                raise AirflowException(
                    "Both asynchronous and deferrable parameters were passed. Please, provide only one."
                )
            self.defer(
                trigger=DataplexDataProfileJobTrigger(
                    job_id=job_id,
                    data_scan_id=self.data_scan_id,
                    project_id=self.project_id,
                    region=self.region,
                    gcp_conn_id=self.gcp_conn_id,
                    impersonation_chain=self.impersonation_chain,
                    polling_interval_seconds=self.polling_interval_seconds,
                ),
                method_name="execute_complete",
            )
        if not self.asynchronous:
            job = hook.wait_for_data_scan_job(
                job_id=job_id,
                data_scan_id=self.data_scan_id,
                project_id=self.project_id,
                region=self.region,
                result_timeout=self.result_timeout,
            )

            if job.state == DataScanJob.State.FAILED.name:  # type: ignore
                raise AirflowException(f"Data Profile job failed: {job_id}")
            if job.state == DataScanJob.State.SUCCEEDED.name:  # type: ignore
                self.log.info("Data Profile job executed successfully.")
            else:
                self.log.info("Data Profile job execution returned status: %s", job.state)

        return job_id

    def execute_complete(self, context, event=None) -> None:
        """
        Act as a callback for when the trigger fires - returns immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        job_state = event["job_state"]
        job_id = event["job_id"]
        if job_state == DataScanJob.State.FAILED.name:  # type: ignore
            raise AirflowException(f"Job failed:\n{job_id}")
        if job_state == DataScanJob.State.CANCELLED.name:  # type: ignore
            raise AirflowException(f"Job was cancelled:\n{job_id}")
        if job_state == DataScanJob.State.SUCCEEDED.name:  # type: ignore
            self.log.info("Data Profile job executed successfully.")
        return job_id


class DataplexGetDataProfileScanResultOperator(GoogleCloudBaseOperator):
    """
    Gets a DataScan Data Profile Job resource.

    :param project_id: Required. The ID of the Google Cloud project that the lake belongs to.
    :param region: Required. The ID of the Google Cloud region that the lake belongs to.
    :param data_scan_id: Required. Data Profile scan identifier.
    :param job_id: Optional. Data Profile scan job identifier.
    :param api_version: The version of the api that will be requested for example 'v1'.
    :param retry: A retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param wait_for_results: Flag indicating whether to wait for the result of a job execution
        or to return the job in its current state.
    :param result_timeout: Value in seconds for which operator will wait for the Data Profile scan result
        when the flag `wait_for_results = True`.
        Throws exception if there is no result found after specified amount of seconds.

    :return: Dict representing DataScanJob.
        When the job completes with a successful status, information about the Data Profile result
        is available.
    """

    template_fields = ("project_id", "data_scan_id", "impersonation_chain")

    def __init__(
        self,
        project_id: str,
        region: str,
        data_scan_id: str,
        job_id: str | None = None,
        api_version: str = "v1",
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        wait_for_results: bool = True,
        result_timeout: float = 60.0 * 10,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.region = region
        self.data_scan_id = data_scan_id
        self.job_id = job_id
        self.api_version = api_version
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.wait_for_results = wait_for_results
        self.result_timeout = result_timeout

    def execute(self, context: Context) -> dict:
        hook = DataplexHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        # fetch the last job
        if not self.job_id:
            jobs = hook.list_data_scan_jobs(
                project_id=self.project_id,
                region=self.region,
                data_scan_id=self.data_scan_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            job_ids = [DataScanJob.to_dict(job) for job in jobs]
            if not job_ids:
                raise AirflowException("There are no jobs, you should create one before.")
            job_id = job_ids[0]["name"]
            self.job_id = job_id.split("/")[-1]

        if self.wait_for_results:
            job = hook.wait_for_data_scan_job(
                job_id=self.job_id,
                data_scan_id=self.data_scan_id,
                project_id=self.project_id,
                region=self.region,
                result_timeout=self.result_timeout,
            )
        else:
            job = hook.get_data_scan_job(
                project_id=self.project_id,
                region=self.region,
                job_id=self.job_id,
                data_scan_id=self.data_scan_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        if job.state == DataScanJob.State.SUCCEEDED:
            self.log.info("Data Profile job executed successfully")
        else:
            self.log.info("Data Profile job execution returned status: %s", job.state)

        result = DataScanJob.to_dict(job)
        result["state"] = DataScanJob.State(result["state"]).name

        return result

    def execute_complete(self, context, event=None) -> None:
        """
        Act as a callback for when the trigger fires - returns immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        job_state = event["job_state"]
        job_id = event["job_id"]
        job = event["job"]
        if job_state == DataScanJob.State.FAILED:
            raise AirflowException(f"Job failed:\n{job_id}")
        if job_state == DataScanJob.State.CANCELLED:
            raise AirflowException(f"Job was cancelled:\n{job_id}")
        if job_state == DataScanJob.State.SUCCEEDED:
            self.log.info("Data Profile job executed successfully")
        else:
            self.log.info("Data Profile job execution returned status: %s", job_state)

        return job


class DataplexCreateZoneOperator(GoogleCloudBaseOperator):
    """
    Creates a Zone resource within a Lake.

    :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
    :param region: Required. The ID of the Google Cloud region that the task belongs to.
    :param lake_id: Required. The ID of the Google Cloud lake that the task belongs to.
    :param body:  Required. The Request body contains an instance of Zone.
    :param zone_id: Required. Task identifier.
    :param api_version: The version of the api that will be requested for example 'v3'.
    :param retry: A retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    :return: Zone
    """

    template_fields = (
        "project_id",
        "zone_id",
        "body",
        "lake_id",
        "impersonation_chain",
    )
    template_fields_renderers = {"body": "json"}

    def __init__(
        self,
        project_id: str,
        region: str,
        lake_id: str,
        body: dict[str, Any] | Zone,
        zone_id: str,
        api_version: str = "v1",
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.region = region
        self.lake_id = lake_id
        self.body = body
        self.zone_id = zone_id
        self.api_version = api_version
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = DataplexHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Creating Dataplex zone %s", self.zone_id)

        try:
            operation = hook.create_zone(
                project_id=self.project_id,
                region=self.region,
                lake_id=self.lake_id,
                body=self.body,
                zone_id=self.zone_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            zone = hook.wait_for_operation(timeout=self.timeout, operation=operation)

        except GoogleAPICallError as e:
            raise AirflowException(f"Error occurred when creating zone {self.zone_id}", e)

        self.log.info("Dataplex zone %s created successfully!", self.zone_id)
        return Zone.to_dict(zone)


class DataplexDeleteZoneOperator(GoogleCloudBaseOperator):
    """
    Deletes a Zone resource. All assets within a zone must be deleted before the zone can be deleted.

    :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
    :param region: Required. The ID of the Google Cloud region that the task belongs to.
    :param lake_id: Required. The ID of the Google Cloud lake that the task belongs to.
    :param zone_id: Required. Zone identifier.
    :param api_version: The version of the api that will be requested for example 'v3'.
    :param retry: A retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :return: None
    """

    template_fields = (
        "project_id",
        "lake_id",
        "zone_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        project_id: str,
        region: str,
        lake_id: str,
        zone_id: str,
        api_version: str = "v1",
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.region = region
        self.lake_id = lake_id
        self.zone_id = zone_id
        self.api_version = api_version
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = DataplexHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Deleting Dataplex zone %s", self.zone_id)

        operation = hook.delete_zone(
            project_id=self.project_id,
            region=self.region,
            lake_id=self.lake_id,
            zone_id=self.zone_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        hook.wait_for_operation(timeout=self.timeout, operation=operation)
        self.log.info("Dataplex zone %s deleted successfully!", self.zone_id)


class DataplexCreateAssetOperator(GoogleCloudBaseOperator):
    """
    Creates an Asset resource.

    :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
    :param region: Required. The ID of the Google Cloud region that the task belongs to.
    :param lake_id: Required. The ID of the Google Cloud lake that the lake belongs to.
    :param zone_id: Required. Zone identifier.
    :param asset_id: Required. Asset identifier.
    :param body:  Required. The Request body contains an instance of Asset.
    :param api_version: The version of the api that will be requested for example 'v3'.
    :param retry: A retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :return: Asset
    """

    template_fields = (
        "project_id",
        "zone_id",
        "asset_id",
        "body",
        "impersonation_chain",
    )
    template_fields_renderers = {"body": "json"}

    def __init__(
        self,
        project_id: str,
        region: str,
        lake_id: str,
        body: dict[str, Any] | Asset,
        zone_id: str,
        asset_id: str,
        api_version: str = "v1",
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.region = region
        self.lake_id = lake_id
        self.body = body
        self.zone_id = zone_id
        self.asset_id = asset_id
        self.api_version = api_version
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = DataplexHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Creating Dataplex asset %s", self.zone_id)
        try:
            operation = hook.create_asset(
                project_id=self.project_id,
                region=self.region,
                lake_id=self.lake_id,
                body=self.body,
                zone_id=self.zone_id,
                asset_id=self.asset_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            result = hook.wait_for_operation(timeout=self.timeout, operation=operation)
        except GoogleAPICallError as e:
            raise AirflowException(f"Error occurred when creating asset {self.asset_id}", e)

        self.log.info("Dataplex asset %s created successfully!", self.asset_id)
        return Asset.to_dict(result)


class DataplexDeleteAssetOperator(GoogleCloudBaseOperator):
    """
    Deletes an asset resource.

    :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
    :param region: Required. The ID of the Google Cloud region that the task belongs to.
    :param lake_id: Required. The ID of the Google Cloud lake that the asset belongs to.
    :param zone_id: Required. Zone identifier.
    :param asset_id: Required. Asset identifier.
    :param api_version: The version of the api that will be requested for example 'v3'.
    :param retry: A retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    :return: None
    """

    template_fields = (
        "project_id",
        "zone_id",
        "asset_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        project_id: str,
        region: str,
        lake_id: str,
        zone_id: str,
        asset_id: str,
        api_version: str = "v1",
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.region = region
        self.lake_id = lake_id
        self.zone_id = zone_id
        self.asset_id = asset_id
        self.api_version = api_version
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = DataplexHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Deleting Dataplex asset %s", self.asset_id)

        operation = hook.delete_asset(
            project_id=self.project_id,
            region=self.region,
            lake_id=self.lake_id,
            zone_id=self.zone_id,
            asset_id=self.asset_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        hook.wait_for_operation(timeout=self.timeout, operation=operation)
        self.log.info("Dataplex asset %s deleted successfully!", self.asset_id)


class DataplexCatalogBaseOperator(GoogleCloudBaseOperator):
    """
    Base class for all Dataplex Catalog operators.

    :param project_id: Required. The ID of the Google Cloud project where the service is used.
    :param location: Required. The ID of the Google Cloud region where the service is used.
    :param gcp_conn_id: Optional. The connection ID to use to connect to Google Cloud.
    :param retry: Optional. A retry object used to retry requests. If `None` is specified, requests will not
        be retried.
    :param timeout: Optional. The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Optional. Additional metadata that is provided to the method.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
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
        "location",
        "gcp_conn_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        project_id: str,
        location: str,
        gcp_conn_id: str = "google_cloud_default",
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata

    @cached_property
    def hook(self) -> DataplexHook:
        return DataplexHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            "location": self.location,
            "project_id": self.project_id,
        }


class DataplexCatalogCreateEntryGroupOperator(DataplexCatalogBaseOperator):
    """
    Create an EntryGroup resource.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataplexCatalogCreateEntryGroupOperator`

    :param entry_group_id: Required. EntryGroup identifier.
    :param entry_group_configuration: Required. EntryGroup configuration.
        For more details please see API documentation:
        https://cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations.entryGroups#EntryGroup
    :param validate_request: Optional. If set, performs request validation, but does not actually
        execute the request.
    :param project_id: Required. The ID of the Google Cloud project where the service is used.
    :param location: Required. The ID of the Google Cloud region where the service is used.
    :param gcp_conn_id: Optional. The connection ID to use to connect to Google Cloud.
    :param retry: Optional. A retry object used to retry requests. If `None` is specified, requests will not
        be retried.
    :param timeout: Optional. The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Optional. Additional metadata that is provided to the method.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple(
        {"entry_group_id", "entry_group_configuration"} | set(DataplexCatalogBaseOperator.template_fields)
    )
    operator_extra_links = (DataplexCatalogEntryGroupLink(),)

    def __init__(
        self,
        entry_group_id: str,
        entry_group_configuration: EntryGroup | dict,
        validate_request: bool = False,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.entry_group_id = entry_group_id
        self.entry_group_configuration = entry_group_configuration
        self.validate_request = validate_request

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            **super().extra_links_params,
            "entry_group_id": self.entry_group_id,
        }

    def execute(self, context: Context):
        DataplexCatalogEntryGroupLink.persist(context=context)
        if self.validate_request:
            self.log.info("Validating a Create Dataplex Catalog EntryGroup request.")
        else:
            self.log.info("Creating a Dataplex Catalog EntryGroup.")

        try:
            operation = self.hook.create_entry_group(
                entry_group_id=self.entry_group_id,
                entry_group_configuration=self.entry_group_configuration,
                location=self.location,
                project_id=self.project_id,
                validate_only=self.validate_request,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            entry_group = self.hook.wait_for_operation(timeout=self.timeout, operation=operation)
        except AlreadyExists:
            entry_group = self.hook.get_entry_group(
                entry_group_id=self.entry_group_id,
                location=self.location,
                project_id=self.project_id,
            )
            self.log.info(
                "Dataplex Catalog EntryGroup %s already exists.",
                self.entry_group_id,
            )
            result = EntryGroup.to_dict(entry_group)
            return result
        except Exception as ex:
            raise AirflowException(ex)
        else:
            result = EntryGroup.to_dict(entry_group) if not self.validate_request else None

        if not self.validate_request:
            self.log.info("Dataplex Catalog EntryGroup %s was successfully created.", self.entry_group_id)
        return result


class DataplexCatalogGetEntryGroupOperator(DataplexCatalogBaseOperator):
    """
    Get an EntryGroup resource.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataplexCatalogGetEntryGroupOperator`

    :param entry_group_id: Required. EntryGroup identifier.
    :param project_id: Required. The ID of the Google Cloud project where the service is used.
    :param location: Required. The ID of the Google Cloud region where the service is used.
    :param gcp_conn_id: Optional. The connection ID to use to connect to Google Cloud.
    :param retry: Optional. A retry object used to retry requests. If `None` is specified, requests will not
        be retried.
    :param timeout: Optional. The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Optional. Additional metadata that is provided to the method.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple(
        {"entry_group_id"} | set(DataplexCatalogBaseOperator.template_fields)
    )
    operator_extra_links = (DataplexCatalogEntryGroupLink(),)

    def __init__(
        self,
        entry_group_id: str,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.entry_group_id = entry_group_id

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            **super().extra_links_params,
            "entry_group_id": self.entry_group_id,
        }

    def execute(self, context: Context):
        DataplexCatalogEntryGroupLink.persist(context=context)
        self.log.info(
            "Retrieving Dataplex Catalog EntryGroup %s.",
            self.entry_group_id,
        )
        try:
            entry_group = self.hook.get_entry_group(
                entry_group_id=self.entry_group_id,
                location=self.location,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except NotFound:
            self.log.info(
                "Dataplex Catalog EntryGroup %s not found.",
                self.entry_group_id,
            )
            raise AirflowException(NotFound)
        except Exception as ex:
            raise AirflowException(ex)

        return EntryGroup.to_dict(entry_group)


class DataplexCatalogDeleteEntryGroupOperator(DataplexCatalogBaseOperator):
    """
    Delete an EntryGroup resource.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataplexCatalogDeleteEntryGroupOperator`

    :param entry_group_id: Required. EntryGroup identifier.
    :param project_id: Required. The ID of the Google Cloud project where the service is used.
    :param location: Required. The ID of the Google Cloud region where the service is used.
    :param gcp_conn_id: Optional. The connection ID to use to connect to Google Cloud.
    :param retry: Optional. A retry object used to retry requests. If `None` is specified, requests will not
        be retried.
    :param timeout: Optional. The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Optional. Additional metadata that is provided to the method.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple(
        {"entry_group_id"} | set(DataplexCatalogBaseOperator.template_fields)
    )

    def __init__(
        self,
        entry_group_id: str,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.entry_group_id = entry_group_id

    def execute(self, context: Context):
        self.log.info(
            "Deleting Dataplex Catalog EntryGroup %s.",
            self.entry_group_id,
        )
        try:
            operation = self.hook.delete_entry_group(
                entry_group_id=self.entry_group_id,
                location=self.location,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            self.hook.wait_for_operation(timeout=self.timeout, operation=operation)

        except NotFound:
            self.log.info(
                "Dataplex Catalog EntryGroup %s not found.",
                self.entry_group_id,
            )
            raise AirflowException(NotFound)
        except Exception as ex:
            raise AirflowException(ex)
        return None


class DataplexCatalogListEntryGroupsOperator(DataplexCatalogBaseOperator):
    """
    List EntryGroup resources.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataplexCatalogListEntryGroupsOperator`

    :param filter_by: Optional. Filter to apply on the list results.
    :param order_by: Optional. Fields to order the results by.
    :param page_size: Optional. Maximum number of EntryGroups to return on the page.
    :param page_token: Optional. Token to retrieve the next page of results.
    :param project_id: Required. The ID of the Google Cloud project where the service is used.
    :param location: Required. The ID of the Google Cloud region where the service is used.
    :param gcp_conn_id: Optional. The connection ID to use to connect to Google Cloud.
    :param retry: Optional. A retry object used to retry requests. If `None` is specified, requests will not
        be retried.
    :param timeout: Optional. The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Optional. Additional metadata that is provided to the method.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple(DataplexCatalogBaseOperator.template_fields)
    operator_extra_links = (DataplexCatalogEntryGroupsLink(),)

    def __init__(
        self,
        page_size: int | None = None,
        page_token: str | None = None,
        filter_by: str | None = None,
        order_by: str | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.page_size = page_size
        self.page_token = page_token
        self.filter_by = filter_by
        self.order_by = order_by

    def execute(self, context: Context):
        DataplexCatalogEntryGroupsLink.persist(context=context)
        self.log.info(
            "Listing Dataplex Catalog EntryGroup from location %s.",
            self.location,
        )
        try:
            entry_group_on_page = self.hook.list_entry_groups(
                location=self.location,
                project_id=self.project_id,
                page_size=self.page_size,
                page_token=self.page_token,
                filter_by=self.filter_by,
                order_by=self.order_by,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            self.log.info("EntryGroup on page: %s", entry_group_on_page)
            context["ti"].xcom_push(
                key="entry_group_page",
                value=ListEntryGroupsResponse.to_dict(entry_group_on_page._response),
            )
        except Exception as ex:
            raise AirflowException(ex)

        # Constructing list to return EntryGroups in readable format
        entry_groups_list = [
            MessageToDict(entry_group._pb, preserving_proto_field_name=True)
            for entry_group in next(iter(entry_group_on_page.pages)).entry_groups
        ]
        return entry_groups_list


class DataplexCatalogUpdateEntryGroupOperator(DataplexCatalogBaseOperator):
    """
    Update an EntryGroup resource.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataplexCatalogUpdateEntryGroupOperator`

    :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
    :param location: Required. The ID of the Google Cloud region that the task belongs to.
    :param update_mask: Optional. Names of fields whose values to overwrite on an entry group.
        If this parameter is absent or empty, all modifiable fields are overwritten. If such
        fields are non-required and omitted in the request body, their values are emptied.
    :param entry_group_id: Required. ID of the EntryGroup to update.
    :param entry_group_configuration: Required. The updated configuration body of the EntryGroup.
        For more details please see API documentation:
        https://cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations.entryGroups#EntryGroup
    :param validate_only: Optional. The service validates the request without performing any mutations.
    :param retry: Optional. A retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: Optional. The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Optional. Additional metadata that is provided to the method.
    :param gcp_conn_id: Optional. The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple(
        {"entry_group_id", "entry_group_configuration", "update_mask"}
        | set(DataplexCatalogBaseOperator.template_fields)
    )
    operator_extra_links = (DataplexCatalogEntryGroupLink(),)

    def __init__(
        self,
        entry_group_id: str,
        entry_group_configuration: dict | EntryGroup,
        update_mask: list[str] | FieldMask | None = None,
        validate_request: bool | None = False,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.entry_group_id = entry_group_id
        self.entry_group_configuration = entry_group_configuration
        self.update_mask = update_mask
        self.validate_request = validate_request

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            **super().extra_links_params,
            "entry_group_id": self.entry_group_id,
        }

    def execute(self, context: Context):
        DataplexCatalogEntryGroupLink.persist(context=context)
        if self.validate_request:
            self.log.info("Validating an Update Dataplex Catalog EntryGroup request.")
        else:
            self.log.info(
                "Updating Dataplex Catalog EntryGroup %s.",
                self.entry_group_id,
            )
        try:
            operation = self.hook.update_entry_group(
                location=self.location,
                project_id=self.project_id,
                entry_group_id=self.entry_group_id,
                entry_group_configuration=self.entry_group_configuration,
                update_mask=self.update_mask,
                validate_only=self.validate_request,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            entry_group = self.hook.wait_for_operation(timeout=self.timeout, operation=operation)

        except NotFound as ex:
            self.log.info("Specified EntryGroup was not found.")
            raise AirflowException(ex)
        except Exception as exc:
            raise AirflowException(exc)
        else:
            result = EntryGroup.to_dict(entry_group) if not self.validate_request else None

        if not self.validate_request:
            self.log.info("EntryGroup %s was successfully updated.", self.entry_group_id)
        return result


class DataplexCatalogCreateEntryTypeOperator(DataplexCatalogBaseOperator):
    """
    Create an EntryType resource.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataplexCatalogCreateEntryTypeOperator`

    :param entry_type_id: Required. EntryType identifier.
    :param entry_type_configuration: Required. EntryType configuration.
        For more details please see API documentation:
        https://cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations.entryGroups#EntryGroup
    :param validate_request: Optional. If set, performs request validation, but does not actually
        execute the request.
    :param project_id: Required. The ID of the Google Cloud project where the service is used.
    :param location: Required. The ID of the Google Cloud region where the service is used.
    :param gcp_conn_id: Optional. The connection ID to use to connect to Google Cloud.
    :param retry: Optional. A retry object used to retry requests. If `None` is specified, requests will not
        be retried.
    :param timeout: Optional. The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Optional. Additional metadata that is provided to the method.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple(
        {"entry_type_id", "entry_type_configuration"} | set(DataplexCatalogBaseOperator.template_fields)
    )
    operator_extra_links = (DataplexCatalogEntryTypeLink(),)

    def __init__(
        self,
        entry_type_id: str,
        entry_type_configuration: EntryType | dict,
        validate_request: bool = False,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.entry_type_id = entry_type_id
        self.entry_type_configuration = entry_type_configuration
        self.validate_request = validate_request

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            **super().extra_links_params,
            "entry_type_id": self.entry_type_id,
        }

    def execute(self, context: Context):
        DataplexCatalogEntryTypeLink.persist(context=context)
        if self.validate_request:
            self.log.info("Validating a Create Dataplex Catalog EntryType request.")
        else:
            self.log.info("Creating a Dataplex Catalog EntryType.")

        try:
            operation = self.hook.create_entry_type(
                entry_type_id=self.entry_type_id,
                entry_type_configuration=self.entry_type_configuration,
                location=self.location,
                project_id=self.project_id,
                validate_only=self.validate_request,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            entry_type = self.hook.wait_for_operation(timeout=self.timeout, operation=operation)
        except AlreadyExists:
            entry_type = self.hook.get_entry_type(
                entry_type_id=self.entry_type_id,
                location=self.location,
                project_id=self.project_id,
            )
            self.log.info(
                "Dataplex Catalog EntryType %s already exists.",
                self.entry_type_id,
            )
            result = EntryType.to_dict(entry_type)
            return result
        except Exception as ex:
            raise AirflowException(ex)
        else:
            result = EntryType.to_dict(entry_type) if not self.validate_request else None

        if not self.validate_request:
            self.log.info("Dataplex Catalog EntryType %s was successfully created.", self.entry_type_id)
        return result


class DataplexCatalogGetEntryTypeOperator(DataplexCatalogBaseOperator):
    """
    Get an EntryType resource.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataplexCatalogGetEntryTypeOperator`

    :param entry_type_id: Required. EntryType identifier.
    :param project_id: Required. The ID of the Google Cloud project where the service is used.
    :param location: Required. The ID of the Google Cloud region where the service is used.
    :param gcp_conn_id: Optional. The connection ID to use to connect to Google Cloud.
    :param retry: Optional. A retry object used to retry requests. If `None` is specified, requests will not
        be retried.
    :param timeout: Optional. The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Optional. Additional metadata that is provided to the method.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple(
        {"entry_type_id"} | set(DataplexCatalogBaseOperator.template_fields)
    )
    operator_extra_links = (DataplexCatalogEntryTypeLink(),)

    def __init__(
        self,
        entry_type_id: str,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.entry_type_id = entry_type_id

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            **super().extra_links_params,
            "entry_type_id": self.entry_type_id,
        }

    def execute(self, context: Context):
        DataplexCatalogEntryTypeLink.persist(context=context)
        self.log.info(
            "Retrieving Dataplex Catalog EntryType %s.",
            self.entry_type_id,
        )
        try:
            entry_type = self.hook.get_entry_type(
                entry_type_id=self.entry_type_id,
                location=self.location,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except NotFound:
            self.log.info(
                "Dataplex Catalog EntryType %s not found.",
                self.entry_type_id,
            )
            raise AirflowException(NotFound)
        except Exception as ex:
            raise AirflowException(ex)

        return EntryType.to_dict(entry_type)


class DataplexCatalogDeleteEntryTypeOperator(DataplexCatalogBaseOperator):
    """
    Delete an EntryType resource.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataplexCatalogDeleteEntryTypeOperator`

    :param entry_type_id: Required. EntryType identifier.
    :param project_id: Required. The ID of the Google Cloud project where the service is used.
    :param location: Required. The ID of the Google Cloud region where the service is used.
    :param gcp_conn_id: Optional. The connection ID to use to connect to Google Cloud.
    :param retry: Optional. A retry object used to retry requests. If `None` is specified, requests will not
        be retried.
    :param timeout: Optional. The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Optional. Additional metadata that is provided to the method.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple(
        {"entry_type_id"} | set(DataplexCatalogBaseOperator.template_fields)
    )

    def __init__(
        self,
        entry_type_id: str,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.entry_type_id = entry_type_id

    def execute(self, context: Context):
        self.log.info(
            "Deleting Dataplex Catalog EntryType %s.",
            self.entry_type_id,
        )
        try:
            operation = self.hook.delete_entry_type(
                entry_type_id=self.entry_type_id,
                location=self.location,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            self.hook.wait_for_operation(timeout=self.timeout, operation=operation)

        except NotFound:
            self.log.info(
                "Dataplex Catalog EntryType %s not found.",
                self.entry_type_id,
            )
            raise AirflowException(NotFound)
        except Exception as ex:
            raise AirflowException(ex)
        return None


class DataplexCatalogListEntryTypesOperator(DataplexCatalogBaseOperator):
    """
    List EntryType resources.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataplexCatalogListEntryTypesOperator`

    :param filter_by: Optional. Filter to apply on the list results.
    :param order_by: Optional. Fields to order the results by.
    :param page_size: Optional. Maximum number of EntryTypes to return on the page.
    :param page_token: Optional. Token to retrieve the next page of results.
    :param project_id: Required. The ID of the Google Cloud project where the service is used.
    :param location: Required. The ID of the Google Cloud region where the service is used.
    :param gcp_conn_id: Optional. The connection ID to use to connect to Google Cloud.
    :param retry: Optional. A retry object used to retry requests. If `None` is specified, requests will not
        be retried.
    :param timeout: Optional. The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Optional. Additional metadata that is provided to the method.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple(DataplexCatalogBaseOperator.template_fields)
    operator_extra_links = (DataplexCatalogEntryTypesLink(),)

    def __init__(
        self,
        page_size: int | None = None,
        page_token: str | None = None,
        filter_by: str | None = None,
        order_by: str | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.page_size = page_size
        self.page_token = page_token
        self.filter_by = filter_by
        self.order_by = order_by

    def execute(self, context: Context):
        DataplexCatalogEntryTypesLink.persist(context=context)
        self.log.info(
            "Listing Dataplex Catalog EntryType from location %s.",
            self.location,
        )
        try:
            entry_type_on_page = self.hook.list_entry_types(
                location=self.location,
                project_id=self.project_id,
                page_size=self.page_size,
                page_token=self.page_token,
                filter_by=self.filter_by,
                order_by=self.order_by,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            self.log.info("EntryType on page: %s", entry_type_on_page)
            context["ti"].xcom_push(
                key="entry_type_page",
                value=ListEntryTypesResponse.to_dict(entry_type_on_page._response),
            )
        except Exception as ex:
            raise AirflowException(ex)

        # Constructing list to return EntryTypes in readable format
        entry_types_list = [
            MessageToDict(entry_type._pb, preserving_proto_field_name=True)
            for entry_type in next(iter(entry_type_on_page.pages)).entry_types
        ]
        return entry_types_list


class DataplexCatalogUpdateEntryTypeOperator(DataplexCatalogBaseOperator):
    """
    Update an EntryType resource.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataplexCatalogUpdateEntryTypeOperator`

    :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
    :param location: Required. The ID of the Google Cloud region that the task belongs to.
    :param update_mask: Optional. Names of fields whose values to overwrite on an entry group.
        If this parameter is absent or empty, all modifiable fields are overwritten. If such
        fields are non-required and omitted in the request body, their values are emptied.
    :param entry_type_id: Required. ID of the EntryType to update.
    :param entry_type_configuration: Required. The updated configuration body of the EntryType.
        For more details please see API documentation:
        https://cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations.entryGroups#EntryGroup
    :param validate_only: Optional. The service validates the request without performing any mutations.
    :param retry: Optional. A retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: Optional. The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Optional. Additional metadata that is provided to the method.
    :param gcp_conn_id: Optional. The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple(
        {"entry_type_id", "entry_type_configuration", "update_mask"}
        | set(DataplexCatalogBaseOperator.template_fields)
    )
    operator_extra_links = (DataplexCatalogEntryTypeLink(),)

    def __init__(
        self,
        entry_type_id: str,
        entry_type_configuration: dict | EntryType,
        update_mask: list[str] | FieldMask | None = None,
        validate_request: bool | None = False,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.entry_type_id = entry_type_id
        self.entry_type_configuration = entry_type_configuration
        self.update_mask = update_mask
        self.validate_request = validate_request

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            **super().extra_links_params,
            "entry_type_id": self.entry_type_id,
        }

    def execute(self, context: Context):
        DataplexCatalogEntryTypeLink.persist(context=context)
        if self.validate_request:
            self.log.info("Validating an Update Dataplex Catalog EntryType request.")
        else:
            self.log.info(
                "Updating Dataplex Catalog EntryType %s.",
                self.entry_type_id,
            )
        try:
            operation = self.hook.update_entry_type(
                location=self.location,
                project_id=self.project_id,
                entry_type_id=self.entry_type_id,
                entry_type_configuration=self.entry_type_configuration,
                update_mask=self.update_mask,
                validate_only=self.validate_request,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            entry_type = self.hook.wait_for_operation(timeout=self.timeout, operation=operation)

        except NotFound as ex:
            self.log.info("Specified EntryType was not found.")
            raise AirflowException(ex)
        except Exception as exc:
            raise AirflowException(exc)
        else:
            result = EntryType.to_dict(entry_type) if not self.validate_request else None

        if not self.validate_request:
            self.log.info("EntryType %s was successfully updated.", self.entry_type_id)
        return result


class DataplexCatalogCreateAspectTypeOperator(DataplexCatalogBaseOperator):
    """
    Create an AspectType resource.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataplexCatalogCreateAspectTypeOperator`

    :param aspect_type_id: Required. AspectType identifier.
    :param aspect_type_configuration: Required. AspectType configuration.
        For more details please see API documentation:
        https://cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations.aspectTypes#AspectType
    :param validate_request: Optional. If set, performs request validation, but does not actually
        execute the request.
    :param project_id: Required. The ID of the Google Cloud project where the service is used.
    :param location: Required. The ID of the Google Cloud region where the service is used.
    :param gcp_conn_id: Optional. The connection ID to use to connect to Google Cloud.
    :param retry: Optional. A retry object used to retry requests. If `None` is specified, requests will not
        be retried.
    :param timeout: Optional. The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Optional. Additional metadata that is provided to the method.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple(
        {"aspect_type_id", "aspect_type_configuration"} | set(DataplexCatalogBaseOperator.template_fields)
    )
    operator_extra_links = (DataplexCatalogAspectTypeLink(),)

    def __init__(
        self,
        aspect_type_id: str,
        aspect_type_configuration: AspectType | dict,
        validate_request: bool = False,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.aspect_type_id = aspect_type_id
        self.aspect_type_configuration = aspect_type_configuration
        self.validate_request = validate_request

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            **super().extra_links_params,
            "aspect_type_id": self.aspect_type_id,
        }

    def execute(self, context: Context):
        DataplexCatalogAspectTypeLink.persist(context=context)
        if self.validate_request:
            self.log.info("Validating a Create Dataplex Catalog AspectType request.")
        else:
            self.log.info("Creating a Dataplex Catalog AspectType.")

        try:
            operation = self.hook.create_aspect_type(
                aspect_type_id=self.aspect_type_id,
                aspect_type_configuration=self.aspect_type_configuration,
                location=self.location,
                project_id=self.project_id,
                validate_only=self.validate_request,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            aspect_type = self.hook.wait_for_operation(timeout=self.timeout, operation=operation)
        except AlreadyExists:
            aspect_type = self.hook.get_aspect_type(
                aspect_type_id=self.aspect_type_id,
                location=self.location,
                project_id=self.project_id,
            )
            self.log.info(
                "Dataplex Catalog AspectType %s already exists.",
                self.aspect_type_id,
            )
            result = AspectType.to_dict(aspect_type)
            return result
        except Exception as ex:
            raise AirflowException(ex)
        else:
            result = AspectType.to_dict(aspect_type) if not self.validate_request else None

        if not self.validate_request:
            self.log.info("Dataplex Catalog AspectType %s was successfully created.", self.aspect_type_id)
        return result


class DataplexCatalogGetAspectTypeOperator(DataplexCatalogBaseOperator):
    """
    Get an AspectType resource.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataplexCatalogGetAspectTypeOperator`

    :param aspect_type_id: Required. AspectType identifier.
    :param project_id: Required. The ID of the Google Cloud project where the service is used.
    :param location: Required. The ID of the Google Cloud region where the service is used.
    :param gcp_conn_id: Optional. The connection ID to use to connect to Google Cloud.
    :param retry: Optional. A retry object used to retry requests. If `None` is specified, requests will not
        be retried.
    :param timeout: Optional. The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Optional. Additional metadata that is provided to the method.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple(
        {"aspect_type_id"} | set(DataplexCatalogBaseOperator.template_fields)
    )
    operator_extra_links = (DataplexCatalogAspectTypeLink(),)

    def __init__(
        self,
        aspect_type_id: str,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.aspect_type_id = aspect_type_id

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            **super().extra_links_params,
            "aspect_type_id": self.aspect_type_id,
        }

    def execute(self, context: Context):
        DataplexCatalogAspectTypeLink.persist(context=context)
        self.log.info(
            "Retrieving Dataplex Catalog AspectType %s.",
            self.aspect_type_id,
        )
        try:
            aspect_type = self.hook.get_aspect_type(
                aspect_type_id=self.aspect_type_id,
                location=self.location,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except NotFound:
            self.log.info(
                "Dataplex Catalog AspectType %s not found.",
                self.aspect_type_id,
            )
            raise AirflowException(NotFound)
        except Exception as ex:
            raise AirflowException(ex)

        return AspectType.to_dict(aspect_type)


class DataplexCatalogListAspectTypesOperator(DataplexCatalogBaseOperator):
    """
    List AspectType resources.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataplexCatalogListAspectTypesOperator`

    :param filter_by: Optional. Filter to apply on the list results.
    :param order_by: Optional. Fields to order the results by.
    :param page_size: Optional. Maximum number of AspectTypes to return on the page.
    :param page_token: Optional. Token to retrieve the next page of results.
    :param project_id: Required. The ID of the Google Cloud project where the service is used.
    :param location: Required. The ID of the Google Cloud region where the service is used.
    :param gcp_conn_id: Optional. The connection ID to use to connect to Google Cloud.
    :param retry: Optional. A retry object used to retry requests. If `None` is specified, requests will not
        be retried.
    :param timeout: Optional. The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Optional. Additional metadata that is provided to the method.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple(DataplexCatalogBaseOperator.template_fields)
    operator_extra_links = (DataplexCatalogAspectTypesLink(),)

    def __init__(
        self,
        page_size: int | None = None,
        page_token: str | None = None,
        filter_by: str | None = None,
        order_by: str | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.page_size = page_size
        self.page_token = page_token
        self.filter_by = filter_by
        self.order_by = order_by

    def execute(self, context: Context):
        DataplexCatalogAspectTypesLink.persist(context=context)
        self.log.info(
            "Listing Dataplex Catalog AspectType from location %s.",
            self.location,
        )
        try:
            aspect_type_on_page = self.hook.list_aspect_types(
                location=self.location,
                project_id=self.project_id,
                page_size=self.page_size,
                page_token=self.page_token,
                filter_by=self.filter_by,
                order_by=self.order_by,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            self.log.info("AspectType on page: %s", aspect_type_on_page)
            context["ti"].xcom_push(
                key="aspect_type_page",
                value=ListAspectTypesResponse.to_dict(aspect_type_on_page._response),
            )
        except Exception as ex:
            raise AirflowException(ex)

        # Constructing list to return AspectTypes in readable format
        aspect_types_list = [
            MessageToDict(aspect_type._pb, preserving_proto_field_name=True)
            for aspect_type in next(iter(aspect_type_on_page.pages)).aspect_types
        ]
        return aspect_types_list


class DataplexCatalogUpdateAspectTypeOperator(DataplexCatalogBaseOperator):
    """
    Update an AspectType resource.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataplexCatalogUpdateAspectTypeOperator`

    :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
    :param location: Required. The ID of the Google Cloud region that the task belongs to.
    :param update_mask: Optional. Names of fields whose values to overwrite on an entry group.
        If this parameter is absent or empty, all modifiable fields are overwritten. If such
        fields are non-required and omitted in the request body, their values are emptied.
    :param aspect_type_id: Required. ID of the AspectType to update.
    :param aspect_type_configuration: Required. The updated configuration body of the AspectType.
        For more details please see API documentation:
        https://cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations.aspectTypes#AspectType
    :param validate_only: Optional. The service validates the request without performing any mutations.
    :param retry: Optional. A retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: Optional. The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Optional. Additional metadata that is provided to the method.
    :param gcp_conn_id: Optional. The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple(
        {"aspect_type_id", "aspect_type_configuration", "update_mask"}
        | set(DataplexCatalogBaseOperator.template_fields)
    )
    operator_extra_links = (DataplexCatalogAspectTypeLink(),)

    def __init__(
        self,
        aspect_type_id: str,
        aspect_type_configuration: dict | AspectType,
        update_mask: list[str] | FieldMask | None = None,
        validate_request: bool | None = False,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.aspect_type_id = aspect_type_id
        self.aspect_type_configuration = aspect_type_configuration
        self.update_mask = update_mask
        self.validate_request = validate_request

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            **super().extra_links_params,
            "aspect_type_id": self.aspect_type_id,
        }

    def execute(self, context: Context):
        DataplexCatalogAspectTypeLink.persist(context=context)
        if self.validate_request:
            self.log.info("Validating an Update Dataplex Catalog AspectType request.")
        else:
            self.log.info(
                "Updating Dataplex Catalog AspectType %s.",
                self.aspect_type_id,
            )
        try:
            operation = self.hook.update_aspect_type(
                location=self.location,
                project_id=self.project_id,
                aspect_type_id=self.aspect_type_id,
                aspect_type_configuration=self.aspect_type_configuration,
                update_mask=self.update_mask,
                validate_only=self.validate_request,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            aspect_type = self.hook.wait_for_operation(timeout=self.timeout, operation=operation)

        except NotFound as ex:
            self.log.info("Specified AspectType was not found.")
            raise AirflowException(ex)
        except Exception as exc:
            raise AirflowException(exc)
        else:
            result = AspectType.to_dict(aspect_type) if not self.validate_request else None

        if not self.validate_request:
            self.log.info("AspectType %s was successfully updated.", self.aspect_type_id)
        return result


class DataplexCatalogDeleteAspectTypeOperator(DataplexCatalogBaseOperator):
    """
    Delete an AspectType resource.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataplexCatalogDeleteAspectTypeOperator`

    :param aspect_type_id: Required. AspectType identifier.
    :param project_id: Required. The ID of the Google Cloud project where the service is used.
    :param location: Required. The ID of the Google Cloud region where the service is used.
    :param gcp_conn_id: Optional. The connection ID to use to connect to Google Cloud.
    :param retry: Optional. A retry object used to retry requests. If `None` is specified, requests will not
        be retried.
    :param timeout: Optional. The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Optional. Additional metadata that is provided to the method.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple(
        {"aspect_type_id"} | set(DataplexCatalogBaseOperator.template_fields)
    )

    def __init__(
        self,
        aspect_type_id: str,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.aspect_type_id = aspect_type_id

    def execute(self, context: Context):
        self.log.info(
            "Deleting Dataplex Catalog AspectType %s.",
            self.aspect_type_id,
        )
        try:
            operation = self.hook.delete_aspect_type(
                aspect_type_id=self.aspect_type_id,
                location=self.location,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            self.hook.wait_for_operation(timeout=self.timeout, operation=operation)

        except NotFound:
            self.log.info(
                "Dataplex Catalog AspectType %s not found.",
                self.aspect_type_id,
            )
            raise AirflowException(NotFound)
        except Exception as ex:
            raise AirflowException(ex)
        return None


class DataplexCatalogCreateEntryOperator(DataplexCatalogBaseOperator):
    """
    Create an Entry resource.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataplexCatalogCreateEntryOperator`

    :param entry_id: Required. Entry identifier. It has to be unique within an Entry Group.
        Entries corresponding to Google Cloud resources use an Entry ID format based on `full resource
        names <https://cloud.google.com/apis/design/resource_names#full_resource_name>`__.
        The format is a full resource name of the resource without the prefix double slashes in the API
        service name part of the full resource name. This allows retrieval of entries using their associated
        resource name.
        For example, if the full resource name of a resource is
        ``//library.googleapis.com/shelves/shelf1/books/book2``, then the suggested entry_id is
        ``library.googleapis.com/shelves/shelf1/books/book2``.
        It is also suggested to follow the same convention for entries corresponding to resources from
        providers or systems other than Google Cloud.
        The maximum size of the field is 4000 characters.
    :param entry_group_id: Required. EntryGroup resource name to which created Entry will belong to.
    :param entry_configuration: Required. Entry configuration.
        For more details please see API documentation:
        https://cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations.entryGroups.entries#Entry
    :param project_id: Required. The ID of the Google Cloud project where the service is used.
    :param location: Required. The ID of the Google Cloud region where the service is used.
    :param gcp_conn_id: Optional. The connection ID to use to connect to Google Cloud.
    :param retry: Optional. A retry object used to retry requests. If `None` is specified, requests will not
        be retried.
    :param timeout: Optional. The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Optional. Additional metadata that is provided to the method.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple(
        {"entry_id", "entry_group_id", "entry_configuration"}
        | set(DataplexCatalogBaseOperator.template_fields)
    )
    operator_extra_links = (DataplexCatalogEntryLink(),)

    def __init__(
        self,
        entry_id: str,
        entry_group_id: str,
        entry_configuration: Entry | dict,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.entry_id = entry_id
        self.entry_group_id = entry_group_id
        self.entry_configuration = entry_configuration

    def _validate_fields(self, entry_configuration):
        required_fields = ["name", "entry_type"]

        missing_fields = [field for field in required_fields if not entry_configuration.get(field)]

        if missing_fields:
            raise AirflowException(
                f"Missing required fields in Entry configuration: {', '.join(missing_fields)}. "
            )

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            **super().extra_links_params,
            "entry_id": self.entry_id,
            "entry_group_id": self.entry_group_id,
        }

    def execute(self, context: Context):
        DataplexCatalogEntryLink.persist(context=context)
        self._validate_fields(self.entry_configuration)
        try:
            entry = self.hook.create_entry(
                entry_id=self.entry_id,
                entry_group_id=self.entry_group_id,
                entry_configuration=self.entry_configuration,
                location=self.location,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except AlreadyExists:
            entry = self.hook.get_entry(
                entry_id=self.entry_id,
                entry_group_id=self.entry_group_id,
                location=self.location,
                project_id=self.project_id,
            )
            self.log.info(
                "Dataplex Catalog Entry %s already exists.",
                self.entry_id,
            )
            result = Entry.to_dict(entry)
            return result
        except Exception as ex:
            raise AirflowException(ex)
        else:
            result = Entry.to_dict(entry)

        self.log.info("Dataplex Catalog Entry %s was successfully created.", self.entry_id)
        return result


class DataplexCatalogGetEntryOperator(DataplexCatalogBaseOperator):
    """
    Get an Entry resource.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataplexCatalogGetEntryOperator`

    :param entry_id: Required. Entry identifier. It has to be unique within an Entry Group.
        Entries corresponding to Google Cloud resources use an Entry ID format based on `full resource
        names <https://cloud.google.com/apis/design/resource_names#full_resource_name>`__.
        The format is a full resource name of the resource without the prefix double slashes in the API
        service name part of the full resource name. This allows retrieval of entries using their associated
        resource name.
        For example, if the full resource name of a resource is
        ``//library.googleapis.com/shelves/shelf1/books/book2``, then the suggested entry_id is
        ``library.googleapis.com/shelves/shelf1/books/book2``.
        It is also suggested to follow the same convention for entries corresponding to resources from
        providers or systems other than Google Cloud.
        The maximum size of the field is 4000 characters.
    :param entry_group_id: Required. EntryGroup resource name to which created Entry will belong to.
    :param project_id: Required. The ID of the Google Cloud project where the service is used.
    :param location: Required. The ID of the Google Cloud region where the service is used.
    :param view: Optional. View to control which parts of an Entry the service should return.
    :param aspect_types: Optional. Limits the aspects returned to the provided aspect types. It only works
        for CUSTOM view.
    :param paths: Optional. Limits the aspects returned to those associated with the provided paths within
        the Entry. It only works for CUSTOM view.
    :param gcp_conn_id: Optional. The connection ID to use to connect to Google Cloud.
    :param retry: Optional. A retry object used to retry requests. If `None` is specified, requests will not
        be retried.
    :param timeout: Optional. The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Optional. Additional metadata that is provided to the method.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple(
        {"entry_id", "entry_group_id"} | set(DataplexCatalogBaseOperator.template_fields)
    )
    operator_extra_links = (DataplexCatalogEntryLink(),)

    def __init__(
        self,
        entry_id: str,
        entry_group_id: str,
        view: EntryView | str | None = None,
        aspect_types: MutableSequence[str] | None = None,
        paths: MutableSequence[str] | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.entry_id = entry_id
        self.entry_group_id = entry_group_id
        self.view = view
        self.aspect_types = aspect_types
        self.paths = paths

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            **super().extra_links_params,
            "entry_id": self.entry_id,
            "entry_group_id": self.entry_group_id,
        }

    def execute(self, context: Context):
        DataplexCatalogEntryLink.persist(context=context)
        self.log.info(
            "Retrieving Dataplex Catalog Entry %s.",
            self.entry_id,
        )
        try:
            entry = self.hook.get_entry(
                entry_id=self.entry_id,
                entry_group_id=self.entry_group_id,
                view=self.view,
                aspect_types=self.aspect_types,
                paths=self.paths,
                location=self.location,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except NotFound:
            self.log.info(
                "Dataplex Catalog Entry %s not found.",
                self.entry_id,
            )
            raise AirflowException(NotFound)
        except Exception as ex:
            raise AirflowException(ex)

        return Entry.to_dict(entry)


class DataplexCatalogListEntriesOperator(DataplexCatalogBaseOperator):
    r"""
    List Entry resources.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataplexCatalogListEntriesOperator`

    :param entry_group_id: Required. EntryGroup resource name to which created Entry belongs to.
    :param filter_by: Optional. A filter on the entries to return. Filters are case-sensitive.
        You can filter the request by the following fields:

        -  entry_type
        -  entry_source.display_name

        The comparison operators are =, !=, <, >, <=, >=. The service compares strings according to
        lexical order.
        You can use the logical operators AND, OR, NOT in the filter. You can use Wildcard "*", but for
        entry_type you need to provide the full project id or number.
        Example filter expressions:

        - "entry_source.display_name=AnExampleDisplayName"
        - "entry_type=projects/example-project/locations/global/entryTypes/example-entry_type"
        - "entry_type=projects/example-project/locations/us/entryTypes/a\*
           OR entry_type=projects/another-project/locations/\*"
        - "NOT entry_source.display_name=AnotherExampleDisplayName".

    :param page_size: Optional. Number of items to return per page. If there are remaining results,
        the service returns a next_page_token. If unspecified, the service returns at most 10 Entries.
        The maximum value is 100; values above 100 will be coerced to 100.
    :param page_token: Optional. Page token received from a previous ``ListEntries`` call. Provide
        this to retrieve the subsequent page.
    :param project_id: Required. The ID of the Google Cloud project where the service is used.
    :param location: Required. The ID of the Google Cloud region where the service is used.
    :param gcp_conn_id: Optional. The connection ID to use to connect to Google Cloud.
    :param retry: Optional. A retry object used to retry requests. If `None` is specified, requests will not
        be retried.
    :param timeout: Optional. The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Optional. Additional metadata that is provided to the method.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple(DataplexCatalogBaseOperator.template_fields)
    operator_extra_links = (DataplexCatalogEntryGroupLink(),)

    def __init__(
        self,
        entry_group_id: str,
        page_size: int | None = None,
        page_token: str | None = None,
        filter_by: str | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.entry_group_id = entry_group_id
        self.page_size = page_size
        self.page_token = page_token
        self.filter_by = filter_by

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            **super().extra_links_params,
            "entry_group_id": self.entry_group_id,
        }

    def execute(self, context: Context):
        DataplexCatalogEntryGroupLink.persist(context=context)
        self.log.info(
            "Listing Dataplex Catalog Entry from location %s.",
            self.location,
        )
        try:
            entries_on_page = self.hook.list_entries(
                entry_group_id=self.entry_group_id,
                location=self.location,
                project_id=self.project_id,
                page_size=self.page_size,
                page_token=self.page_token,
                filter_by=self.filter_by,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            self.log.info("Entries on page: %s", entries_on_page)
            context["ti"].xcom_push(
                key="entry_page",
                value=ListEntriesResponse.to_dict(entries_on_page._response),
            )
        except Exception as ex:
            raise AirflowException(ex)

        # Constructing list to return Entries in readable format
        entries_list = [
            MessageToDict(entry._pb, preserving_proto_field_name=True)
            for entry in next(iter(entries_on_page.pages)).entries
        ]
        return entries_list


class DataplexCatalogSearchEntriesOperator(DataplexCatalogBaseOperator):
    """
    Search for Entries matching the given query and scope.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataplexCatalogSearchEntriesOperator`

    :param query: Required. The query against which entries in scope should be matched. The query
        syntax is defined in `Search syntax for Dataplex Catalog
        <https://cloud.google.com/dataplex/docs/search-syntax>`__.
    :param order_by: Optional. Specifies the ordering of results. Supported values are:

        - ``relevance`` (default)
        - ``last_modified_timestamp``
        - ``last_modified_timestamp asc``

    :param scope: Optional. The scope under which the search should be operating. It must either be
        ``organizations/<org_id>`` or ``projects/<project_ref>``. If it is unspecified, it
        defaults to the organization where the project provided in ``name`` is located.
    :param page_size: Optional. Number of items to return per page. If there are remaining results,
        the service returns a next_page_token. If unspecified, the service returns at most 10 Entries.
        The maximum value is 100; values above 100 will be coerced to 100.
    :param page_token: Optional. Page token received from a previous ``ListEntries`` call. Provide
        this to retrieve the subsequent page.
    :param project_id: Required. The ID of the Google Cloud project where the service is used.
    :param location: Required. The ID of the Google Cloud region where the service is used.
    :param gcp_conn_id: Optional. The connection ID to use to connect to Google Cloud.
    :param retry: Optional. A retry object used to retry requests. If `None` is specified, requests will not
        be retried.
    :param timeout: Optional. The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Optional. Additional metadata that is provided to the method.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple({"query"} | set(DataplexCatalogBaseOperator.template_fields))

    def __init__(
        self,
        query: str,
        order_by: str | None = None,
        scope: str | None = None,
        page_size: int | None = None,
        page_token: str | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.query = query
        self.page_size = page_size
        self.page_token = page_token
        self.order_by = order_by
        self.scope = scope

    def execute(self, context: Context):
        self.log.info(
            "Listing Entries from location %s matching the given query %s and scope %s.",
            self.location,
            self.query,
            self.scope,
        )
        try:
            entries_on_page = self.hook.search_entries(
                query=self.query,
                location=self.location,
                project_id=self.project_id,
                page_size=self.page_size,
                page_token=self.page_token,
                order_by=self.order_by,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            self.log.info("Entries on page: %s", entries_on_page)
            context["ti"].xcom_push(
                key="entry_page",
                value=SearchEntriesResponse.to_dict(entries_on_page._response),
            )
        except Exception as ex:
            raise AirflowException(ex)

        # Constructing list to return Entries in readable format
        entries_list = [
            MessageToDict(entry._pb, preserving_proto_field_name=True)
            for entry in next(iter(entries_on_page.pages)).results
        ]
        return entries_list


class DataplexCatalogLookupEntryOperator(DataplexCatalogBaseOperator):
    """
    Look up a single Entry by name using the permission on the source system.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataplexCatalogLookupEntryOperator`

    :param entry_id: Required. Entry identifier. It has to be unique within an Entry Group.
        Entries corresponding to Google Cloud resources use an Entry ID format based on `full resource
        names <https://cloud.google.com/apis/design/resource_names#full_resource_name>`__.
        The format is a full resource name of the resource without the prefix double slashes in the API
        service name part of the full resource name. This allows retrieval of entries using their associated
        resource name.
        For example, if the full resource name of a resource is
        ``//library.googleapis.com/shelves/shelf1/books/book2``, then the suggested entry_id is
        ``library.googleapis.com/shelves/shelf1/books/book2``.
        It is also suggested to follow the same convention for entries corresponding to resources from
        providers or systems other than Google Cloud.
        The maximum size of the field is 4000 characters.
    :param entry_group_id: Required. EntryGroup resource name to which created Entry will belong to.
    :param project_id: Required. The ID of the Google Cloud project where the service is used.
    :param location: Required. The ID of the Google Cloud region where the service is used.
    :param view: Optional. View to control which parts of an Entry the service should return.
    :param aspect_types: Optional. Limits the aspects returned to the provided aspect types. It only works
        for CUSTOM view.
    :param paths: Optional. Limits the aspects returned to those associated with the provided paths within
        the Entry. It only works for CUSTOM view.
    :param gcp_conn_id: Optional. The connection ID to use to connect to Google Cloud.
    :param retry: Optional. A retry object used to retry requests. If `None` is specified, requests will not
        be retried.
    :param timeout: Optional. The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Optional. Additional metadata that is provided to the method.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple(
        {"entry_id", "entry_group_id"} | set(DataplexCatalogBaseOperator.template_fields)
    )
    operator_extra_links = (DataplexCatalogEntryLink(),)

    def __init__(
        self,
        entry_id: str,
        entry_group_id: str,
        view: EntryView | str | None = None,
        aspect_types: MutableSequence[str] | None = None,
        paths: MutableSequence[str] | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.entry_id = entry_id
        self.entry_group_id = entry_group_id
        self.view = view
        self.aspect_types = aspect_types
        self.paths = paths

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            **super().extra_links_params,
            "entry_id": self.entry_id,
            "entry_group_id": self.entry_group_id,
        }

    def execute(self, context: Context):
        DataplexCatalogEntryLink.persist(context=context)
        self.log.info(
            "Looking for Dataplex Catalog Entry %s.",
            self.entry_id,
        )
        try:
            entry = self.hook.lookup_entry(
                entry_id=self.entry_id,
                entry_group_id=self.entry_group_id,
                view=self.view,
                aspect_types=self.aspect_types,
                paths=self.paths,
                location=self.location,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except NotFound:
            self.log.info(
                "Dataplex Catalog Entry %s not found.",
                self.entry_id,
            )
            raise AirflowException(NotFound)
        except Exception as ex:
            raise AirflowException(ex)

        return Entry.to_dict(entry)


class DataplexCatalogUpdateEntryOperator(DataplexCatalogBaseOperator):
    """
    Update an Entry resource.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataplexCatalogUpdateEntryOperator`

    :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
    :param location: Required. The ID of the Google Cloud region that the task belongs to.
    :param entry_id: Required. Entry identifier. It has to be unique within an Entry Group.
        Entries corresponding to Google Cloud resources use an Entry ID format based on `full resource
        names <https://cloud.google.com/apis/design/resource_names#full_resource_name>`__.
        The format is a full resource name of the resource without the prefix double slashes in the API
        service name part of the full resource name. This allows retrieval of entries using their
        associated resource name.
        For example, if the full resource name of a resource is
        ``//library.googleapis.com/shelves/shelf1/books/book2``, then the suggested entry_id is
        ``library.googleapis.com/shelves/shelf1/books/book2``.
        It is also suggested to follow the same convention for entries corresponding to resources from
        providers or systems other than Google Cloud.
        The maximum size of the field is 4000 characters.
    :param entry_group_id: Required. EntryGroup resource name to which created Entry belongs to.
    :param entry_configuration: Required. The updated configuration body of the Entry.
    :param allow_missing: Optional. If set to true and entry doesn't exist, the service will create it.
    :param delete_missing_aspects: Optional. If set to true and the aspect_keys specify aspect
        ranges, the service deletes any existing aspects from that range that were not provided
        in the request.
    :param aspect_keys: Optional. The map keys of the Aspects which the service should modify.
        It supports the following syntax:

        - ``<aspect_type_reference>`` - matches an aspect of the given type and empty path.
        - ``<aspect_type_reference>@path`` - matches an aspect of the given type and specified path.
            For example, to attach an aspect to a field that is specified by the ``schema``
            aspect, the path should have the format ``Schema.<field_name>``.
        - ``<aspect_type_reference>@*`` - matches aspects of the given type for all paths.
        - ``*@path`` - matches aspects of all types on the given path.

        The service will not remove existing aspects matching the syntax unless ``delete_missing_aspects``
        is set to true.
        If this field is left empty, the service treats it as specifying exactly those Aspects present
        in the request.
    :param retry: Optional. A retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: Optional. The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Optional. Additional metadata that is provided to the method.
    :param gcp_conn_id: Optional. The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple(
        {"entry_id", "entry_group_id", "entry_configuration"}
        | set(DataplexCatalogBaseOperator.template_fields)
    )
    operator_extra_links = (DataplexCatalogEntryLink(),)

    def __init__(
        self,
        entry_id: str,
        entry_group_id: str,
        entry_configuration: dict | Entry,
        allow_missing: bool | None = False,
        delete_missing_aspects: bool | None = False,
        aspect_keys: MutableSequence[str] | None = None,
        update_mask: list[str] | FieldMask | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.entry_id = entry_id
        self.entry_group_id = entry_group_id
        self.entry_configuration = entry_configuration
        self.update_mask = update_mask
        self.allow_missing = allow_missing
        self.delete_missing_aspects = delete_missing_aspects
        self.aspect_keys = aspect_keys

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            **super().extra_links_params,
            "entry_id": self.entry_id,
            "entry_group_id": self.entry_group_id,
        }

    def execute(self, context: Context):
        DataplexCatalogEntryLink.persist(context=context)
        try:
            entry = self.hook.update_entry(
                location=self.location,
                project_id=self.project_id,
                entry_id=self.entry_id,
                entry_group_id=self.entry_group_id,
                entry_configuration=self.entry_configuration,
                update_mask=self.update_mask,
                allow_missing=self.allow_missing,
                delete_missing_aspects=self.delete_missing_aspects,
                aspect_keys=self.aspect_keys,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )

        except NotFound as ex:
            self.log.info("Specified Entry was not found.")
            raise AirflowException(ex)
        except Exception as exc:
            raise AirflowException(exc)
        else:
            result = Entry.to_dict(entry)
        self.log.info("Entry %s was successfully updated.", self.entry_id)
        return result


class DataplexCatalogDeleteEntryOperator(DataplexCatalogBaseOperator):
    """
    Delete an Entry resource.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataplexCatalogDeleteEntryOperator`

    :param entry_id: Required. Entry identifier. It has to be unique within an Entry Group.
        Entries corresponding to Google Cloud resources use an Entry ID format based on `full resource
        names <https://cloud.google.com/apis/design/resource_names#full_resource_name>`__.
        The format is a full resource name of the resource without the prefix double slashes in the API
        service name part of the full resource name. This allows retrieval of entries using their
        associated resource name.
        For example, if the full resource name of a resource is
        ``//library.googleapis.com/shelves/shelf1/books/book2``, then the suggested entry_id is
        ``library.googleapis.com/shelves/shelf1/books/book2``.
        It is also suggested to follow the same convention for entries corresponding to resources from
        providers or systems other than Google Cloud.
        The maximum size of the field is 4000 characters.
    :param entry_group_id: Required. EntryGroup resource name to which created Entry will belong to.
    :param project_id: Required. The ID of the Google Cloud project where the service is used.
    :param location: Required. The ID of the Google Cloud region where the service is used.
    :param gcp_conn_id: Optional. The connection ID to use to connect to Google Cloud.
    :param retry: Optional. A retry object used to retry requests. If `None` is specified, requests will not
        be retried.
    :param timeout: Optional. The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Optional. Additional metadata that is provided to the method.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple(
        {"entry_id", "entry_group_id"} | set(DataplexCatalogBaseOperator.template_fields)
    )

    def __init__(
        self,
        entry_id: str,
        entry_group_id: str,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.entry_id = entry_id
        self.entry_group_id = entry_group_id

    def execute(self, context: Context):
        self.log.info(
            "Deleting Dataplex Catalog Entry %s.",
            self.entry_id,
        )
        try:
            entry = self.hook.delete_entry(
                entry_id=self.entry_id,
                entry_group_id=self.entry_group_id,
                location=self.location,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )

        except NotFound:
            self.log.info(
                "Dataplex Catalog Entry %s not found.",
                self.entry_id,
            )
            raise AirflowException(NotFound)
        except Exception as ex:
            raise AirflowException(ex)
        return Entry.to_dict(entry)
