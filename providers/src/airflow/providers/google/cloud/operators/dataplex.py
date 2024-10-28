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
from typing import TYPE_CHECKING, Any, Sequence

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.triggers.dataplex import (
    DataplexDataProfileJobTrigger,
    DataplexDataQualityJobTrigger,
)

if TYPE_CHECKING:
    from google.protobuf.field_mask_pb2 import FieldMask

    from airflow.utils.context import Context

from google.api_core.exceptions import AlreadyExists, GoogleAPICallError
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.api_core.retry import Retry, exponential_sleep_generator
from google.cloud.dataplex_v1.types import Asset, DataScan, DataScanJob, Lake, Task, Zone
from googleapiclient.errors import HttpError

from airflow.configuration import conf
from airflow.providers.google.cloud.hooks.dataplex import AirflowDataQualityScanException, DataplexHook
from airflow.providers.google.cloud.links.dataplex import (
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

    def execute(self, context: Context) -> dict:
        hook = DataplexHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Creating Dataplex task %s", self.dataplex_task_id)
        DataplexTaskLink.persist(context=context, task_instance=self)

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

    def execute(self, context: Context) -> list[dict]:
        hook = DataplexHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Listing Dataplex tasks from lake %s", self.lake_id)
        DataplexTasksLink.persist(context=context, task_instance=self)

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

    def execute(self, context: Context) -> dict:
        hook = DataplexHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Retrieving Dataplex task %s", self.dataplex_task_id)
        DataplexTaskLink.persist(context=context, task_instance=self)

        task = hook.get_task(
            project_id=self.project_id,
            region=self.region,
            lake_id=self.lake_id,
            dataplex_task_id=self.dataplex_task_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        DataplexTasksLink.persist(context=context, task_instance=self)
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
        DataplexLakeLink.persist(
            context=context,
            task_instance=self,
        )
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
        DataplexLakeLink.persist(context=context, task_instance=self)
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
        except GoogleAPICallError as e:
            raise AirflowException(f"Error creating Data Quality scan {self.data_scan_id}", e)

        return self.data_scan_id


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
        if job_state == DataScanJob.State.FAILED:
            raise AirflowException(f"Job failed:\n{job_id}")
        if job_state == DataScanJob.State.CANCELLED:
            raise AirflowException(f"Job was cancelled:\n{job_id}")
        if job_state == DataScanJob.State.SUCCEEDED:
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
        if job_state == DataScanJob.State.FAILED:
            raise AirflowException(f"Job failed:\n{job_id}")
        if job_state == DataScanJob.State.CANCELLED:
            raise AirflowException(f"Job was cancelled:\n{job_id}")
        if job_state == DataScanJob.State.SUCCEEDED:
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

            if job.state == DataScanJob.State.FAILED:
                raise AirflowException(f"Data Profile job failed: {job_id}")
            if job.state == DataScanJob.State.SUCCEEDED:
                self.log.info("Data Profile job executed successfully.")
            else:
                self.log.info("Data Profile job execution returned status: %s", job.status)

        return job_id

    def execute_complete(self, context, event=None) -> None:
        """
        Act as a callback for when the trigger fires - returns immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        job_state = event["job_state"]
        job_id = event["job_id"]
        if job_state == DataScanJob.State.FAILED:
            raise AirflowException(f"Job failed:\n{job_id}")
        if job_state == DataScanJob.State.CANCELLED:
            raise AirflowException(f"Job was cancelled:\n{job_id}")
        if job_state == DataScanJob.State.SUCCEEDED:
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
