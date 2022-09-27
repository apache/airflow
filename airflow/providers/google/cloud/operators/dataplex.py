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

from time import sleep
from typing import TYPE_CHECKING, Any, Sequence

if TYPE_CHECKING:
    from airflow.utils.context import Context

from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.api_core.retry import Retry, exponential_sleep_generator
from google.cloud.dataplex_v1.types import Task
from googleapiclient.errors import HttpError

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.dataplex import DataplexHook
from airflow.providers.google.cloud.links.dataplex import DataplexTaskLink, DataplexTasksLink


class DataplexCreateTaskOperator(BaseOperator):
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
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
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
        "delegate_to",
        "impersonation_chain",
    )
    template_fields_renderers = {'body': 'json'}
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
        delegate_to: str | None = None,
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
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        self.asynchronous = asynchronous

    def execute(self, context: Context) -> dict:
        hook = DataplexHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
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
            if err.resp.status not in (409, '409'):
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
                if task['state'] != 'CREATING':
                    break
                sleep(time_to_wait)

        return Task.to_dict(task)


class DataplexDeleteTaskOperator(BaseOperator):
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
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields = ("project_id", "dataplex_task_id", "delegate_to", "impersonation_chain")

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
        delegate_to: str | None = None,
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
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = DataplexHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
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


class DataplexListTasksOperator(BaseOperator):
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
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
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
        "delegate_to",
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
        delegate_to: str | None = None,
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
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> list[dict]:
        hook = DataplexHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
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


class DataplexGetTaskOperator(BaseOperator):
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
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields = ("project_id", "dataplex_task_id", "delegate_to", "impersonation_chain")
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
        delegate_to: str | None = None,
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
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> dict:
        hook = DataplexHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
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
        return Task.to_dict(task)
