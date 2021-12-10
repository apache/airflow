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
from time import sleep
from typing import Any, Dict, Optional

from google.api_core.retry import exponential_sleep_generator
from googleapiclient.errors import HttpError

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.dataplex import DataplexHook


class DataplexCreateTaskOperator(BaseOperator):
    """
    Creates a task resource within a lake.

    :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
    :type project_id: str
    :param region: Required. The ID of the Google Cloud region that the task belongs to.
    :type region: str
    :param lake_id: Required. The ID of the Google Cloud lake that the task belongs to.
    :type lake_id: str
    :param body:  Required. The Request body contains an instance of Task.
    :type body: Dict[str, Any]
    :param dataplex_task_id: Required. Task identifier.
    :type dataplex_task_id: str
    :param validate_only: Optional. Only validate the request, but do not perform mutations. The default is
        false.
    :type validate_only: bool
    :param api_version: The version of the api that will be requested for example 'v3'.
    :type api_version: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :type delegate_to: str
    :param asynchronous: Flag informing should the Dataplex task be created asynchronously.
        This is useful for long running creating tasks and
        waiting on them asynchronously using the DataplexTaskSensor
    :type asynchronous: bool
    """

    template_fields = ("project_id", "dataplex_task_id", "body", "validate_only", "delegate_to")
    template_fields_renderers = {'body': 'json'}

    def __init__(
        self,
        project_id: str,
        region: str,
        lake_id: str,
        body: Dict[str, Any],
        dataplex_task_id: str,
        validate_only: Optional[bool] = None,
        api_version: str = "v1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str = None,
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
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.asynchronous = asynchronous

    def execute(self, context: dict) -> dict:
        hook = DataplexHook(
            gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to, api_version=self.api_version
        )
        if not self.asynchronous:
            self.log.info(f"Creating Dataplex task {self.dataplex_task_id}")
        else:
            self.log.info(f"Creating Dataplex task {self.dataplex_task_id} asynchronously")
        try:
            operation = hook.create_task(
                project_id=self.project_id,
                region=self.region,
                lake_id=self.lake_id,
                body=self.body,
                dataplex_task_id=self.dataplex_task_id,
                validate_only=self.validate_only,
            )
            if not self.asynchronous:
                self.log.info(f"Waiting for Dataplex task {self.dataplex_task_id} to be created")
                task = hook.wait_for_operation(operation)
                self.log.info(f"Task {self.dataplex_task_id} created successfully")
            else:
                self.log.info(f"Is operation done already? {operation['done']}")
                return operation
        except HttpError as err:
            if err.resp.status not in (409, '409'):
                raise
            self.log.info(f"Task {self.dataplex_task_id} already exists")
            task = hook.get_task(
                project_id=self.project_id,
                region=self.region,
                lake_id=self.lake_id,
                dataplex_task_id=self.dataplex_task_id,
            )
            # Wait for task to be ready
            for time_to_wait in exponential_sleep_generator(initial=10, maximum=120):
                if task['state'] != 'CREATING':
                    break
                sleep(time_to_wait)
                task = hook.get_task(
                    project_id=self.project_id,
                    region=self.region,
                    lake_id=self.lake_id,
                    dataplex_task_id=self.dataplex_task_id,
                )
        return task


class DataplexDeleteTaskOperator(BaseOperator):
    """
    Delete the task resource.

    :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
    :type project_id: str
    :param region: Required. The ID of the Google Cloud region that the task belongs to.
    :type region: str
    :param lake_id: Required. The ID of the Google Cloud lake that the task belongs to.
    :type lake_id: str
    :param dataplex_task_id: Required. Task identifier.
    :type dataplex_task_id: str
    :param api_version: The version of the api that will be requested for example 'v3'.
    :type api_version: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = ("project_id", "dataplex_task_id", "delegate_to")

    def __init__(
        self,
        project_id: str,
        region: str,
        lake_id: str,
        dataplex_task_id: str,
        api_version: str = "v1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.region = region
        self.lake_id = lake_id
        self.dataplex_task_id = dataplex_task_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def execute(self, context: Dict):
        hook = DataplexHook(
            gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to, api_version=self.api_version
        )
        self.log.info(f"Deleting Dataplex task {self.dataplex_task_id}")

        operation = hook.delete_task(
            project_id=self.project_id,
            region=self.region,
            lake_id=self.lake_id,
            dataplex_task_id=self.dataplex_task_id,
        )
        hook.wait_for_operation(operation)
        self.log.info(f"Dataplex task {self.dataplex_task_id} deleted successfully!")


class DataplexListTasksOperator(BaseOperator):
    """
    Lists tasks under the given lake.

    :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
    :type project_id: str
    :param region: Required. The ID of the Google Cloud region that the task belongs to.
    :type region: str
    :param lake_id: Required. The ID of the Google Cloud lake that the task belongs to.
    :type lake_id: str
    :param page_size: Optional. Maximum number of tasks to return. The service may return fewer than this
        value. If unspecified, at most 10 tasks will be returned. The maximum value is 1000; values above 1000
        will be coerced to 1000.
    :type page_size: Optional[int]
    :param page_token: Optional. Page token received from a previous ListZones call. Provide this to retrieve
        the subsequent page. When paginating, all other parameters provided to ListZones must match the call
        that provided the page token.
    :type page_token: Optional[str]
    :param filter: Optional. Filter request.
    :type filter: Optional[str]
    :param order_by: Optional. Order by fields for the result.
    :type order_by: Optional[str]
    :param api_version: The version of the api that will be requested for example 'v3'.
    :type api_version: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = ("project_id", "page_size", "page_token", "filter", "order_by", "delegate_to")

    def __init__(
        self,
        project_id: str,
        region: str,
        lake_id: str,
        page_size: Optional[int] = None,
        page_token: Optional[str] = None,
        filter: Optional[str] = None,
        order_by: Optional[str] = None,
        api_version: str = "v1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str = None,
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
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def execute(self, context: Dict):
        hook = DataplexHook(
            gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to, api_version=self.api_version
        )
        self.log.info(f"Listing Dataplex tasks from lake {self.lake_id}")

        tasks = hook.list_tasks(
            project_id=self.project_id,
            region=self.region,
            lake_id=self.lake_id,
            page_size=self.page_size,
            page_token=self.page_token,
            filter=self.filter,
            order_by=self.order_by,
        )
        return tasks


class DataplexGetTaskOperator(BaseOperator):
    """
    Get task resource.

    :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
    :type project_id: str
    :param region: Required. The ID of the Google Cloud region that the task belongs to.
    :type region: str
    :param lake_id: Required. The ID of the Google Cloud lake that the task belongs to.
    :type lake_id: str
    :param dataplex_task_id: Required. Task identifier.
    :type dataplex_task_id: str
    :param api_version: The version of the api that will be requested for example 'v3'.
    :type api_version: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = ("project_id", "dataplex_task_id", "delegate_to")

    def __init__(
        self,
        project_id: str,
        region: str,
        lake_id: str,
        dataplex_task_id: str,
        api_version: str = "v1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.region = region
        self.lake_id = lake_id
        self.dataplex_task_id = dataplex_task_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def execute(self, context: Dict):
        hook = DataplexHook(
            gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to, api_version=self.api_version
        )
        self.log.info(f"Retrieving Dataplex task {self.dataplex_task_id}")

        task = hook.get_task(
            project_id=self.project_id,
            region=self.region,
            lake_id=self.lake_id,
            dataplex_task_id=self.dataplex_task_id,
        )
        return task
