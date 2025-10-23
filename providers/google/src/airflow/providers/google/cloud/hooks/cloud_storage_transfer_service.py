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
"""
This module contains a Google Storage Transfer Service Hook.

.. spelling::

    ListTransferJobsAsyncPager
    StorageTransferServiceAsyncClient

"""

from __future__ import annotations

import json
import logging
import time
import warnings
from collections.abc import Sequence
from copy import deepcopy
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from google.cloud.storage_transfer_v1 import (
    ListTransferJobsRequest,
    RunTransferJobRequest,
    StorageTransferServiceAsyncClient,
    TransferJob,
    TransferOperation,
)
from google.protobuf.json_format import MessageToDict
from googleapiclient.discovery import Resource, build
from googleapiclient.errors import HttpError

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import (
    PROVIDE_PROJECT_ID,
    GoogleBaseAsyncHook,
    GoogleBaseHook,
)

if TYPE_CHECKING:
    from google.api_core import operation_async
    from google.cloud.storage_transfer_v1.services.storage_transfer_service.pagers import (
        ListTransferJobsAsyncPager,
    )
    from google.longrunning import operations_pb2
    from proto import Message

log = logging.getLogger(__name__)

# Time to sleep between active checks of the operation results
TIME_TO_SLEEP_IN_SECONDS = 10


class GcpTransferJobsStatus:
    """Google Cloud Transfer job status."""

    ENABLED = "ENABLED"
    DISABLED = "DISABLED"
    DELETED = "DELETED"


class GcpTransferOperationStatus:
    """Google Cloud Transfer operation status."""

    IN_PROGRESS = "IN_PROGRESS"
    PAUSED = "PAUSED"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    ABORTED = "ABORTED"


# A list of keywords used to build a request or response
ACCESS_KEY_ID = "accessKeyId"
ALREADY_EXISTING_IN_SINK = "overwriteObjectsAlreadyExistingInSink"
AWS_ACCESS_KEY = "awsAccessKey"
AWS_SECRET_ACCESS_KEY = "secretAccessKey"
AWS_S3_DATA_SOURCE = "awsS3DataSource"
AWS_ROLE_ARN = "roleArn"
BODY = "body"
BUCKET_NAME = "bucketName"
COUNTERS = "counters"
DAY = "day"
DESCRIPTION = "description"
FILTER = "filter"
FILTER_JOB_NAMES = "job_names"
FILTER_PROJECT_ID = "project_id"
GCS_DATA_SINK = "gcsDataSink"
GCS_DATA_SOURCE = "gcsDataSource"
HOURS = "hours"
HTTP_DATA_SOURCE = "httpDataSource"
INCLUDE_PREFIXES = "includePrefixes"
JOB_NAME = "name"
LIST_URL = "list_url"
METADATA = "metadata"
MINUTES = "minutes"
MONTH = "month"
NAME = "name"
OBJECT_CONDITIONS = "object_conditions"
OPERATIONS = "operations"
OVERWRITE_OBJECTS_ALREADY_EXISTING_IN_SINK = "overwriteObjectsAlreadyExistingInSink"
PATH = "path"
PROJECT_ID = "projectId"
SCHEDULE = "schedule"
SCHEDULE_END_DATE = "scheduleEndDate"
SCHEDULE_START_DATE = "scheduleStartDate"
SECONDS = "seconds"
SECRET_ACCESS_KEY = "secretAccessKey"
START_TIME_OF_DAY = "startTimeOfDay"
STATUS = "status"
STATUS1 = "status"
TRANSFER_JOB = "transfer_job"
TRANSFER_JOBS = "transferJobs"
TRANSFER_JOB_FIELD_MASK = "update_transfer_job_field_mask"
TRANSFER_OPERATIONS = "transferOperations"
TRANSFER_OPTIONS = "transfer_options"
TRANSFER_SPEC = "transferSpec"
YEAR = "year"
ALREADY_EXIST_CODE = 409

NEGATIVE_STATUSES = {GcpTransferOperationStatus.FAILED, GcpTransferOperationStatus.ABORTED}


def gen_job_name(job_name: str) -> str:
    """
    Add a unique suffix to the job name.

    :param job_name:
    :return: job_name with suffix
    """
    uniq = int(time.time())
    return f"{job_name}_{uniq}"


class CloudDataTransferServiceHook(GoogleBaseHook):
    """
    Google Storage Transfer Service functionalities.

    All methods in the hook with *project_id* in the signature must be called
    with keyword arguments rather than positional.
    """

    def __init__(
        self,
        api_version: str = "v1",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )
        self.api_version = api_version
        self._conn = None

    def get_conn(self) -> Resource:
        """
        Retrieve connection to Google Storage Transfer service.

        :return: Google Storage Transfer service object
        """
        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build(
                "storagetransfer", self.api_version, http=http_authorized, cache_discovery=False
            )
        return self._conn

    def create_transfer_job(self, body: dict) -> dict:
        """
        Create a transfer job that runs periodically.

        :param body: (Required) The request body, as described in
            https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs/patch#request-body
        :return: The transfer job. See:
            https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs#TransferJob
        """
        body = self._inject_project_id(body, BODY, PROJECT_ID)

        try:
            transfer_job = (
                self.get_conn().transferJobs().create(body=body).execute(num_retries=self.num_retries)
            )
        except HttpError as e:
            # If status code "Conflict"
            # https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferOperations#Code.ENUM_VALUES.ALREADY_EXISTS
            # we should try to find this job
            job_name = body.get(JOB_NAME, "")
            if int(e.resp.status) == ALREADY_EXIST_CODE and job_name:
                transfer_job = self.get_transfer_job(job_name=job_name, project_id=body.get(PROJECT_ID))
                # Generate new job_name, if jobs status is deleted
                # and try to create this job again
                if transfer_job.get(STATUS) == GcpTransferJobsStatus.DELETED:
                    body[JOB_NAME] = gen_job_name(job_name)
                    self.log.info(
                        "Job `%s` has been soft deleted. Creating job with new name `%s`",
                        job_name,
                        {body[JOB_NAME]},
                    )

                    return (
                        self.get_conn().transferJobs().create(body=body).execute(num_retries=self.num_retries)
                    )
                if transfer_job.get(STATUS) == GcpTransferJobsStatus.DISABLED:
                    return self.enable_transfer_job(job_name=job_name, project_id=body.get(PROJECT_ID))
            else:
                raise e
        self.log.info("Created job %s", transfer_job[NAME])
        return transfer_job

    @GoogleBaseHook.fallback_to_default_project_id
    def get_transfer_job(self, job_name: str, project_id: str) -> dict:
        """
        Get latest state of a long-running Google Storage Transfer Service job.

        :param job_name: (Required) Name of the job to be fetched
        :param project_id: (Optional) the ID of the project that owns the Transfer
            Job. If set to None or missing, the default project_id from the Google Cloud
            connection is used.
        :return: Transfer Job
        """
        return (
            self.get_conn()
            .transferJobs()
            .get(jobName=job_name, projectId=project_id)
            .execute(num_retries=self.num_retries)
        )

    def list_transfer_job(self, request_filter: dict | None = None, **kwargs) -> list[dict]:
        """
        List long-running operations in Google Storage Transfer Service.

        A filter can be specified to match only certain entries.

        :param request_filter: (Required) A request filter, as described in
            https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs/list#body.QUERY_PARAMETERS.filter
        :return: List of Transfer Jobs
        """
        # To preserve backward compatibility
        # TODO: remove one day
        if request_filter is None:
            if "filter" in kwargs:
                request_filter = kwargs["filter"]
                if not isinstance(request_filter, dict):
                    raise ValueError(f"The request_filter should be dict and is {type(request_filter)}")
                warnings.warn(
                    "Use 'request_filter' instead of 'filter'",
                    AirflowProviderDeprecationWarning,
                    stacklevel=2,
                )
            else:
                raise TypeError("list_transfer_job missing 1 required positional argument: 'request_filter'")

        conn = self.get_conn()
        request_filter = self._inject_project_id(request_filter, FILTER, FILTER_PROJECT_ID)
        request = conn.transferJobs().list(filter=json.dumps(request_filter))
        jobs: list[dict] = []

        while request is not None:
            response = request.execute(num_retries=self.num_retries)
            jobs.extend(response[TRANSFER_JOBS])

            request = conn.transferJobs().list_next(previous_request=request, previous_response=response)

        return jobs

    @GoogleBaseHook.fallback_to_default_project_id
    def enable_transfer_job(self, job_name: str, project_id: str) -> dict:
        """
        Make new transfers be performed based on the schedule.

        :param job_name: (Required) Name of the job to be updated
        :param project_id: (Optional) the ID of the project that owns the Transfer
            Job. If set to None or missing, the default project_id from the Google Cloud
            connection is used.
        :return: If successful, TransferJob.
        """
        return (
            self.get_conn()
            .transferJobs()
            .patch(
                jobName=job_name,
                body={
                    PROJECT_ID: project_id,
                    TRANSFER_JOB: {STATUS1: GcpTransferJobsStatus.ENABLED},
                    TRANSFER_JOB_FIELD_MASK: STATUS1,
                },
            )
            .execute(num_retries=self.num_retries)
        )

    def update_transfer_job(self, job_name: str, body: dict) -> dict:
        """
        Update a transfer job that runs periodically.

        :param job_name: (Required) Name of the job to be updated
        :param body: A request body, as described in
            https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs/patch#request-body
        :return: If successful, TransferJob.
        """
        body = self._inject_project_id(body, BODY, PROJECT_ID)
        return (
            self.get_conn()
            .transferJobs()
            .patch(jobName=job_name, body=body)
            .execute(num_retries=self.num_retries)
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_transfer_job(self, job_name: str, project_id: str) -> None:
        """
        Delete a transfer job.

        This is a soft delete. After a transfer job is deleted, the job and all
        the transfer executions are subject to garbage collection. Transfer jobs
        become eligible for garbage collection 30 days after soft delete.

        :param job_name: (Required) Name of the job to be deleted
        :param project_id: (Optional) the ID of the project that owns the Transfer
            Job. If set to None or missing, the default project_id from the Google Cloud
            connection is used.
        """
        (
            self.get_conn()
            .transferJobs()
            .patch(
                jobName=job_name,
                body={
                    PROJECT_ID: project_id,
                    TRANSFER_JOB: {STATUS1: GcpTransferJobsStatus.DELETED},
                    TRANSFER_JOB_FIELD_MASK: STATUS1,
                },
            )
            .execute(num_retries=self.num_retries)
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def run_transfer_job(self, job_name: str, project_id: str) -> dict:
        """
        Run Google Storage Transfer Service job.

        :param job_name: (Required) Name of the job to be fetched
        :param project_id: (Optional) the ID of the project that owns the Transfer
            Job. If set to None or missing, the default project_id from the Google Cloud
            connection is used.
        :return: If successful, Operation. See:
            https://cloud.google.com/storage-transfer/docs/reference/rest/v1/Operation

        .. seealso:: https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs/run

        """
        return (
            self.get_conn()
            .transferJobs()
            .run(
                jobName=job_name,
                body={
                    PROJECT_ID: project_id,
                },
            )
            .execute(num_retries=self.num_retries)
        )

    def cancel_transfer_operation(self, operation_name: str) -> None:
        """
        Cancel a transfer operation in Google Storage Transfer Service.

        :param operation_name: Name of the transfer operation.
        """
        self.get_conn().transferOperations().cancel(name=operation_name).execute(num_retries=self.num_retries)

    def get_transfer_operation(self, operation_name: str) -> dict:
        """
        Get a transfer operation in Google Storage Transfer Service.

        :param operation_name: (Required) Name of the transfer operation.
        :return: transfer operation

        .. seealso:: https://cloud.google.com/storage-transfer/docs/reference/rest/v1/Operation
        """
        return (
            self.get_conn()
            .transferOperations()
            .get(name=operation_name)
            .execute(num_retries=self.num_retries)
        )

    def list_transfer_operations(self, request_filter: dict | None = None, **kwargs) -> list[dict]:
        """
        Get a transfer operation in Google Storage Transfer Service.

        :param request_filter: (Required) A request filter, as described in
            https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs/list#body.QUERY_PARAMETERS.filter
            With one additional improvement:
        :return: transfer operation

        The ``project_id`` parameter is optional if you have a project ID
        defined in the connection. See: :doc:`/connections/gcp`
        """
        # To preserve backward compatibility
        # TODO: remove one day
        if request_filter is None:
            if "filter" in kwargs:
                request_filter = kwargs["filter"]
                if not isinstance(request_filter, dict):
                    raise ValueError(f"The request_filter should be dict and is {type(request_filter)}")
                warnings.warn(
                    "Use 'request_filter' instead of 'filter'",
                    AirflowProviderDeprecationWarning,
                    stacklevel=2,
                )
            else:
                raise TypeError(
                    "list_transfer_operations missing 1 required positional argument: 'request_filter'"
                )

        conn = self.get_conn()

        request_filter = self._inject_project_id(request_filter, FILTER, FILTER_PROJECT_ID)

        operations: list[dict] = []

        request = conn.transferOperations().list(name=TRANSFER_OPERATIONS, filter=json.dumps(request_filter))

        while request is not None:
            response = request.execute(num_retries=self.num_retries)
            if OPERATIONS in response:
                operations.extend(response[OPERATIONS])

            request = conn.transferOperations().list_next(
                previous_request=request, previous_response=response
            )

        return operations

    def pause_transfer_operation(self, operation_name: str) -> None:
        """
        Pause a transfer operation in Google Storage Transfer Service.

        :param operation_name: (Required) Name of the transfer operation.
        """
        self.get_conn().transferOperations().pause(name=operation_name).execute(num_retries=self.num_retries)

    def resume_transfer_operation(self, operation_name: str) -> None:
        """
        Resume a transfer operation in Google Storage Transfer Service.

        :param operation_name: (Required) Name of the transfer operation.
        """
        self.get_conn().transferOperations().resume(name=operation_name).execute(num_retries=self.num_retries)

    def wait_for_transfer_job(
        self,
        job: dict,
        expected_statuses: set[str] | None = None,
        timeout: float | timedelta | None = None,
    ) -> None:
        """
        Wait until the job reaches the expected state.

        :param job: The transfer job to wait for. See:
            https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs#TransferJob
        :param expected_statuses: The expected state. See:
            https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferOperations#Status
        :param timeout: Time in which the operation must end in seconds. If not
            specified, defaults to 60 seconds.
        """
        expected_statuses = (
            {GcpTransferOperationStatus.SUCCESS} if not expected_statuses else expected_statuses
        )
        if timeout is None:
            timeout = 60
        elif isinstance(timeout, timedelta):
            timeout = timeout.total_seconds()

        start_time = time.monotonic()
        while time.monotonic() - start_time < timeout:
            request_filter = {FILTER_PROJECT_ID: job[PROJECT_ID], FILTER_JOB_NAMES: [job[NAME]]}
            operations = self.list_transfer_operations(request_filter=request_filter)

            for operation in operations:
                self.log.info("Progress for operation %s: %s", operation[NAME], operation[METADATA][COUNTERS])

            if self.operations_contain_expected_statuses(operations, expected_statuses):
                return
            time.sleep(TIME_TO_SLEEP_IN_SECONDS)
        raise AirflowException("Timeout. The operation could not be completed within the allotted time.")

    def _inject_project_id(self, body: dict, param_name: str, target_key: str) -> dict:
        body = deepcopy(body)
        body[target_key] = body.get(target_key, self.project_id)
        if not body.get(target_key):
            raise AirflowException(
                f"The project id must be passed either as `{target_key}` key in `{param_name}` "
                f"parameter or as project_id extra in Google Cloud connection definition. Both are not set!"
            )
        return body

    @staticmethod
    def operations_contain_expected_statuses(
        operations: list[dict], expected_statuses: set[str] | str
    ) -> bool:
        """
        Check whether an operation exists with the expected status.

        :param operations: (Required) List of transfer operations to check.
        :param expected_statuses: (Required) The expected status. See:
            https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferOperations#Status
        :return: If there is an operation with the expected state in the
            operation list, returns true,
        :raises AirflowException: If it encounters operations with state FAILED
            or ABORTED in the list.
        """
        expected_statuses_set = (
            {expected_statuses} if isinstance(expected_statuses, str) else set(expected_statuses)
        )
        if not operations:
            return False

        current_statuses = {operation[METADATA][STATUS] for operation in operations}

        if len(current_statuses - expected_statuses_set) != len(current_statuses):
            return True

        if len(NEGATIVE_STATUSES - current_statuses) != len(NEGATIVE_STATUSES):
            raise AirflowException(
                f"An unexpected operation status was encountered. "
                f"Expected: {', '.join(expected_statuses_set)}"
            )
        return False


class CloudDataTransferServiceAsyncHook(GoogleBaseAsyncHook):
    """Asynchronous hook for Google Storage Transfer Service."""

    sync_hook_class = CloudDataTransferServiceHook

    def __init__(self, project_id: str = PROVIDE_PROJECT_ID, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self._client: StorageTransferServiceAsyncClient | None = None

    async def get_conn(self) -> StorageTransferServiceAsyncClient:
        """
        Return async connection to the Storage Transfer Service.

        :return: Google Storage Transfer asynchronous client.
        """
        if not self._client:
            credentials = (await self.get_sync_hook()).get_credentials()
            self._client = StorageTransferServiceAsyncClient(
                credentials=credentials,
                client_info=CLIENT_INFO,
            )
        return self._client

    async def get_jobs(self, job_names: list[str]) -> ListTransferJobsAsyncPager:
        """
        Get the latest state of a long-running operations in Google Storage Transfer Service.

        :param job_names: (Required) List of names of the jobs to be fetched.
        :return: Object that yields Transfer jobs.
        """
        client = await self.get_conn()
        jobs_list_request = ListTransferJobsRequest(
            filter=json.dumps({"project_id": self.project_id, "job_names": job_names})
        )
        return await client.list_transfer_jobs(request=jobs_list_request)

    async def get_latest_operation(self, job: TransferJob) -> Message | None:
        """
        Get the latest operation of the given TransferJob instance.

        :param job: Transfer job instance.
        :return: The latest job operation.
        """
        latest_operation_name = job.latest_operation_name
        if latest_operation_name:
            client = await self.get_conn()
            response_operation = await client.transport.operations_client.get_operation(latest_operation_name)
            operation = TransferOperation.deserialize(response_operation.metadata.value)
            return operation
        return None

    async def list_transfer_operations(
        self,
        request_filter: dict | None = None,
        **kwargs,
    ) -> list[dict[str, Any]]:
        """
        Get a transfer operation in Google Storage Transfer Service.

        :param request_filter: (Required) A request filter, as described in
            https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs/list#body.QUERY_PARAMETERS.filter
            With one additional improvement:
        :return: transfer operation

        The ``project_id`` parameter is optional if you have a project ID
        defined in the connection. See: :doc:`/connections/gcp`
        """
        # To preserve backward compatibility
        # TODO: remove one day
        if request_filter is None:
            if "filter" in kwargs:
                request_filter = kwargs["filter"]
                if not isinstance(request_filter, dict):
                    raise ValueError(f"The request_filter should be dict and is {type(request_filter)}")
                warnings.warn(
                    "Use 'request_filter' instead of 'filter'",
                    AirflowProviderDeprecationWarning,
                    stacklevel=2,
                )
            else:
                raise TypeError(
                    "list_transfer_operations missing 1 required positional argument: 'request_filter'"
                )

        conn = await self.get_conn()

        request_filter = await self._inject_project_id(request_filter, FILTER, FILTER_PROJECT_ID)

        operations: list[operations_pb2.Operation] = []

        response = await conn.list_operations(
            request={
                "name": TRANSFER_OPERATIONS,
                "filter": json.dumps(request_filter),
            }
        )

        while response is not None:
            operations.extend(response.operations)
            response = (
                await conn.list_operations(
                    request={
                        "name": TRANSFER_OPERATIONS,
                        "filter": json.dumps(request_filter),
                        "page_token": response.next_page_token,
                    }
                )
                if response.next_page_token
                else None
            )

        transfer_operations = [
            MessageToDict(
                getattr(op, "_pb", op),
                preserving_proto_field_name=True,
                use_integers_for_enums=True,
            )
            for op in operations
        ]

        return transfer_operations

    async def _inject_project_id(self, body: dict, param_name: str, target_key: str) -> dict:
        body = deepcopy(body)
        body[target_key] = body.get(target_key, self.project_id)
        if not body.get(target_key):
            raise AirflowException(
                f"The project id must be passed either as `{target_key}` key in `{param_name}` "
                f"parameter or as project_id extra in Google Cloud connection definition. Both are not set!"
            )
        return body

    @staticmethod
    async def operations_contain_expected_statuses(
        operations: list[dict[str, Any]], expected_statuses: set[str] | str
    ) -> bool:
        """
        Check whether an operation exists with the expected status.

        :param operations: (Required) List of transfer operations to check.
        :param expected_statuses: (Required) The expected status. See:
            https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferOperations#Status
        :return: If there is an operation with the expected state in the
            operation list, returns true,
        :raises AirflowException: If it encounters operations with state FAILED
            or ABORTED in the list.
        """
        expected_statuses_set = (
            {expected_statuses} if isinstance(expected_statuses, str) else set(expected_statuses)
        )
        if not operations:
            return False

        current_statuses = {TransferOperation.Status(op["metadata"]["status"]).name for op in operations}

        if len(current_statuses - expected_statuses_set) != len(current_statuses):
            return True

        if len(NEGATIVE_STATUSES - current_statuses) != len(NEGATIVE_STATUSES):
            raise AirflowException(
                f"An unexpected operation status was encountered. "
                f"Expected: {', '.join(expected_statuses_set)}"
            )
        return False

    async def run_transfer_job(self, job_name: str) -> operation_async.AsyncOperation:
        """
        Run Google Storage Transfer Service job.

        :param job_name: (Required) Name of the job to run.
        """
        client = await self.get_conn()
        request = RunTransferJobRequest(
            job_name=job_name,
            project_id=self.project_id,
        )
        operation = await client.run_transfer_job(request=request)
        return operation
