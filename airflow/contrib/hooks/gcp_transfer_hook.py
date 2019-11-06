# -*- coding: utf-8 -*-
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
"""

import json
import time
from copy import deepcopy

import six
from googleapiclient.discovery import build

from airflow.exceptions import AirflowException
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook

# Time to sleep between active checks of the operation results
TIME_TO_SLEEP_IN_SECONDS = 10


class GcpTransferJobsStatus:
    """
    Class with GCP Transfer jobs statuses.
    """
    ENABLED = "ENABLED"
    DISABLED = "DISABLED"
    DELETED = "DELETED"


class GcpTransferOperationStatus:
    """
    Class with GCP Transfer operations statuses.
    """
    IN_PROGRESS = "IN_PROGRESS"
    PAUSED = "PAUSED"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    ABORTED = "ABORTED"


# A list of keywords used to build a request or response
ACCESS_KEY_ID = "accessKeyId"
ALREADY_EXISTING_IN_SINK = "overwriteObjectsAlreadyExistingInSink"
AWS_ACCESS_KEY = "awsAccessKey"
AWS_S3_DATA_SOURCE = 'awsS3DataSource'
BODY = 'body'
BUCKET_NAME = 'bucketName'
DAY = 'day'
DESCRIPTION = "description"
FILTER = 'filter'
FILTER_JOB_NAMES = 'job_names'
FILTER_PROJECT_ID = 'project_id'
GCS_DATA_SINK = 'gcsDataSink'
GCS_DATA_SOURCE = 'gcsDataSource'
HOURS = "hours"
HTTP_DATA_SOURCE = 'httpDataSource'
LIST_URL = 'list_url'
METADATA = 'metadata'
MINUTES = "minutes"
MONTH = 'month'
NAME = 'name'
OBJECT_CONDITIONS = 'object_conditions'
OPERATIONS = 'operations'
PROJECT_ID = 'projectId'
SCHEDULE = 'schedule'
SCHEDULE_END_DATE = 'scheduleEndDate'
SCHEDULE_START_DATE = 'scheduleStartDate'
SECONDS = "seconds"
SECRET_ACCESS_KEY = "secretAccessKey"
START_TIME_OF_DAY = 'startTimeOfDay'
STATUS = "status"
STATUS1 = 'status'
TRANSFER_JOB = 'transfer_job'
TRANSFER_JOB_FIELD_MASK = 'update_transfer_job_field_mask'
TRANSFER_JOBS = 'transferJobs'
TRANSFER_OPERATIONS = 'transferOperations'
TRANSFER_OPTIONS = 'transfer_options'
TRANSFER_SPEC = 'transferSpec'
YEAR = 'year'

NEGATIVE_STATUSES = {GcpTransferOperationStatus.FAILED, GcpTransferOperationStatus.ABORTED}


# noinspection PyAbstractClass
class GCPTransferServiceHook(GoogleCloudBaseHook):
    """
    Hook for Google Storage Transfer Service.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.
    """
    def __init__(self, api_version='v1', gcp_conn_id='google_cloud_default', delegate_to=None):
        super().__init__(gcp_conn_id, delegate_to)
        self.api_version = api_version
        self.num_retries = self._get_field('num_retries', 5)
        self._conn = None

    def get_conn(self):
        """
        Retrieves connection to Google Storage Transfer service.

        :return: Google Storage Transfer service object
        :rtype: dict
        """
        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build(
                'storagetransfer', self.api_version, http=http_authorized, cache_discovery=False
            )
        return self._conn

    @GoogleCloudBaseHook.catch_http_exception
    def create_transfer_job(self, body):
        """
        Creates a transfer job that runs periodically.

        :param body: (Required) A request body, as described in
            https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs/patch#request-body
        :type body: dict
        :return: transfer job.
            See:
            https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs#TransferJob
        :rtype: dict
        """
        body = self._inject_project_id(body, BODY, PROJECT_ID)
        return self.get_conn().transferJobs().create(body=body).execute(  # pylint: disable=no-member
            num_retries=self.num_retries)

    @GoogleCloudBaseHook.fallback_to_default_project_id
    @GoogleCloudBaseHook.catch_http_exception
    def get_transfer_job(self, job_name, project_id=None):
        """
        Gets the latest state of a long-running operation in Google Storage
        Transfer Service.

        :param job_name: (Required) Name of the job to be fetched
        :type job_name: str
        :param project_id: (Optional) the ID of the project that owns the Transfer
            Job. If set to None or missing, the default project_id from the GCP
            connection is used.
        :type project_id: str
        :return: Transfer Job
        :rtype: dict
        """
        return (
            self.get_conn()  # pylint: disable=no-member
            .transferJobs()
            .get(jobName=job_name, projectId=project_id)
            .execute(num_retries=self.num_retries)
        )

    def list_transfer_job(self, request_filter=None, **kwargs):
        """
        Lists long-running operations in Google Storage Transfer
        Service that match the specified filter.

        :param request_filter: (Required) A request filter, as described in
            https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs/list#body.QUERY_PARAMETERS.filter
        :type request_filter: dict
        :return: List of Transfer Jobs
        :rtype: list[dict]
        """
        # To preserve backward compatibility
        # TODO: remove one day
        if request_filter is None:
            if 'filter' in kwargs:
                request_filter = kwargs['filter']
                DeprecationWarning("Use 'request_filter' instead of 'filter'")
            else:
                TypeError("list_transfer_job missing 1 required positional argument: 'request_filter'")

        conn = self.get_conn()
        request_filter = self._inject_project_id(request_filter, FILTER, FILTER_PROJECT_ID)
        request = conn.transferJobs().list(filter=json.dumps(request_filter))  # pylint: disable=no-member
        jobs = []

        while request is not None:
            response = request.execute(num_retries=self.num_retries)
            jobs.extend(response[TRANSFER_JOBS])

            request = conn.transferJobs().list_next(previous_request=request,  # pylint: disable=no-member
                                                    previous_response=response)

        return jobs

    @GoogleCloudBaseHook.catch_http_exception
    def update_transfer_job(self, job_name, body):
        """
        Updates a transfer job that runs periodically.

        :param job_name: (Required) Name of the job to be updated
        :type job_name: str
        :param body: A request body, as described in
            https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs/patch#request-body
        :type body: dict
        :return: If successful, TransferJob.
        :rtype: dict
        """
        body = self._inject_project_id(body, BODY, PROJECT_ID)
        return (
            self.get_conn()  # pylint: disable=no-member
            .transferJobs()
            .patch(jobName=job_name, body=body)
            .execute(num_retries=self.num_retries)
        )

    @GoogleCloudBaseHook.fallback_to_default_project_id
    @GoogleCloudBaseHook.catch_http_exception
    def delete_transfer_job(self, job_name, project_id):
        """
        Deletes a transfer job. This is a soft delete. After a transfer job is
        deleted, the job and all the transfer executions are subject to garbage
        collection. Transfer jobs become eligible for garbage collection
        30 days after soft delete.

        :param job_name: (Required) Name of the job to be deleted
        :type job_name: str
        :param project_id: (Optional) the ID of the project that owns the Transfer
            Job. If set to None or missing, the default project_id from the GCP
            connection is used.
        :type project_id: str
        :rtype: None
        """

        return (
            self.get_conn()  # pylint: disable=no-member
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

    @GoogleCloudBaseHook.catch_http_exception
    def cancel_transfer_operation(self, operation_name):
        """
        Cancels an transfer operation in Google Storage Transfer Service.

        :param operation_name: Name of the transfer operation.
        :type operation_name: str
        :rtype: None
        """
        self.get_conn().transferOperations().cancel(  # pylint: disable=no-member
            name=operation_name).execute(num_retries=self.num_retries)

    @GoogleCloudBaseHook.catch_http_exception
    def get_transfer_operation(self, operation_name):
        """
        Gets an transfer operation in Google Storage Transfer Service.

        :param operation_name: (Required) Name of the transfer operation.
        :type operation_name: str
        :return: transfer operation
            See:
            https://cloud.google.com/storage-transfer/docs/reference/rest/v1/Operation
        :rtype: dict
        """
        return (
            self.get_conn()  # pylint: disable=no-member
            .transferOperations()
            .get(name=operation_name)
            .execute(num_retries=self.num_retries)
        )

    @GoogleCloudBaseHook.catch_http_exception
    def list_transfer_operations(self, request_filter=None, **kwargs):
        """
        Gets an transfer operation in Google Storage Transfer Service.

        :param request_filter: (Required) A request filter, as described in
            https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs/list#body.QUERY_PARAMETERS.filter
            With one additional improvement:

            * project_id is optional if you have a project id defined
              in the connection
              See: :ref:`howto/connection:gcp`

        :type request_filter: dict
        :return: transfer operation
        :rtype: list[dict]
        """
        # To preserve backward compatibility
        # TODO: remove one day
        if request_filter is None:
            if 'filter' in kwargs:
                request_filter = kwargs['filter']
                DeprecationWarning("Use 'request_filter' instead of 'filter'")
            else:
                TypeError("list_transfer_operations missing 1 required positional argument: 'request_filter'")

        conn = self.get_conn()

        request_filter = self._inject_project_id(request_filter, FILTER, FILTER_PROJECT_ID)

        operations = []

        request = conn.transferOperations().list(  # pylint: disable=no-member
            name=TRANSFER_OPERATIONS, filter=json.dumps(request_filter))

        while request is not None:
            response = request.execute(num_retries=self.num_retries)
            if OPERATIONS in response:
                operations.extend(response[OPERATIONS])

            request = conn.transferOperations().list_next(  # pylint: disable=no-member
                previous_request=request, previous_response=response
            )

        return operations

    @GoogleCloudBaseHook.catch_http_exception
    def pause_transfer_operation(self, operation_name):
        """
        Pauses an transfer operation in Google Storage Transfer Service.

        :param operation_name: (Required) Name of the transfer operation.
        :type operation_name: str
        :rtype: None
        """
        self.get_conn().transferOperations().pause(  # pylint: disable=no-member
            name=operation_name).execute(num_retries=self.num_retries)

    @GoogleCloudBaseHook.catch_http_exception
    def resume_transfer_operation(self, operation_name):
        """
        Resumes an transfer operation in Google Storage Transfer Service.

        :param operation_name: (Required) Name of the transfer operation.
        :type operation_name: str
        :rtype: None
        """
        self.get_conn().transferOperations().resume(  # pylint: disable=no-member
            name=operation_name).execute(num_retries=self.num_retries)

    @GoogleCloudBaseHook.catch_http_exception
    def wait_for_transfer_job(self, job, expected_statuses=(GcpTransferOperationStatus.SUCCESS,), timeout=60):
        """
        Waits until the job reaches the expected state.

        :param job: Transfer job
            See:
            https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs#TransferJob
        :type job: dict
        :param expected_statuses: State that is expected
            See:
            https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferOperations#Status
        :type expected_statuses: set[str]
        :param timeout:
        :type timeout: time in which the operation must end in seconds
        :rtype: None
        """
        while timeout > 0:
            operations = self.list_transfer_operations(
                request_filter={FILTER_PROJECT_ID: job[PROJECT_ID], FILTER_JOB_NAMES: [job[NAME]]}
            )

            if GCPTransferServiceHook.operations_contain_expected_statuses(operations, expected_statuses):
                return
            time.sleep(TIME_TO_SLEEP_IN_SECONDS)
            timeout -= TIME_TO_SLEEP_IN_SECONDS
        raise AirflowException("Timeout. The operation could not be completed within the allotted time.")

    def _inject_project_id(self, body, param_name, target_key):
        body = deepcopy(body)
        body[target_key] = body.get(target_key, self.project_id)
        if not body.get(target_key):
            raise AirflowException(
                "The project id must be passed either as `{}` key in `{}` parameter or as project_id "
                "extra in GCP connection definition. Both are not set!".format(target_key, param_name)
            )
        return body

    @staticmethod
    def operations_contain_expected_statuses(operations, expected_statuses):
        """
        Checks whether the operation list has an operation with the
        expected status, then returns true
        If it encounters operations in FAILED or ABORTED state
        throw :class:`airflow.exceptions.AirflowException`.

        :param operations: (Required) List of transfer operations to check.
        :type operations: list[dict]
        :param expected_statuses: (Required) status that is expected
            See:
            https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferOperations#Status
        :type expected_statuses: set[str]
        :return: If there is an operation with the expected state
            in the operation list, returns true,
        :raises: airflow.exceptions.AirflowException If it encounters operations
            with a state in the list,
        :rtype: bool
        """
        expected_statuses = (
            {expected_statuses} if isinstance(expected_statuses, six.string_types) else set(expected_statuses)
        )
        if not operations:
            return False

        current_statuses = {operation[METADATA][STATUS] for operation in operations}

        if len(current_statuses - set(expected_statuses)) != len(current_statuses):
            return True

        if len(NEGATIVE_STATUSES - current_statuses) != len(NEGATIVE_STATUSES):
            raise AirflowException(
                'An unexpected operation status was encountered. Expected: {}'.format(
                    ", ".join(expected_statuses)
                )
            )
        return False
