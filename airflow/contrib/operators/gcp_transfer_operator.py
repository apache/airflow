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
#
"""
This module contains Google Cloud Transfer operators.
"""

from copy import deepcopy
from datetime import date, time

from airflow import AirflowException
from airflow.contrib.hooks.gcp_transfer_hook import (
    GCPTransferServiceHook,
    GcpTransferJobsStatus,
    TRANSFER_OPTIONS,
    OBJECT_CONDITIONS,
    PROJECT_ID,
    BUCKET_NAME,
    GCS_DATA_SINK,
    STATUS,
    DESCRIPTION,
    GCS_DATA_SOURCE,
    HTTP_DATA_SOURCE,
    SECONDS,
    MINUTES,
    HOURS,
    YEAR,
    MONTH,
    DAY,
    START_TIME_OF_DAY,
    SCHEDULE_END_DATE,
    SCHEDULE_START_DATE,
    SCHEDULE,
    SECRET_ACCESS_KEY,
    ACCESS_KEY_ID,
    AWS_ACCESS_KEY,
    AWS_S3_DATA_SOURCE,
    TRANSFER_SPEC,
)
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

try:
    from airflow.contrib.hooks.aws_hook import AwsHook
except ImportError:  # pragma: no cover
    AwsHook = None  # type: ignore


class TransferJobPreprocessor:
    """
    Helper class for preprocess of transfer job body.
    """

    def __init__(self, body, aws_conn_id='aws_default', default_schedule=False):
        self.body = body
        self.aws_conn_id = aws_conn_id
        self.default_schedule = default_schedule

    def _inject_aws_credentials(self):
        if TRANSFER_SPEC in self.body and AWS_S3_DATA_SOURCE in self.body[TRANSFER_SPEC]:
            aws_hook = AwsHook(self.aws_conn_id)
            aws_credentials = aws_hook.get_credentials()
            aws_access_key_id = aws_credentials.access_key
            aws_secret_access_key = aws_credentials.secret_key
            self.body[TRANSFER_SPEC][AWS_S3_DATA_SOURCE][AWS_ACCESS_KEY] = {
                ACCESS_KEY_ID: aws_access_key_id,
                SECRET_ACCESS_KEY: aws_secret_access_key,
            }

    def _reformat_date(self, field_key):
        schedule = self.body[SCHEDULE]
        if field_key not in schedule:
            return
        if isinstance(schedule[field_key], date):
            schedule[field_key] = self._convert_date_to_dict(schedule[field_key])

    def _reformat_time(self, field_key):
        schedule = self.body[SCHEDULE]
        if field_key not in schedule:
            return
        if isinstance(schedule[field_key], time):
            schedule[field_key] = self._convert_time_to_dict(schedule[field_key])

    def _reformat_schedule(self):
        if SCHEDULE not in self.body:
            if self.default_schedule:
                self.body[SCHEDULE] = {
                    SCHEDULE_START_DATE: date.today(),
                    SCHEDULE_END_DATE: date.today()
                }
            else:
                return
        self._reformat_date(SCHEDULE_START_DATE)
        self._reformat_date(SCHEDULE_END_DATE)
        self._reformat_time(START_TIME_OF_DAY)

    def process_body(self):
        """
        Injects AWS credentials into body if needed and
        reformats schedule information.

        :return: Preprocessed body
        :rtype: dict
        """

        self._inject_aws_credentials()
        self._reformat_schedule()
        return self.body

    @staticmethod
    def _convert_date_to_dict(field_date):
        """
        Convert native python ``datetime.date`` object  to a format supported by the API
        """
        return {DAY: field_date.day, MONTH: field_date.month, YEAR: field_date.year}

    @staticmethod
    def _convert_time_to_dict(time_object):
        """
        Convert native python ``datetime.time`` object  to a format supported by the API
        """
        return {HOURS: time_object.hour, MINUTES: time_object.minute, SECONDS: time_object.second}


class TransferJobValidator:
    """
    Helper class for validating transfer job body.
    """
    def __init__(self, body):
        if not body:
            raise AirflowException("The required parameter 'body' is empty or None")

        self.body = body

    def _verify_data_source(self):
        is_gcs = GCS_DATA_SOURCE in self.body[TRANSFER_SPEC]
        is_aws_s3 = AWS_S3_DATA_SOURCE in self.body[TRANSFER_SPEC]
        is_http = HTTP_DATA_SOURCE in self.body[TRANSFER_SPEC]

        sources_count = sum([is_gcs, is_aws_s3, is_http])
        if sources_count > 1:
            raise AirflowException(
                "More than one data source detected. Please choose exactly one data source from: "
                "gcsDataSource, awsS3DataSource and httpDataSource."
            )

    def _restrict_aws_credentials(self):
        aws_transfer = AWS_S3_DATA_SOURCE in self.body[TRANSFER_SPEC]
        if aws_transfer and AWS_ACCESS_KEY in self.body[TRANSFER_SPEC][AWS_S3_DATA_SOURCE]:
            raise AirflowException(
                "AWS credentials detected inside the body parameter (awsAccessKey). This is not allowed, "
                "please use Airflow connections to store credentials."
            )

    def validate_body(self):
        """
        Validates the body. Checks if body specifies `transferSpec`
        if yes, then check if AWS credentials are passed correctly and
        no more than 1 data source was selected.

        :raises: AirflowException
        """
        if TRANSFER_SPEC in self.body:
            self._restrict_aws_credentials()
            self._verify_data_source()


class GcpTransferServiceJobCreateOperator(BaseOperator):
    """
    Creates a transfer job that runs periodically.

    .. warning::

        This operator is NOT idempotent. If you run it many times, many transfer
        jobs will be created in the Google Cloud Platform.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GcpTransferServiceJobCreateOperator`

    :param body: (Required) The request body, as described in
        https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs/create#request-body
        With three additional improvements:

        * dates can be given in the form :class:`datetime.date`
        * times can be given in the form :class:`datetime.time`
        * credentials to Amazon Web Service should be stored in the connection and indicated by the
          aws_conn_id parameter

    :type body: dict
    :param aws_conn_id: The connection ID used to retrieve credentials to
        Amazon Web Service.
    :type aws_conn_id: str
    :param gcp_conn_id: The connection ID used to connect to Google Cloud
        Platform.
    :type gcp_conn_id: str
    :param api_version: API version used (e.g. v1).
    :type api_version: str
    """
    # [START gcp_transfer_job_create_template_fields]
    template_fields = ('body', 'gcp_conn_id', 'aws_conn_id')
    # [END gcp_transfer_job_create_template_fields]

    @apply_defaults
    def __init__(
        self,
        body,
        aws_conn_id='aws_default',
        gcp_conn_id='google_cloud_default',
        api_version='v1',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.body = deepcopy(body)
        self.aws_conn_id = aws_conn_id
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self._validate_inputs()

    def _validate_inputs(self):
        TransferJobValidator(body=self.body).validate_body()

    def execute(self, context):
        TransferJobPreprocessor(body=self.body, aws_conn_id=self.aws_conn_id).process_body()
        hook = GCPTransferServiceHook(api_version=self.api_version, gcp_conn_id=self.gcp_conn_id)
        return hook.create_transfer_job(body=self.body)


class GcpTransferServiceJobUpdateOperator(BaseOperator):
    """
    Updates a transfer job that runs periodically.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GcpTransferServiceJobUpdateOperator`

    :param job_name: (Required) Name of the job to be updated
    :type job_name: str
    :param body: (Required) The request body, as described in
        https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs/patch#request-body
        With three additional improvements:

        * dates can be given in the form :class:`datetime.date`
        * times can be given in the form :class:`datetime.time`
        * credentials to Amazon Web Service should be stored in the connection and indicated by the
          aws_conn_id parameter

    :type body: dict
    :param aws_conn_id: The connection ID used to retrieve credentials to
        Amazon Web Service.
    :type aws_conn_id: str
    :param gcp_conn_id: The connection ID used to connect to Google Cloud
        Platform.
    :type gcp_conn_id: str
    :param api_version: API version used (e.g. v1).
    :type api_version: str
    """
    # [START gcp_transfer_job_update_template_fields]
    template_fields = ('job_name', 'body', 'gcp_conn_id', 'aws_conn_id')
    # [END gcp_transfer_job_update_template_fields]

    @apply_defaults
    def __init__(
        self,
        job_name,
        body,
        aws_conn_id='aws_default',
        gcp_conn_id='google_cloud_default',
        api_version='v1',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.job_name = job_name
        self.body = body
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self.aws_conn_id = aws_conn_id
        self._validate_inputs()

    def _validate_inputs(self):
        TransferJobValidator(body=self.body).validate_body()
        if not self.job_name:
            raise AirflowException("The required parameter 'job_name' is empty or None")

    def execute(self, context):
        TransferJobPreprocessor(body=self.body, aws_conn_id=self.aws_conn_id).process_body()
        hook = GCPTransferServiceHook(api_version=self.api_version, gcp_conn_id=self.gcp_conn_id)
        return hook.update_transfer_job(job_name=self.job_name, body=self.body)


class GcpTransferServiceJobDeleteOperator(BaseOperator):
    """
    Delete a transfer job. This is a soft delete. After a transfer job is
    deleted, the job and all the transfer executions are subject to garbage
    collection. Transfer jobs become eligible for garbage collection
    30 days after soft delete.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GcpTransferServiceJobDeleteOperator`

    :param job_name: (Required) Name of the TRANSFER operation
    :type job_name: str
    :param project_id: (Optional) the ID of the project that owns the Transfer
        Job. If set to None or missing, the default project_id from the GCP
        connection is used.
    :type project_id: str
    :param gcp_conn_id: The connection ID used to connect to Google Cloud
        Platform.
    :type gcp_conn_id: str
    :param api_version: API version used (e.g. v1).
    :type api_version: str
    """
    # [START gcp_transfer_job_delete_template_fields]
    template_fields = ('job_name', 'project_id', 'gcp_conn_id', 'api_version')
    # [END gcp_transfer_job_delete_template_fields]

    @apply_defaults
    def __init__(
        self, job_name, gcp_conn_id='google_cloud_default', api_version='v1', project_id=None, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.job_name = job_name
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self._validate_inputs()

    def _validate_inputs(self):
        if not self.job_name:
            raise AirflowException("The required parameter 'job_name' is empty or None")

    def execute(self, context):
        self._validate_inputs()
        hook = GCPTransferServiceHook(api_version=self.api_version, gcp_conn_id=self.gcp_conn_id)
        hook.delete_transfer_job(job_name=self.job_name, project_id=self.project_id)


class GcpTransferServiceOperationGetOperator(BaseOperator):
    """
    Gets the latest state of a long-running operation in Google Storage Transfer
    Service.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GcpTransferServiceOperationGetOperator`

    :param operation_name: (Required) Name of the transfer operation.
    :type operation_name: str
    :param gcp_conn_id: The connection ID used to connect to Google
        Cloud Platform.
    :type gcp_conn_id: str
    :param api_version: API version used (e.g. v1).
    :type api_version: str
    """
    # [START gcp_transfer_operation_get_template_fields]
    template_fields = ('operation_name', 'gcp_conn_id')
    # [END gcp_transfer_operation_get_template_fields]

    @apply_defaults
    def __init__(self, operation_name, gcp_conn_id='google_cloud_default', api_version='v1', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.operation_name = operation_name
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self._validate_inputs()

    def _validate_inputs(self):
        if not self.operation_name:
            raise AirflowException("The required parameter 'operation_name' is empty or None")

    def execute(self, context):
        hook = GCPTransferServiceHook(api_version=self.api_version, gcp_conn_id=self.gcp_conn_id)
        operation = hook.get_transfer_operation(operation_name=self.operation_name)
        return operation


class GcpTransferServiceOperationsListOperator(BaseOperator):
    """
    Lists long-running operations in Google Storage Transfer
    Service that match the specified filter.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GcpTransferServiceOperationsListOperator`

    :param filter: (Required) A request filter, as described in
            https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs/list#body.QUERY_PARAMETERS.filter
    :type filter: dict
    :param gcp_conn_id: The connection ID used to connect to Google
        Cloud Platform.
    :type gcp_conn_id: str
    :param api_version: API version used (e.g. v1).
    :type api_version: str
    """
    # [START gcp_transfer_operations_list_template_fields]
    template_fields = ('filter', 'gcp_conn_id')
    # [END gcp_transfer_operations_list_template_fields]

    def __init__(self,
                 request_filter=None,
                 gcp_conn_id='google_cloud_default',
                 api_version='v1',
                 *args,
                 **kwargs):
        # To preserve backward compatibility
        # TODO: remove one day
        if request_filter is None:
            if 'filter' in kwargs:
                request_filter = kwargs['filter']
                DeprecationWarning("Use 'request_filter' instead 'filter' to pass the argument.")
            else:
                TypeError("__init__() missing 1 required positional argument: 'request_filter'")

        super().__init__(*args, **kwargs)
        self.filter = request_filter
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self._validate_inputs()

    def _validate_inputs(self):
        if not self.filter:
            raise AirflowException("The required parameter 'filter' is empty or None")

    def execute(self, context):
        hook = GCPTransferServiceHook(api_version=self.api_version, gcp_conn_id=self.gcp_conn_id)
        operations_list = hook.list_transfer_operations(request_filter=self.filter)
        self.log.info(operations_list)
        return operations_list


class GcpTransferServiceOperationPauseOperator(BaseOperator):
    """
    Pauses a transfer operation in Google Storage Transfer Service.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GcpTransferServiceOperationPauseOperator`

    :param operation_name: (Required) Name of the transfer operation.
    :type operation_name: str
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param api_version:  API version used (e.g. v1).
    :type api_version: str
    """
    # [START gcp_transfer_operation_pause_template_fields]
    template_fields = ('operation_name', 'gcp_conn_id', 'api_version')
    # [END gcp_transfer_operation_pause_template_fields]

    @apply_defaults
    def __init__(self, operation_name, gcp_conn_id='google_cloud_default', api_version='v1', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.operation_name = operation_name
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self._validate_inputs()

    def _validate_inputs(self):
        if not self.operation_name:
            raise AirflowException("The required parameter 'operation_name' is empty or None")

    def execute(self, context):
        hook = GCPTransferServiceHook(api_version=self.api_version, gcp_conn_id=self.gcp_conn_id)
        hook.pause_transfer_operation(operation_name=self.operation_name)


class GcpTransferServiceOperationResumeOperator(BaseOperator):
    """
    Resumes a transfer operation in Google Storage Transfer Service.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GcpTransferServiceOperationResumeOperator`

    :param operation_name: (Required) Name of the transfer operation.
    :type operation_name: str
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :param api_version: API version used (e.g. v1).
    :type api_version: str
    :type gcp_conn_id: str
    """
    # [START gcp_transfer_operation_resume_template_fields]
    template_fields = ('operation_name', 'gcp_conn_id', 'api_version')
    # [END gcp_transfer_operation_resume_template_fields]

    @apply_defaults
    def __init__(self, operation_name, gcp_conn_id='google_cloud_default', api_version='v1', *args, **kwargs):
        self.operation_name = operation_name
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self._validate_inputs()
        super().__init__(*args, **kwargs)

    def _validate_inputs(self):
        if not self.operation_name:
            raise AirflowException("The required parameter 'operation_name' is empty or None")

    def execute(self, context):
        hook = GCPTransferServiceHook(api_version=self.api_version, gcp_conn_id=self.gcp_conn_id)
        hook.resume_transfer_operation(operation_name=self.operation_name)


class GcpTransferServiceOperationCancelOperator(BaseOperator):
    """
    Cancels a transfer operation in Google Storage Transfer Service.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GcpTransferServiceOperationCancelOperator`

    :param operation_name: (Required) Name of the transfer operation.
    :type operation_name: str
    :param api_version: API version used (e.g. v1).
    :type api_version: str
    :param gcp_conn_id: The connection ID used to connect to Google
        Cloud Platform.
    :type gcp_conn_id: str
    """
    # [START gcp_transfer_operation_cancel_template_fields]
    template_fields = ('operation_name', 'gcp_conn_id', 'api_version')
    # [END gcp_transfer_operation_cancel_template_fields]

    @apply_defaults
    def __init__(self, operation_name, api_version='v1', gcp_conn_id='google_cloud_default', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.operation_name = operation_name
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self._validate_inputs()

    def _validate_inputs(self):
        if not self.operation_name:
            raise AirflowException("The required parameter 'operation_name' is empty or None")

    def execute(self, context):
        hook = GCPTransferServiceHook(api_version=self.api_version, gcp_conn_id=self.gcp_conn_id)
        hook.cancel_transfer_operation(operation_name=self.operation_name)


class S3ToGoogleCloudStorageTransferOperator(BaseOperator):
    """
    Synchronizes an S3 bucket with a Google Cloud Storage bucket using the
    GCP Storage Transfer Service.

    .. warning::

        This operator is NOT idempotent. If you run it many times, many transfer
        jobs will be created in the Google Cloud Platform.

    **Example**:

    .. code-block:: python

       s3_to_gcs_transfer_op = S3ToGoogleCloudStorageTransferOperator(
            task_id='s3_to_gcs_transfer_example',
            s3_bucket='my-s3-bucket',
            project_id='my-gcp-project',
            gcs_bucket='my-gcs-bucket',
            dag=my_dag)

    :param s3_bucket: The S3 bucket where to find the objects. (templated)
    :type s3_bucket: str
    :param gcs_bucket: The destination Google Cloud Storage bucket
        where you want to store the files. (templated)
    :type gcs_bucket: str
    :param project_id: Optional ID of the Google Cloud Platform Console project that
        owns the job
    :type project_id: str
    :param aws_conn_id: The source S3 connection
    :type aws_conn_id: str
    :param gcp_conn_id: The destination connection ID to use
        when connecting to Google Cloud Storage.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param description: Optional transfer service job description
    :type description: str
    :param schedule: Optional transfer service schedule;
        If not set, run transfer job once as soon as the operator runs
        The format is described
        https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs.
        With two additional improvements:

        * dates they can be passed as :class:`datetime.date`
        * times they can be passed as :class:`datetime.time`

    :type schedule: dict
    :param object_conditions: Optional transfer service object conditions; see
        https://cloud.google.com/storage-transfer/docs/reference/rest/v1/TransferSpec
    :type object_conditions: dict
    :param transfer_options: Optional transfer service transfer options; see
        https://cloud.google.com/storage-transfer/docs/reference/rest/v1/TransferSpec
    :type transfer_options: dict
    :param wait: Wait for transfer to finish
    :type wait: bool
    :param timeout: Time to wait for the operation to end in seconds
    :type timeout: int
    """

    template_fields = ('gcp_conn_id', 's3_bucket', 'gcs_bucket', 'description', 'object_conditions')
    ui_color = '#e09411'

    @apply_defaults
    def __init__(  # pylint: disable=too-many-arguments
        self,
        s3_bucket,
        gcs_bucket,
        project_id=None,
        aws_conn_id='aws_default',
        gcp_conn_id='google_cloud_default',
        delegate_to=None,
        description=None,
        schedule=None,
        object_conditions=None,
        transfer_options=None,
        wait=True,
        timeout=None,
        *args,
        **kwargs
    ):

        super().__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.gcs_bucket = gcs_bucket
        self.project_id = project_id
        self.aws_conn_id = aws_conn_id
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.description = description
        self.schedule = schedule
        self.object_conditions = object_conditions
        self.transfer_options = transfer_options
        self.wait = wait
        self.timeout = timeout

    def execute(self, context):
        hook = GCPTransferServiceHook(gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to)
        body = self._create_body()

        TransferJobPreprocessor(body=body, aws_conn_id=self.aws_conn_id, default_schedule=True).process_body()

        job = hook.create_transfer_job(body=body)

        if self.wait:
            hook.wait_for_transfer_job(job, timeout=self.timeout)

    def _create_body(self):
        body = {
            DESCRIPTION: self.description,
            STATUS: GcpTransferJobsStatus.ENABLED,
            TRANSFER_SPEC: {
                AWS_S3_DATA_SOURCE: {BUCKET_NAME: self.s3_bucket},
                GCS_DATA_SINK: {BUCKET_NAME: self.gcs_bucket},
            },
        }

        if self.project_id is not None:
            body[PROJECT_ID] = self.project_id

        if self.schedule is not None:
            body[SCHEDULE] = self.schedule

        if self.object_conditions is not None:
            body[TRANSFER_SPEC][OBJECT_CONDITIONS] = self.object_conditions

        if self.transfer_options is not None:
            body[TRANSFER_SPEC][TRANSFER_OPTIONS] = self.transfer_options

        return body


class GoogleCloudStorageToGoogleCloudStorageTransferOperator(BaseOperator):
    """
    Copies objects from a bucket to another using the GCP Storage Transfer
    Service.

    .. warning::

        This operator is NOT idempotent. If you run it many times, many transfer
        jobs will be created in the Google Cloud Platform.

    **Example**:

    .. code-block:: python

       gcs_to_gcs_transfer_op = GoogleCloudStorageToGoogleCloudStorageTransferOperator(
            task_id='gcs_to_gcs_transfer_example',
            source_bucket='my-source-bucket',
            destination_bucket='my-destination-bucket',
            project_id='my-gcp-project',
            dag=my_dag)

    :param source_bucket: The source Google cloud storage bucket where the
         object is. (templated)
    :type source_bucket: str
    :param destination_bucket: The destination Google cloud storage bucket
        where the object should be. (templated)
    :type destination_bucket: str
    :param project_id: The ID of the Google Cloud Platform Console project that
        owns the job
    :type project_id: str
    :param gcp_conn_id: Optional connection ID to use when connecting to Google Cloud
        Storage.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param description: Optional transfer service job description
    :type description: str
    :param schedule: Optional transfer service schedule;
        If not set, run transfer job once as soon as the operator runs
        See:
        https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferJobs.
        With two additional improvements:

        * dates they can be passed as :class:`datetime.date`
        * times they can be passed as :class:`datetime.time`

    :type schedule: dict
    :param object_conditions: Optional transfer service object conditions; see
        https://cloud.google.com/storage-transfer/docs/reference/rest/v1/TransferSpec#ObjectConditions
    :type object_conditions: dict
    :param transfer_options: Optional transfer service transfer options; see
        https://cloud.google.com/storage-transfer/docs/reference/rest/v1/TransferSpec#TransferOptions
    :type transfer_options: dict
    :param wait: Wait for transfer to finish; defaults to `True`
    :type wait: bool
    :param timeout: Time to wait for the operation to end in seconds
    :type timeout: int
    """

    template_fields = (
        'gcp_conn_id',
        'source_bucket',
        'destination_bucket',
        'description',
        'object_conditions',
    )
    ui_color = '#e09411'

    @apply_defaults
    def __init__(  # pylint: disable=too-many-arguments
        self,
        source_bucket,
        destination_bucket,
        project_id=None,
        gcp_conn_id='google_cloud_default',
        delegate_to=None,
        description=None,
        schedule=None,
        object_conditions=None,
        transfer_options=None,
        wait=True,
        timeout=None,
        *args,
        **kwargs
    ):

        super().__init__(*args, **kwargs)
        self.source_bucket = source_bucket
        self.destination_bucket = destination_bucket
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.description = description
        self.schedule = schedule
        self.object_conditions = object_conditions
        self.transfer_options = transfer_options
        self.wait = wait
        self.timeout = timeout

    def execute(self, context):
        hook = GCPTransferServiceHook(gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to)

        body = self._create_body()

        TransferJobPreprocessor(body=body, default_schedule=True).process_body()

        job = hook.create_transfer_job(body=body)

        if self.wait:
            hook.wait_for_transfer_job(job, timeout=self.timeout)

    def _create_body(self):
        body = {
            DESCRIPTION: self.description,
            STATUS: GcpTransferJobsStatus.ENABLED,
            TRANSFER_SPEC: {
                GCS_DATA_SOURCE: {BUCKET_NAME: self.source_bucket},
                GCS_DATA_SINK: {BUCKET_NAME: self.destination_bucket},
            },
        }

        if self.project_id is not None:
            body[PROJECT_ID] = self.project_id

        if self.schedule is not None:
            body[SCHEDULE] = self.schedule

        if self.object_conditions is not None:
            body[TRANSFER_SPEC][OBJECT_CONDITIONS] = self.object_conditions

        if self.transfer_options is not None:
            body[TRANSFER_SPEC][TRANSFER_OPTIONS] = self.transfer_options

        return body
