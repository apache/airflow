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
import os
import subprocess
import tempfile

from googleapiclient.errors import HttpError

from airflow import AirflowException
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.contrib.hooks.gcp_transfer_hook import GCPTransferServiceHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class GcpStorageTransferJobCreateOperator(BaseOperator):
    """
    Creates a transfer job that runs periodically.

    :param description: A description for the job. Its max length is 1024 bytes
        when Unicode-encoded.
    :type description: str
    :param data_source
    :type data_source: str
    :param target_bucket_name
    :type target_bucket_name: str
    :param min_time_elapsed_since_last_modification:
    :type min_time_elapsed_since_last_modification:
    :param max_time_elapsed_since_last_modification:
    :type max_time_elapsed_since_last_modification:
    :param include_prefixes:
    :type include_prefixes: str[]
    :param exclude_prefixes:
    :type exclude_prefixes: str[]
    :param source_bucket_name:
    :type source_bucket_name: str
    :param list_url:
    :type list_url: str
    :param overwrite_objects_already_existing_in_sink
    :type overwrite_objects_already_existing_in_sink: bool
    :param delete_objects_unique_in_sink
    :type delete_objects_unique_in_sink: bool
    :param delete_objects_from_source_after_transfer
    :type delete_objects_from_source_after_transfer: bool
    :param schedule_start_date
    :type schedule_start_date: bool
    :param schedule_end_date
    :type schedule_end_date: bool
    :param start_time_of_day
    :type start_time_of_day: bool
    :param project_id: The Google Developers Console [project ID or project number]
    :type project_id: str
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: str
    :param api_version: The api version to use
    :type api_version: str
    """
    # [START gcp_transfer_job_update_template_fields]
    template_fields = ('operation_name', 'gcp_conn_id', 'api_version')
    # [END gcp_transfer_job_update_template_fields]

    @apply_defaults
    def __init__(self,
                 data_source,
                 target_bucket_name,
                 status=None,
                 description=None,
                 project_id=None,
                 min_time_elapsed_since_last_modification=None,
                 max_time_elapsed_since_last_modification=None,
                 include_prefixes=None,
                 exclude_prefixes=None,
                 source_bucket_name=None,
                 list_url=None,
                 overwrite_objects_already_existing_in_sink=None,
                 delete_objects_unique_in_sink=None,
                 delete_objects_from_source_after_transfer=None,
                 schedule_start_date=None,
                 schedule_end_date=None,
                 start_time_of_day=None,
                 aws_conn_id='aws_default',
                 gcp_conn_id='google_cloud_default',
                 api_version='v1',
                 *args,
                 **kwargs):
        super(GcpStorageTransferJobCreateOperator, self).__init__(*args, **kwargs)
        self.data_source = data_source
        self.status = status
        self.target_bucket_name = target_bucket_name
        self.description = description
        self.project_id = project_id
        self.min_time_elapsed_since_last_modification = min_time_elapsed_since_last_modification
        self.max_time_elapsed_since_last_modification = max_time_elapsed_since_last_modification
        self.include_prefixes = include_prefixes
        self.exclude_prefixes = exclude_prefixes
        self.source_bucket_name = source_bucket_name
        self.list_url = list_url
        self.overwrite_objects_already_existing_in_sink=overwrite_objects_already_existing_in_sink
        self.delete_objects_unique_in_sink=delete_objects_unique_in_sink
        self.delete_objects_from_source_after_transfer=delete_objects_from_source_after_transfer,
        self.schedule_start_date = schedule_start_date
        self.schedule_end_date = schedule_end_date
        self.start_time_of_day = start_time_of_day
        self.aws_conn_id = aws_conn_id
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self._check_input()

    def _check_input(self):
        pass

    def execute(self, context):
        hook = GCPTransferServiceHook(
            api_version=self.api_version,
            gcp_conn_id=self.gcp_conn_id,
        )
        if self.data_source == 'AWS_S3':
            aws_hook = AwsHook(self.aws_conn_id)
            aws_credentials = aws_hook.get_credentials()
            aws_access_key_id = aws_credentials.access_key
            aws_secret_access_key = aws_credentials.secret_key

        hook.create_transfer_job(
            project_id=self.project_id,
            status=self.status,
            target_bucket_name=self.target_bucket_name,
            description=self.description,
            min_time_elapsed_since_last_modification=self.min_time_elapsed_since_last_modification,
            max_time_elapsed_since_last_modification=self.max_time_elapsed_since_last_modification,
            include_prefixes=self.include_prefixes,
            exclude_prefixes=self.exclude_prefixes,
            source_bucket_name=self.source_bucket_name,
            list_url=self.list_url,
            overwrite_objects_already_existing_in_sink=self.overwrite_objects_already_existing_in_sink,
            delete_objects_unique_in_sink=self.delete_objects_unique_in_sink,
            delete_objects_from_source_after_transfer=self.delete_objects_from_source_after_transfer,
            schedule_start_date=self.schedule_start_date,
            schedule_end_date=self.schedule_end_date,
            start_time_of_day=self.start_time_of_day,
            aws_access_key_id=aws_access_key_id if self.data_source == 'AWS_S3' else None,
            aws_secret_access_key=aws_secret_access_key if self.data_source == 'AWS_S3' else None
        )


class GpcStorageTransferJobUpdateOperator(BaseOperator):

    # [START gcp_transfer_job_update_template_fields]
    template_fields = ('operation_name', 'gcp_conn_id', 'api_version')
    # [END gcp_transfer_job_update_template_fields]

    @apply_defaults
    def __init__(self,
                 operation_name,
                 api_version='v1',
                 gcp_conn_id='google_cloud_default',
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.operation_name = operation_name
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        hook = GCPTransferServiceHook(
            api_version=self.api_version,
            gcp_conn_id=self.gcp_conn_id,
        )
        hook.update_transfer_job(self.operation_name)


class GpcStorageTransferOperationsGetOperator(BaseOperator):
    """
    Get a state of an transfer operation in Google Storage Transfer Service.

    :param operation_name: Name of the transfer operation. Required.
    :type operation_name: str
    :param api_version: Optional, API version used. Defaults to v1.
    :type api_version: str
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud
    Platform. Defaults to 'google_cloud_default'.
    :type gcp_conn_id: str
    """
    # [START gcp_transfer_operation_get_template_fields]
    template_fields = ('operation_name', 'gcp_conn_id', 'api_version')
    # [END gcp_transfer_operation_get_template_fields]

    @apply_defaults
    def __init__(self,
                 operation_name,
                 api_version='v1',
                 gcp_conn_id='google_cloud_default',
                 *args,
                 **kwargs):

        super().__init__(*args, **kwargs)
        self.operation_name = operation_name
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self._validate_inputs()

    def _validate_inputs(self):
        if not self.operation_name:
            raise AirflowException("The required parameter 'operation_name' "
                                   "is empty or None")

    def execute(self, context):
        hook = GCPTransferServiceHook(
            api_version=self.api_version,
            gcp_conn_id=self.gcp_conn_id,
        )
        hook.get_transfer_operation(
            operation_name=self.operation_name
        )
        # TODO: What's next?
        return hook.cancel_transfer_operation(self.operation_name)


class GcpStorageTransferOperationsListOperator(BaseOperator):
    def __init__(self,
                 operation_name,
                 api_version='v1',
                 gcp_conn_id='google_cloud_default',
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.operation_name = operation_name
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self._validate_inputs()

    def _validate_inputs(self):
        if not self.operation_name:
            raise AirflowException("The required parameter 'operation_name' "
                                   "is empty or None")

    def execute(self, context):
        hook = GCPTransferServiceHook(
            api_version=self.api_version,
            gcp_conn_id=self.gcp_conn_id,
        )
        # hook.list_transfer_operation(self.operation_name)


class GcpStorageTransferOperationsPauseOperator(BaseOperator):
    """
    Pauses an transfer operation in Google Storage Transfer Service.

    :param operation_name: Name of the transfer operation. Required.
    :type operation_name: str
    :param api_version: Optional, API version used. Defaults to v1.
    :type api_version: str
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud
    Platform. Defaults to 'google_cloud_default'.
    :type gcp_conn_id: str
    """
    # [START gcp_transfer_operation_pause_template_fields]
    template_fields = ('operation_name', 'gcp_conn_id', 'api_version')
    # [END gcp_transfer_operation_pause_template_fields]

    @apply_defaults
    def __init__(self,
                 operation_name,
                 api_version='v1',
                 gcp_conn_id='google_cloud_default',
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.operation_name = operation_name
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self._validate_inputs()

    def _validate_inputs(self):
        if not self.operation_name:
            raise AirflowException("The required parameter 'operation_name' "
                                   "is empty or None")

    def execute(self, context):
        hook = GCPTransferServiceHook(
            api_version=self.api_version,
            gcp_conn_id=self.gcp_conn_id,
        )

        return hook.pause_transfer_operation(self.operation_name)


class GcpStorageTransferOperationsResumeOperator(BaseOperator):
    """
    Resumes an transfer operation in Google Storage Transfer Service.

    :param operation_name: Name of the transfer operation. Required.
    :type operation_name: str
    :param api_version: Optional, API version used. Defaults to v1.
    :type api_version: str
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud
     Platform. Defaults to 'google_cloud_default'.
    :type gcp_conn_id: str
    """
    # [START gcp_transfer_operation_resume_template_fields]
    template_fields = ('operation_name', 'gcp_conn_id', 'api_version')
    # [END gcp_transfer_operation_resume_template_fields]

    @apply_defaults
    def __init__(self,
                 operation_name,
                 api_version='v1',
                 gcp_conn_id='google_cloud_default',
                 *args,
                 **kwargs):

        self.operation_name = operation_name
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self._validate_inputs()
        super().__init__(*args, **kwargs)

    def _validate_inputs(self):
        if not self.operation_name:
            raise AirflowException("The required parameter 'operation_name' "
                                   "is empty or None")

    def execute(self, context):
        hook = GCPTransferServiceHook(
            api_version=self.api_version,
            gcp_conn_id=self.gcp_conn_id,
        )

        return hook.resume_transfer_operation(self.operation_name)


class GcpStorageTransferOperationsCancelOperator(BaseOperator):
    """
    Cancels an transfer operation in Google Storage Transfer Service.

    :param operation_name: Name of the transfer operation. Required.
    :type operation_name: str
    :param api_version: Optional, API version used. Defaults to v1.
    :type api_version: str
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud
        Platform. Defaults to 'google_cloud_default'.
    :type gcp_conn_id: str
    """
    # [START gcp_transfer_operation_cancel_template_fields]
    template_fields = ('operation_name', 'gcp_conn_id', 'api_version')
    # [END gcp_transfer_operation_cancel_template_fields]

    @apply_defaults
    def __init__(self,
                 operation_name,
                 api_version='v1',
                 gcp_conn_id='google_cloud_default',
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.operation_name = operation_name
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self._validate_inputs()

    def _validate_inputs(self):
        if not self.operation_name:
            raise AirflowException("The required parameter 'operation_name' "
                                   "is empty or None")

    def execute(self, context):
        hook = GCPTransferServiceHook(
            api_version=self.api_version,
            gcp_conn_id=self.gcp_conn_id,
        )
        return hook.cancel_transfer_operation(self.operation_name)
