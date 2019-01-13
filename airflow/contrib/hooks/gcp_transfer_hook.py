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

import json
import time
import datetime
from googleapiclient.discovery import build

from airflow.exceptions import AirflowException
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook

# Time to sleep between active checks of the operation results
TIME_TO_SLEEP_IN_SECONDS = 10


class GcpTransferJobsStatus:
    ENABLED = "ENABLED"
    DISABLED = "DISABLED"
    DELETED = "DELETED"


class GcpTransferOperationStatus:
    IN_PROGRESS = "IN_PROGRESS"
    PAUSED = "PAUSED"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    ABORTED = "ABORTED"


class GcpTransferSource:
    GCS = "GCS"
    AWS_S3 = "AWS_S3"
    HTTP = "HTTP"


# noinspection PyAbstractClass
class GCPTransferServiceHook(GoogleCloudBaseHook):
    """
    Hook for GCP Storage Transfer Service.
    """
    _conn = None

    def __init__(self,
                 api_version='v1',
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None):
        super(GCPTransferServiceHook, self).__init__(gcp_conn_id, delegate_to)
        self.api_version = api_version

    def get_conn(self):
        """
        Retrieves connection to Google Storage Transfer service.

        :return: Google Storage Transfer service object
        :rtype: dict
        """
        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build('storagetransfer', self.api_version,
                               http=http_authorized, cache_discovery=False)
        return self._conn

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def create_transfer_job(self,
                            project_id,
                            data_source,
                            target_bucket_name,
                            status=None,
                            description=None,
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
                            aws_access_key_id=None,
                            aws_secret_access_key=None
                            ):

        transferSpec = {
            "object_conditions": {
                "min_time_elapsed_since_last_modification": min_time_elapsed_since_last_modification,
                "max_time_elapsed_since_last_modification": max_time_elapsed_since_last_modification,
                "include_prefixes": include_prefixes,
                "exclude_prefixes": exclude_prefixes,
            },
            "transfer_options": {
                "overwrite_objects_already_existing_in_sink": overwrite_objects_already_existing_in_sink,
                "delete_objects_unique_in_sink": delete_objects_unique_in_sink,
                "delete_objects_from_source_after_transfer": delete_objects_from_source_after_transfer
            },
            "gcs_data_sink": {
                "gcs_data_sink": target_bucket_name
            }
        }

        if data_source == GcpTransferSource.GCS:
            transferSpec["gcs_data_source"] = {
                "bucket_name": source_bucket_name
            }
        elif data_source == GcpTransferSource.AWS_S3:
            transferSpec["aws_s3_data_source"] = {
                "bucket_name": source_bucket_name,
                "aws_access_key": {
                    "access_key_id": aws_access_key_id,
                    "aws_secret_access_key": aws_secret_access_key
                }
            }
        elif data_source == GcpTransferSource.HTTP:
            transferSpec["http_data_source"] = {
                "list_url": list_url
            }
        else:
            raise AirflowException("The parameter 'data_source' received an "
                                   "unexpected value %s. Allowed values: %s" %
                                   (data_source, (GcpTransferSource.AWS_S3,
                                                  GcpTransferSource.GCS,
                                                  GcpTransferSource.HTTP)))

        body = {
            "description": description,
            "project_id": project_id,
            "transfer_spec": transferSpec,
            "schedule": {
                "schedule_start_date": {
                    "year": schedule_start_date.year,
                    "month": schedule_start_date.month,
                    "day": schedule_start_date.day
                },
                "schedule_end_date": {
                    "year": schedule_end_date.year,
                    "month": schedule_end_date.month,
                    "day": schedule_end_date.day
                },
                "start_time_of_day": {
                    "hours": start_time_of_day.hour,
                    "minutes": start_time_of_day.minutes,
                    "seconds": start_time_of_day.seconds,
                }
            },
            "status": status, # ENABLED/DISABLED
        }
        self.get_conn().transferOperations().create(body=body).execute()

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def update_transfer_job(self, project_id, job_name, description):
        bdoy = {
            "project_id": project_id,
            "transfer_job": {
                "description": "",
                "transferSpec": {},
                "status": GcpTransferJobsStatus.ENABLED
            },
            "update_transfer_job_field_mask": ""
        }
        self.get_conn().transferOperations().update(jobName=job_name, body=bdoy)

    def cancel_transfer_operation(self, operation_name):
        self.get_conn().transferOperations().cancel(name=operation_name)

    def delete_transfer_operation(self, operation_name):
        self.get_conn().transferOperations().delete(name=operation_name)

    def get_transfer_operation(self,  operation_name):
        return self.get_conn().transferOperations().list(name=operation_name)

    def list_transfer_operations(self, operation_name):
        conn = self.get_conn()

        operations = []

        request = conn.transferOperations().list(name=operation_name)
        while request is not None:
            response = request.execute()
            operations.extend(response['operations'])

            request = conn.transferOperations().list_next(
                previous_request=request,
                previous_response=response)

        return operations

    def pause_transfer_operation(self,  operation_name):
        self.get_conn()\
            .transferOperations()\
            .pause(name=operation_name)\
            .execute()

    def resume_transfer_operation(self,  operation_name):
        self.get_conn()\
            .transferOperations()\
            .resume(name=operation_name)\
            .execute()

    def wait_for_transfer_job(self, job):
        while True:
            result = self.get_conn().transferOperations().list(
                name='transferOperations',
                filter=json.dumps({
                    'project_id': job['projectId'],
                    'job_names': [job['name']],
                }),
            ).execute()
            if GCPTransferServiceHook._check_operations_result(result):
                return True
            time.sleep(TIME_TO_SLEEP_IN_SECONDS)

    @staticmethod
    def _check_operations_result(result):
        operations = result.get('operations', [])
        if len(operations) == 0:
            return False
        for operation in operations:
            status = operation['metadata']['status']
            if status in {
                GcpTransferOperationStatus.FAILED,
                GcpTransferOperationStatus.ABORTED
            }:
                name = operation['name']
                # TODO: Better error message
                raise AirflowException('Operation {} {}'.format(
                    name, status))
            if status != GcpTransferOperationStatus.SUCCESS:
                return False
        return True
