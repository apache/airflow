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

    def create_transfer_job(self, description, schedule, transfer_spec, project_id=None):
        transfer_job = {
            'status': 'ENABLED',
            'projectId': project_id or self.project_id,
            'description': description,
            'transferSpec': transfer_spec,
            'schedule': schedule or self._schedule_once_now(),
        }
        return self.get_conn().transferJobs().create(body=transfer_job).execute()

    def wait_for_transfer_job(self, job):
        while True:
            result = self.get_conn().transferOperations().list(
                name='transferOperations',
                filter=json.dumps({
                    'project_id': job['projectId'],
                    'job_names': [job['name']],
                }),
            ).execute()
            if self._check_operations_result(result):
                return True
            time.sleep(TIME_TO_SLEEP_IN_SECONDS)

    def _check_operations_result(self, result):
        operations = result.get('operations', [])
        if len(operations) == 0:
            return False
        for operation in operations:
            if operation['metadata']['status'] in {'FAILED', 'ABORTED'}:
                raise AirflowException('Operation {} {}'.format(
                    operation['name'], operation['metadata']['status']))
            if operation['metadata']['status'] != 'SUCCESS':
                return False
        return True

    def _schedule_once_now(self):
        now = datetime.datetime.utcnow()
        return {
            'scheduleStartDate': {
                'day': now.day,
                'month': now.month,
                'year': now.year,
            },
            'scheduleEndDate': {
                'day': now.day,
                'month': now.month,
                'year': now.year,
            }
        }
