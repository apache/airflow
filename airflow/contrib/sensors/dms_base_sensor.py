# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from airflow.operators.sensors import BaseSensorOperator
from airflow.contrib.hooks.dms_hook import DMSHook
from airflow.utils import apply_defaults
from airflow.exceptions import AirflowException


class DMSBaseSensor(BaseSensorOperator):
    """
    Contains general sensor behavior for EMR.
    Subclasses should implement get_emr_response() and state_from_response() methods.
    Subclasses should also implement NON_TERMINAL_STATES and FAILED_STATE constants.
    """
    ui_color = '#66c3ff'
    template_fields = ['replication_task_arn']
    template_ext = ()

    @apply_defaults
    def __init__(
            self,
            replication_task_arn,
            aws_conn_id='aws_default',
            *args, **kwargs):
        super(DMSBaseSensor, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.replication_task_arn = replication_task_arn

    def poke(self, context):
        response = self.get_dms_response()

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            self.log.info('Bad HTTP response: %s', response)
            return False

        status = self.status_from_response(response)
        self.log.info('DMS Replication task (%s) currently %s', self.replication_task_arn, status)

        if status in self.NON_TERMINAL_STATUSES:
            return False

        if status in self.FAILED_STATUSES:
            raise AirflowException('DMS replication task failed')

        if status in self.TARGET_STATUSES:
            return True

        self.log.info('DMS Replication task (%s) will ignore unexpected '
                      'status: %s', self.replication_task_arn, status)
        return False

    def get_dms_response(self):
        self.log.info('Poking replication task %s', self.replication_task_arn)
        return self.get_client().describe_replication_tasks(
            Filters=[
                {
                    'Name': 'replication-task-arn',
                    'Values': [
                        self.replication_task_arn,
                        ]
                },
            ],
        )

    def stop_reason_handling(self, stop_reason):
        pass

    def status_from_response(self, response):
        replication_task = response['ReplicationTasks'][0]
        if 'StopReason' in replication_task:
            stop_reason = replication_task['StopReason']
            self.stop_reason_handling(stop_reason)
            self.log.info(
                'Replication task: %s stopped with '
                'StopReason: %s', self.replication_task_arn, stop_reason
            )
        return replication_task['Status']

    def get_client(self):
        return DMSHook(
            aws_conn_id=self.aws_conn_id
        )
