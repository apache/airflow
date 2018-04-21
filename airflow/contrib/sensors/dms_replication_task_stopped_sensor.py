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

from airflow.exceptions import AirflowException
from airflow.contrib.sensors.dms_base_sensor import DMSBaseSensor
from airflow.utils import apply_defaults


class DMSReplicationTaskStoppedSensor(DMSBaseSensor):
    """
    Asks for the state of the step until it reaches a terminal state.
    If it fails the sensor errors, failing the task.

    :param job_flow_id: job_flow_id which contains the step check the state of
    :type job_flow_id: string
    :param step_id: step to check the state of
    :type step_id: string
    """

    NON_TERMINAL_STATUSES = ['creating', 'running', 'ready', 'starting']
    FAILED_STATUSES = ['failed']
    TARGET_STATUSES = ['stopped']

    @apply_defaults
    def __init__(
            self,
            # job_flow_id,
            # step_id,
            stop_reasons,
            *args, **kwargs):
        super(DMSReplicationTaskStoppedSensor, self).__init__(*args, **kwargs)
        self.stop_reasons = stop_reasons

    def stop_reason_handling(self, stop_reason):
        if stop_reason not in self.stop_reasons:
            raise AirflowException(
                'DMS replication task: %s failed to stop with acceptable '
                'reasons %s but instead %s',
                self.replication_task_arn, self.stop_reasons, stop_reason
            )
