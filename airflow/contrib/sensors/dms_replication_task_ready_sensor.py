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

from airflow.contrib.sensors.dms_base_sensor import DMSBaseSensor
from airflow.utils import apply_defaults


class DMSReplicationTaskReadySensor(DMSBaseSensor):
    """
        Asks for the state of the step until it reaches a terminal state.
        If it fails the sensor errors, failing the task.

        :param job_flow_id: job_flow_idwhich contains the step check the state of
        :type job_flow_id: string
        :param step_id: step to check the state of
        :type step_id: string
        """

    NON_TERMINAL_STATUSES = ['creating', 'starting', 'running']
    FAILED_STATUSES = ['failed', 'stopped']
    TARGET_STATUSES = ['ready']

    @apply_defaults
    def __init__(
        self,
        # job_flow_id,
        # step_id,
        *args, **kwargs):
        super(DMSReplicationTaskReadySensor, self).__init__(*args, **kwargs)
