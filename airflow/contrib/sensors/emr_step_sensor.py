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
<<<<<<< HEAD
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

from airflow.contrib.hooks.emr_hook import EmrHook
from airflow.contrib.sensors.emr_base_sensor import EmrBaseSensor
from airflow.utils import apply_defaults

_log = logging.getLogger(__name__)


class EmrStepSensor(EmrBaseSensor):
    """
    Asks for the state of the step until it reaches a terminal state.
    If it fails the sensor errors, failing the task.

    :param job_flow_id: job_flow_idwhich contains the step check the state of
    :type job_flow_id: string
    :param step_id: step to check the state of
    :type step_id: string
    """

    NON_TERMINAL_STATES = ['PENDING', 'RUNNING', 'CONTINUE']
    FAILED_STATE = 'FAILED'
    template_fields = ['job_flow_id', 'step_id']
    template_ext = ()

    @apply_defaults
    def __init__(
            self,
            job_flow_id,
            step_id,
            *args, **kwargs):
        super(EmrStepSensor, self).__init__(*args, **kwargs)
        self.job_flow_id = job_flow_id
        self.step_id = step_id

    def get_emr_response(self):
        emr = EmrHook(aws_conn_id=self.aws_conn_id).get_conn()

        _log.info('Poking step {0} on cluster {1}'.format(self.step_id, self.job_flow_id))
        return emr.describe_step(ClusterId=self.job_flow_id, StepId=self.step_id)

    def state_from_response(self, response):
        return response['Step']['Status']['State']
=======
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""This module is deprecated. Please use `airflow.providers.amazon.aws.sensors.emr_step`."""

import warnings

# pylint: disable=unused-import
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor  # noqa

warnings.warn(
    "This module is deprecated. Please use `airflow.providers.amazon.aws.sensors.emr_step`.",
    DeprecationWarning, stacklevel=2
)
>>>>>>> 0d5ecde61bc080d2c53c9021af252973b497fb7d
