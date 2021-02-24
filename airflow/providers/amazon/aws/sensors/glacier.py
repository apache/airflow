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
from enum import Enum
from typing import Any

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.glacier import GlacierHook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class JobStatus(Enum):
    """Glacier jobs description"""

    IN_PROGRESS = "InProgress"
    SUCCEEDED = "Succeeded"


class GlacierJobOperationSensor(BaseSensorOperator):
    """
    Glacier sensor for checking job state. This operator runs only in reschedule mode.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GlacierJobOperationSensor`

    :param aws_conn_id: The reference to the AWS connection details
    :type aws_conn_id: str
    :param vault_name: name of Glacier vault on which job is executed
    :type vault_name: str
    :param job_id: the job ID was returned by retrieve_inventory()
    :type job_id: str
    :param poke_interval: Time in seconds that the job should wait in
        between each tries
    :type poke_interval: float
    :param mode: How the sensor operates.
        Options are: ``{ poke | reschedule }``, default is ``poke``.
        When set to ``poke`` the sensor is taking up a worker slot for its
        whole execution time and sleeps between pokes. Use this mode if the
        expected runtime of the sensor is short or if a short poke interval
        is required. Note that the sensor will hold onto a worker slot and
        a pool slot for the duration of the sensor's runtime in this mode.
        When set to ``reschedule`` the sensor task frees the worker slot when
        the criteria is not yet met and it's rescheduled at a later time. Use
        this mode if the time before the criteria is met is expected to be
        quite long. The poke interval should be more than one minute to
        prevent too much load on the scheduler.
    :type mode: str
    """

    template_fields = ["vault_name", "job_id"]

    @apply_defaults
    def __init__(
        self,
        *,
        aws_conn_id: str = 'aws_default',
        vault_name: str,
        job_id: str,
        poke_interval: int = 60 * 20,
        mode: str = "reschedule",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.vault_name = vault_name
        self.job_id = job_id
        self.poke_interval = poke_interval
        self.mode = mode

    def poke(self, context) -> bool:
        hook = GlacierHook(aws_conn_id=self.aws_conn_id)
        response = hook.describe_job(vault_name=self.vault_name, job_id=self.job_id)

        if response["StatusCode"] == JobStatus.SUCCEEDED.value:
            self.log.info("Job status: %s, code status: %s", response["Action"], response["StatusCode"])
            self.log.info("Job finished successfully")
            return True
        elif response["StatusCode"] == JobStatus.IN_PROGRESS.value:
            self.log.info("Processing...")
            self.log.warning("Code status: %s", response["StatusCode"])
            return False
        else:
            raise AirflowException(
                f'Sensor failed. Job status: {response["Action"]}, code status: {response["StatusCode"]}'
            )
