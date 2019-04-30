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

from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

from celery.app import control


class CeleryQueueSensor(BaseSensorOperator):
    """
    Waits for a Celery queue to be empty. By default, in order to be considered
    empty, the queue must not have any tasks in the ``reserved``, ``scheduled``
    or ``active`` states.

    :param celery_queue: The name of the Celery queue to wait for.
    :type celery_queue: str
    :param target_task_id: Task id for checking
    :type target_task_id: str
    """
    @apply_defaults
    def __init__(
            self,
            celery_queue,
            target_task_id=None,
            *args,
            **kwargs):

        super().__init__(*args, **kwargs)
        self.celery_queue = celery_queue
        self.target_task_id = target_task_id

    def _check_task_id(self, context):
        """
        Gets the returned Celery result from the Airflow task
        ID provided to the sensor, and returns True if the
        celery result has been finished execution.

        :param context: Airflow's execution context
        :type context: dict
        :return: True if task has been executed, otherwise False
        :rtype: bool
        """
        ti = context['ti']
        celery_result = ti.xcom_pull(task_ids=self.target_task_id)
        return celery_result.ready()

    def poke(self, context):

        if self.target_task_id:
            return self._check_task_id(context)

        inspect_result = control.Inspect()
        reserved = inspect_result.reserved()
        scheduled = inspect_result.scheduled()
        active = inspect_result.active()

        try:
            reserved = len(reserved[self.celery_queue])
            scheduled = len(scheduled[self.celery_queue])
            active = len(active[self.celery_queue])

            self.log.info(
                'Checking if celery queue %s is empty.', self.celery_queue
            )

            return reserved == 0 and scheduled == 0 and active == 0
        except KeyError:
            raise KeyError(
                'Could not locate Celery queue {0}'.format(
                    self.celery_queue
                )
            )
