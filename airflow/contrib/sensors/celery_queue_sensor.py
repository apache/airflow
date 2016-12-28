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

import logging

from airflow.operators.sensors import BaseSensorOperator
from airflow.utils import apply_defaults
from celery.task.control import inspect


class CeleryQueueSensor(BaseSensorOperator):
    """
    Waits for a Celery queue to be empty. By default, in order to be considered 
    empty, the queue must not have any tasks in the ``reserved``, ``scheduled`` 
    or ``active`` states.

    :param celery_queue: The name of the Celery queue to wait for.
    :type celery_queue: str
    """
    @apply_defaults
    def __init__(
            self,
            celery_queue,
            af_task_id=None,
            *args,
            **kwargs):
        """The constructor."""
        super(CeleryQueueSensor, self).__init__(*args, **kwargs)
        self.celery_queue = celery_queue
        self.af_task_id = af_task_id

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
        celery_result = ti.xcom_pull(task_ids=self.af_task_id)
        return celery_result.ready()

    def poke(self, context):
        """Poke."""
        from celery.task.control import inspect

        if self.af_task_id:
            return self._check_task_id(context)

        i = inspect()
        reserved = i.reserved()
        scheduled = i.scheduled()
        active = i.active()

        try:
            reserved = len(reserved[self.celery_queue])
            scheduled = len(scheduled[self.celery_queue])
            active = len(active[self.celery_queue])

            logging.info(
                'Checking if celery queue {0} is empty.'.format(
                    self.celery_queue
                )
            )

            return reserved == 0 and scheduled == 0 and active == 0
        except KeyError:
            raise KeyError(
                'Could not locate Celery queue {0}'.format(
                    self.celery_queue
                )
            )
