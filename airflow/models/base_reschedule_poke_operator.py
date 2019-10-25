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

"""
Base Operator for kicking off a long running
operations and polling for completion with reschedule mode.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union
from datetime import timedelta

from airflow.exceptions import AirflowSensorTimeout, \
    AirflowSkipException, AirflowRescheduleException
from airflow.models import BaseOperator, SkipMixin, TaskReschedule
from airflow.models.xcom import XCOM_EXTERNAL_RESOURCE_ID_KEY
from airflow.utils import timezone
from airflow.utils.decorators import apply_defaults
from airflow.ti_deps.deps.ready_to_reschedule import ReadyToRescheduleDep


class BaseReschedulePokeOperator(BaseOperator, SkipMixin, ABC):
    """
    ReschedulePokeOperators are derived from this class and inherit these attributes.
    ReschedulePokeOperators should be used for long running operations where the task
    can tolerate a longer poke interval. They use the task rescheduling
    mechanism similar to sensors to avoid occupying a worker slot between
    pokes.

    Developing concrete operators that provide parameterized flexibility
    for synchronous or asynchronous poking depending on the invocation is
    possible by programing against this `BaseReschedulePokeOperator` interface,
    and overriding the execute method as demonstrated below.

    .. code-block:: python

        class DummyFlexiblePokingOperator(BaseReschedulePokeOperator):
          def __init__(self, async=False, *args, **kwargs):
            self.async = async
            super().__init(*args, **kwargs)

          def execute(self, context: Dict) -> None:
            if self.async:
              # use the BaseReschedulePokeOperator's execute
              super().execute(context)
            else:
              self.submit_request(context)
              while not self.poke():
                time.sleep(self.poke_interval)
              self.process_results(context)

          def sumbit_request(self, context: Dict) -> Optional[str]:
            return None

          def poke(self, context: Dict) -> bool:
            return bool(random.getrandbits(1))

    ReschedulePokeOperators must override the following methods:
    :py:meth:`submit_request`: fire a request for a long running operation
    :py:meth:`poke`: a method to check if the long running operation is
    complete it should return True when a success criteria is met.

    Optionally, ReschedulePokeOperators can override:
    :py:meth:`process_result` to perform any operations after the success
    criteria is met in :py:meth: `poke`

    :py:meth:`poke` is executed at a time interval and succeed when a
    criteria is met and fail if and when they time out.

    :param soft_fail: Set to true to mark the task as SKIPPED on failure
    :type soft_fail: bool
    :param poke_interval: Time in seconds that the job should wait in
        between each tries
    :type poke_interval: int
    :param timeout: Time, in seconds before the task times out and fails.
    :type timeout: int

    """
    ui_color = '#9933ff'  # type: str

    @apply_defaults
    def __init__(self,
                 poke_interval: float = 60,
                 timeout: float = 60 * 60 * 24 * 7,
                 soft_fail: bool = False,
                 *args,
                 **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.mode = 'reschedule'
        self.poke_interval = poke_interval
        self.soft_fail = soft_fail
        self.timeout = timeout

    @abstractmethod
    def submit_request(self, context: Dict) -> Optional[Union[str, List, Dict]]:
        """
        This method should kick off a long running operation.
        This method should return the ID for the long running operation if
        applicable.
        Context is the same dictionary used as when rendering jinja templates.

        Refer to get_template_context for more context.

        :returns: a resource_id for the long running operation.
        :rtype: Optional[Union[String, List, Dict]]
        """
        raise NotImplementedError

    def poke(self, context: Dict) -> bool:
        """
        Function that the sensors defined while deriving this class should
        override.
        """
        raise NotImplementedError

    def process_result(self, context: Dict):
        """
        This method can optionally be overriden to process the result of a long running operation.
        Context is the same dictionary used as when rendering jinja templates.

        Refer to get_template_context for more context.
        """
        self.log.info('Using default process_result. Got result of %s. Done.',
                      self.get_external_resource_id(context))

    def execute(self, context: Dict) -> None:
        # On the first execute call submit_request and set the
        # external resource id.

        # pylint: disable=no-value-for-parameter
        task_reschedules = TaskReschedule.find_for_task_instance(context['ti'])
        if not task_reschedules:
            resource_id = self.submit_request(context)
            self.set_state(context, resource_id)

        self.handle_reschedule(context)
        resource_id = self.get_external_resource_id(context)
        self.log.info("Calling process_result for %s.", resource_id)
        self.process_result(context)

    @staticmethod
    def set_state(context, value):
        """
        Utility for setting the XCom for the external resource id.
        :param context: Template rendering context
        :type context: dict
        :param value: the resource id to store in XCom.
        :type value: Union[str, Dict, List]
        """
        return context['ti'].xcom_push(key=XCOM_EXTERNAL_RESOURCE_ID_KEY,
                                       value=value)

    @staticmethod
    def get_external_resource_id(context):
        """
        Utility for getting the XCom for the external resource id.
        :param context: Template rendering context
        :type context: dict
        """
        return context['ti'].xcom_pull(task_ids=context['task'].task_id,
                                       key=XCOM_EXTERNAL_RESOURCE_ID_KEY)

    def handle_reschedule(self, context: Dict) -> None:
        """
        Reschedules the task at the next poke interval until the success
        criteria is met.
        :param context: Template rendering context
        :type context: dict
        """
        started_at = timezone.utcnow()
        # If reschedule, use first start date of current try
        # pylint: disable=no-value-for-parameter
        task_reschedules = TaskReschedule.find_for_task_instance(context['ti'])
        if task_reschedules:
            started_at = task_reschedules[0].start_date
        while not self.poke(context):
            if (timezone.utcnow() - started_at).total_seconds() > self.timeout:
                # If sensor is in soft fail mode but will be retried then
                # give it a chance and fail with timeout.
                # This gives the ability to set up non-blocking AND soft-fail sensors.
                if self.soft_fail and not context['ti'].is_eligible_to_retry():
                    self._do_skip_downstream_tasks(context)
                    raise AirflowSkipException('Snap. Time is OUT.')
                else:
                    raise AirflowSensorTimeout('Snap. Time is OUT.')
            reschedule_date = timezone.utcnow() + timedelta(
                seconds=self.poke_interval)
            raise AirflowRescheduleException(reschedule_date)

    def _do_skip_downstream_tasks(self, context: Dict) -> None:
        downstream_tasks = context['task'].get_flat_relatives(upstream=False)
        self.log.debug("Downstream task_ids %s", downstream_tasks)
        if downstream_tasks:
            self.skip(context['dag_run'], context['ti'].execution_date, downstream_tasks)

    @property
    def reschedule(self):
        """set mode to reschedule"""
        return self.mode == 'reschedule'

    @property
    def deps(self):
        """
        Adds one additional dependency for all reschedul operators that
        checks if a sensor task instance can be rescheduled.
        """
        # pylint: disable=no-member
        return BaseOperator.deps.fget(self) | {ReadyToRescheduleDep()}
