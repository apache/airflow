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
Base Asynchronous Operator for kicking off a long running
operations and polling for completion with reschedule mode.
"""
from abc import abstractmethod
from functools import wraps
from typing import Dict, List, Union, Optional

from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.exceptions import AirflowException, AirflowRescheduleException
from airflow.models.xcom import XCOM_EXTERNAL_RESOURCE_ID_KEY
from airflow.models import TaskReschedule

PLACEHOLDER_RESOURCE_ID = 'RESOURCE_ID_NOT_APPLICABLE'

class BaseAsyncOperator(BaseSensorOperator, SkipMixin):
    """
    AsyncOperators are derived from this class and inherit these attributes.

    AsyncOperators must override the following methods:
        :meth:`submit_request`: fire a request for a long running operation
        :meth:`poke`: a method to check if the long running operation is
            complete it should return True when a success criteria is met.
    Optionally, AsyncOperators can override:
        :meth: `process_result` to perform any operations after the success
            criteria is met in :meth: `poke`
    :meth: `poke` is executed at a time interval and succeed when a
    criteria is met and fail if and when they time out. They are effctively
    an opinionated way use combine an Operator and a Sensor in order to kick
    off a long running process and poll for completion without tying up a
    worker slot while waiting between pokes leveraging reschedule mode.

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
                 *args,
                 **kwargs) -> None:
        super().__init__(mode='reschedule', *args, **kwargs)

    @abstractmethod
    def submit_request(self, context) -> Optional[Union[String, List, Dict]]:
        """
        This method should kick off a long running operation.
        This method should return the ID for the long running operation if
        applicable.
        Context is the same dictionary used as when rendering jinja templates.

        Refer to get_template_context for more context.

        :returns: a resource_id for the long running operation.
        :rtype: Optional[Union[String, List, Dict]]
        """
        raise AirflowException('Async Operators must override the `submit_request` method.')

    def process_result(self, context):
        """
        This method can optionally be overriden to process the result of a long running operation.
        Context is the same dictionary used as when rendering jinja templates.

        Refer to get_template_context for more context.
        """
        self.log.info('Using default process_result. Got result of %s. Done.',
                      self.get_external_resource_id(context))

    def execute(self, context):
        # On the first execute call submit_request and set the
        # external resource id.
        task_reschedules = TaskReschedule.find_for_task_instance(context['ti'])
        if not task_reschedules:
            resource_id = self.submit_request(self, context)
            if not resource_id:
                resource_id = PLACEHOLDER_RESOURCE_ID
            self.set_external_resource_id(context, resource_id)

        super().execute(self, context)

        # The above should raise AirflowRescheduleException if we are
        # rescheduling a poke, and thus never reach this code below.
        try:
            resource_id = self.get_external_resource_id(context)
            if resource_id == PLACEHOLDER_RESOURCE_ID:
                self.log.info("Calling process_result for %s.", resource_id)
            else:
                self.log.info("Calling process_result.")
            self.process_result(context)
        finally:
            #TODO(mik-laj) is there a way to clear this key?
            # Clear the resource id for this task.
            self.set_external_resource_id(context, None)

    @staticmethod
    def set_external_resource_id(context, value):
        return context['ti'].xcom_push(key=XCOM_EXTERNAL_RESOURCE_ID_KEY,
                                       value=value)

    @staticmethod
    def get_external_resource_id(context):
        return context['ti'].xcom_pull(task_ids=context['task'].task_id,
                                       key=XCOM_EXTERNAL_RESOURCE_ID_KEY)
