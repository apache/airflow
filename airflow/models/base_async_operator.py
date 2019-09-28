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

from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.exceptions import AirflowException

class BaseAsyncOperator(BaseSensorOperator, SkipMixin):
    """
    AsyncOperators are derived from this class and inherit these attributes.

    AsyncOperators must define a `pre_execute` to fire a request for a
    long running operation with a method and then executes a `poll` method
    executing at a time interval and succeed when a criteria is met and fail
    if and when they time out. They are effctively an opinionated way use
    combine an Operator and a Sensor in order to kick off a long running
    process without blocking a worker slot while waiting for the long running
    process to complete by leveraging reschedule mode.

    :param soft_fail: Set to true to mark the task as SKIPPED on failure
    :type soft_fail: bool
    :param poke_interval: Time in seconds that the job should wait in
        between each tries
    :type poke_interval: int
    :param timeout: Time, in seconds before the task times out and fails.
    :type timeout: int
    :type mode: str
    """
    ui_color = '#9933ff'  # type: str
    valid_modes = ['poke', 'reschedule']  # type: Iterable[str]

    @apply_defaults
    def __init__(self,
                 *args,
                 **kwargs) -> None:
        super().__init__(mode='reschedule', *args, **kwargs)

    def pre_execute(self, context) -> None:
        """
        This method should kick off a long running operation.
        Context is the same dictionary used as when rendering jinja templates.

        Refer to get_template_context for more context.
        """
        raise AirflowException('Async Operators must define a `pre_execute` method.')
