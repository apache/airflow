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

import pendulum

from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils import timezone
from airflow.utils.decorators import apply_defaults


class TimeDeltaSensor(BaseSensorOperator):
    """
    Waits for a timedelta after the task's execution_date + schedule_interval.
    If delta_on_upstream is set to True and upstream tasks are there,
    then it will wait for a timedelta after upstream tasks end time.
    In Airflow, the daily task stamped with ``execution_date``
    2016-01-01 can only start running on 2016-01-02. The timedelta here
    represents the time after the execution period has closed.

    :param delta: time length to wait after execution_date before succeeding
    :type delta: datetime.timedelta
    :param delta_on_upstream: flag to add delta to upstream end time
    :type delta_on_upstream: bool
    """

    @apply_defaults
    def __init__(self, delta, delta_on_upstream=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.delta = delta
        self.delta_on_upstream = delta_on_upstream

    def poke(self, context):
        dag = context['dag']
        target_dttm = dag.following_schedule(context['execution_date'])
        # check for upstream task end times if upstream tasks are there and delta_on_upstream flag is True
        self.log.error('Upstream Flag: (%s), Upstreams: (%s)', self.delta_on_upstream, self.upstream_task_ids)
        if self.delta_on_upstream and self.upstream_task_ids and context['dag_run']:
            dag_run = context['dag_run']
            for upstream_task_id in self.upstream_task_ids:
                upstream_task_instance = dag_run.get_task_instance(task_id=upstream_task_id)
                if upstream_task_instance.end_date and target_dttm < upstream_task_instance.end_date:
                    target_dttm = pendulum.instance(upstream_task_instance.end_date)
        target_dttm += self.delta
        self.log.info('Checking if the time (%s) has come', target_dttm)
        return timezone.utcnow() > target_dttm
