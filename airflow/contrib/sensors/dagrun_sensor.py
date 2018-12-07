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
from airflow import settings
from airflow.exceptions import AirflowException
from airflow.utils.state import State
from airflow.utils.decorators import apply_defaults
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import DagRun
from airflow.sensors.base_sensor_operator import BaseSensorOperator


class DagRunSensor(BaseSensorOperator):
    """
    Waits for DAG run(s) to complete and checks status.

    :param trigger_task_id: The id of the task that triggered the dags
        and returns a list of dagrun ids to monitor.
    :type trigger_task_id: str
    :param sensor_rule: criteria for success after dagruns complete.
        Default is ``TriggerRule.ONE_SUCCESS``
    :type sensor_rule: str
    """
    @apply_defaults
    def __init__(
            self,
            trigger_task_id,
            sensor_rule=None,
            *args, **kwargs):
        super(DagRunSensor, self).__init__(*args, **kwargs)
        self.sensor_rule = sensor_rule or TriggerRule.ONE_SUCCESS
        self.trigger_task_id = trigger_task_id

    def poke(self, context):
        session = settings.Session()
        try:
            runcount = 0
            ti = context['ti']
            dagrun_ids = ti.xcom_pull(task_ids=self.trigger_task_id)
            if dagrun_ids:
                ids = dagrun_ids[:2]
                ids = ids + ['...'] if len(dagrun_ids) > 2 else ids
                logging.info('Poking for {}'.format(','.join(ids)))
                runcount = session.query(DagRun).filter(
                    DagRun.run_id.in_(dagrun_ids),
                    DagRun.state == State.RUNNING,
                ).count()
            else:
                raise AirflowException("No dagrun ids returned by '{}'".format(
                    self.trigger_task_id))
            logging.info('runcount={}'.format(runcount))
            if runcount == 0:
                successcount = session.query(DagRun).filter(
                    DagRun.run_id.in_(dagrun_ids),
                    DagRun.state == State.SUCCESS,
                ).count()
                if self.sensor_rule == TriggerRule.ONE_SUCCESS:
                    if successcount == 0:
                        raise AirflowException('No dagruns completed successfully.')
                else:
                    raise AirflowException("sensor rule '{}' is not supported".format(
                        self.sensor_rule))
            return runcount == 0
        finally:
            session.close()
