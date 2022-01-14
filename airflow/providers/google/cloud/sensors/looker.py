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
#
"""This module contains Google Cloud Looker sensors."""

from typing import Optional

from airflow.exceptions import AirflowException
from airflow.sensors.base import BaseSensorOperator
from airflow.providers.google.cloud.hooks.looker import LookerHook, JobStatus


class LookerCheckPdtBuildSensor(BaseSensorOperator):
    """
    Check for the state of a previously submitted PDT materialization job.

    :param materialization_id: Required. The materialization job ID to poll. (templated)
    :type materialization_id: str
    :param looker_conn_id: Required. The connection ID to use connecting to Looker.
    :type looker_conn_id: str
    :param cancel_on_kill: Optional. Flag which indicates whether cancel the hook's job or not, when on_kill is called.
    :type cancel_on_kill: bool
    """

    template_fields = ["materialization_id"]

    def __init__(
        self,
        materialization_id: str,
        looker_conn_id: str,
        cancel_on_kill: bool = True,
        **kwargs) -> None:
        super().__init__(**kwargs)
        self.materialization_id = materialization_id
        self.looker_conn_id = looker_conn_id
        self.cancel_on_kill = cancel_on_kill
        self.hook: Optional[LookerHook] = None

    def poke(self, context):

        # Airflow 1 support (tentative - possible follow-up feature)
        # (can be done in DAG during task creation, not here, so materialization_id is always passed)
        # self.materialization_id = self.xcom_pull(task_ids='looker_start_task_id', key='materialization_id', context=context)

        # Airflow 2: use templated var pulling output from start task

        self.hook = LookerHook(looker_conn_id=self.looker_conn_id)

        status = self.hook.pdt_build_status(self.materialization_id)

        if status == JobStatus.ERROR.value:
            raise AirflowException(f'PDT materialization job failed. Job id: {self.materialization_id}.')
        elif status == JobStatus.CANCELLED.value:
            raise AirflowException(f'PDT materialization job was cancelled. Job id: {self.materialization_id}.')
        elif status == JobStatus.DONE.value:
            self.log.debug("PDT materialization job completed successfully. Job id: %s.", self.materialization_id)
            return True

        self.log.info("Waiting for PDT materialization job to complete. Job id: %s.", self.materialization_id)
        return False

    def on_kill(self):
        if self.materialization_id and self.cancel_on_kill:
            self.hook.stop_pdt_build(materialization_id=self.materialization_id)
