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

import time

from airflow import settings
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.utils.state import State
from airflow.utils.decorators import apply_defaults
from airflow.utils.db import create_session, provide_session


class SubDagOperator(BaseOperator):
    """
    This runs a sub dag. By convention, a sub dag's dag_id
    should be prefixed by its parent and a dot. As in `parent.child`.

    :param subdag: the DAG object to run as a subdag of the current DAG.
    :param poke_interval: how often we check if the subdag is finished
    """

    ui_color = '#555'
    ui_fgcolor = '#fff'

    @provide_session
    @apply_defaults
    def __init__(
            self,
            subdag: DAG,
            poke_interval: int=10,
            *args, **kwargs):

        super().__init__(*args, **kwargs)
        self.poke_interval = poke_interval
        self.subdag = subdag
        dag = kwargs.get('dag') or settings.CONTEXT_MANAGER_DAG
        if not dag:
            raise AirflowException('Please pass in the `dag` param or call '
                                   'within a DAG context manager')

        # validate subdag name
        if dag.dag_id + '.' + kwargs['task_id'] != subdag.dag_id:
            raise AirflowException(
                "The subdag's dag_id should have the form "
                "'{{parent_dag_id}}.{{this_task_id}}'. Expected "
                "'{d}.{t}'; received '{rcvd}'.".format(
                    d=dag.dag_id, t=kwargs['task_id'], rcvd=subdag.dag_id))

        if self.pool:
            raise AirflowException("SubDagOperator should not occupy pool slots.")

    @provide_session
    def _get_dagrun(self, execution_date, session=None):
        return (
            session.query(DagRun)
            .filter(DagRun.dag_id == self.subdag.dag_id)
            .filter(DagRun.execution_date == execution_date)
            .first()
        )

    def execute(self, context):
        execution_date = context['execution_date']
        with create_session() as session:
            dag_run = (
                session.query(DagRun)
                .filter(DagRun.dag_id == self.subdag.dag_id)
                .filter(DagRun.execution_date == execution_date)
                .first()
            )

            if dag_run is None:
                dag_run = self.subdag.create_dagrun(
                    run_id="scheduled__{}".format(execution_date.isoformat()),
                    execution_date=execution_date,
                    state=State.RUNNING,
                    external_trigger=True,
                )

                self.log.info("Created DagRun: %s", dag_run.run_id)
            else:
                self.log.info("Found existing DagRun: %s", dag_run.run_id)

            while dag_run.state == State.RUNNING:
                self.log.info("dag run is still running...")
                time.sleep(self.poke_interval)
                dag_run = self._get_dagrun(execution_date=execution_date)

        self.log.info("Execution finished. State is %s", dag_run.state)

        if dag_run.state != State.SUCCESS:
            raise AirflowException(
                "Expected state: SUCCESS. Actual state: {}".format(dag_run.state))

