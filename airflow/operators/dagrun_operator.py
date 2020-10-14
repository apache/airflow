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

import datetime
from typing import Dict, Optional, Union
from urllib.parse import quote

from airflow import settings
from airflow.api.common.experimental.trigger_dag import trigger_dag
from airflow.exceptions import DagNotFound, DagRunAlreadyExists
from airflow.models import BaseOperator, BaseOperatorLink, DagBag, DagModel, DagRun
from airflow.utils import timezone
from airflow.utils.decorators import apply_defaults
from airflow.utils.types import DagRunType


class TriggerDagRunLink(BaseOperatorLink):
    """
    Operator link for TriggerDagRunOperator. It allows users to access
    DAG triggered by task using TriggerDagRunOperator.
    """

    name = 'Triggered DAG'

    def get_link(self, operator, dttm):
        return f"/graph?dag_id={operator.trigger_dag_id}&root=&execution_date={quote(dttm.isoformat())}"


class TriggerDagRunOperator(BaseOperator):
    """
    Triggers a DAG run for a specified ``dag_id``

    :param trigger_dag_id: the dag_id to trigger (templated)
    :type trigger_dag_id: str
    :param conf: Configuration for the DAG run
    :type conf: dict
    :param execution_date: Execution date for the dag (templated)
    :type execution_date: str or datetime.datetime
    :param reset_dag_run: Whether or not clear existing dag run if already exists.
        This is useful when backfill or rerun an existing dag run.
        When reset_dag_run=False and dag run exists, DagRunAlreadyExists will be raised.
        When reset_dag_run=True and dag run exists, existing dag run will be cleared to rerun.
    :type reset_dag_run: bool
    """

    template_fields = ("trigger_dag_id", "execution_date", "conf")
    ui_color = "#ffefeb"

    @property
    def operator_extra_links(self):
        """Return operator extra links"""
        return [TriggerDagRunLink()]

    @apply_defaults
    def __init__(
        self,
        *,
        trigger_dag_id: str,
        conf: Optional[Dict] = None,
        execution_date: Optional[Union[str, datetime.datetime]] = None,
        reset_dag_run: bool = False,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.trigger_dag_id = trigger_dag_id
        self.conf = conf
        self.reset_dag_run = reset_dag_run

        if not isinstance(execution_date, (str, datetime.datetime, type(None))):
            raise TypeError(
                "Expected str or datetime.datetime type for execution_date."
                "Got {}".format(type(execution_date))
            )

        self.execution_date: Optional[datetime.datetime] = execution_date  # type: ignore

    def execute(self, context: Dict):
        if isinstance(self.execution_date, datetime.datetime):
            execution_date = self.execution_date
        elif isinstance(self.execution_date, str):
            execution_date = timezone.parse(self.execution_date)
            self.execution_date = execution_date
        else:
            execution_date = timezone.utcnow()

        run_id = DagRun.generate_run_id(DagRunType.MANUAL, execution_date)
        try:
            # Ignore MyPy type for self.execution_date
            # because it doesn't pick up the timezone.parse() for strings
            trigger_dag(
                dag_id=self.trigger_dag_id,
                run_id=run_id,
                conf=self.conf,
                execution_date=self.execution_date,
                replace_microseconds=False,
            )

        except DagRunAlreadyExists as e:
            if self.reset_dag_run:
                self.log.info("Clearing %s on %s", self.trigger_dag_id, self.execution_date)

                # Get target dag object and call clear()

                dag_model = DagModel.get_current(self.trigger_dag_id)
                if dag_model is None:
                    raise DagNotFound(f"Dag id {self.trigger_dag_id} not found in DagModel")

                dag_bag = DagBag(
                    dag_folder=dag_model.fileloc,
                    store_serialized_dags=settings.STORE_SERIALIZED_DAGS
                )

                dag = dag_bag.get_dag(self.trigger_dag_id)

                dag.clear(start_date=self.execution_date, end_date=self.execution_date)
            else:
                raise e
