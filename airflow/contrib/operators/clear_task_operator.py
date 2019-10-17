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
This module contains ClearTaskOperator
"""

import datetime
import os
from typing import Optional, Union

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, DagBag, DagModel
from airflow.utils import timezone
from airflow.utils.db import provide_session


class ClearTaskOperator(BaseOperator):
    """
    Clears a given external task on a different DAG for a specific execution_date. Optionally, the downstream
    tasks and future instances of the given task can be cleared too.

    This operator makes it possible to re-run completed tasks based on some conditions from within a DAG.
    This is useful in situations where completed tasks and their downstream tasks need to be re-run due to
    changing conditions.

    :param external_task_id: The task_id of the external task you want to clear
    :param external_dag_id: The dag_id of the DAG that has the external task you want to clear.
        The default is the current dag.
    :param execution_date: The execution_date of the external task that you want to clear
    :param downstream: Whether downstream tasks of the external task should be cleared. Default is True
    :param future: Whether future execution_date of the same external task should be cleared. Default is False
    """

    template_fields = {"external_task_id", "external_dag_id", "execution_date"}

    def __init__(
        self,
        external_task_id: str,
        *args,
        external_dag_id: str = "{{ dag.dag_id }}",
        execution_date: Optional[Union[str, datetime.datetime]] = "{{ ds_nodash }}",
        downstream: bool = True,
        future: bool = False,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.external_dag_id = external_dag_id
        self.external_task_id = external_task_id
        self.downstream = downstream
        self.future = future

        if isinstance(execution_date, datetime.datetime):
            self.execution_date = execution_date.isoformat()
        elif isinstance(execution_date, str):
            self.execution_date = execution_date
        else:
            raise TypeError(
                'Expected str or datetime.datetime type '
                'for execution_date. Got {}'.format(
                    type(execution_date)))

    @provide_session
    def execute(self, context, session=None):  # pylint: disable=unused-argument
        execution_date = timezone.parse(self.execution_date)

        external_dag_model = (
            session.query(DagModel).filter(DagModel.dag_id == self.external_dag_id).first()
        )

        if not external_dag_model:
            raise AirflowException('The external DAG {} does not exist.'.format(self.external_dag_id))

        if not os.path.exists(external_dag_model.fileloc):
            raise AirflowException("The external DAG {} was deleted".format(self.external_dag_id))

        external_dag = DagBag(external_dag_model.fileloc).get_dag(self.external_dag_id)
        to_clear = external_dag.sub_dag(
            task_regex="^{}$".format(self.external_task_id),
            include_downstream=self.downstream,
            include_upstream=False,
        )

        count = to_clear.clear(
            start_date=execution_date,
            end_date=None
            if self.future
            else execution_date,  # Set to None to clear all future runs.
        )

        self.log.info("Cleared %s tasks", count)
