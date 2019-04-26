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

from airflow.exceptions import (DagNotFound, TaskNotFound,
                                DagRunNotFound, TaskInstanceNotFound)
from airflow.models import DagBag
from airflow.models import TaskInstance
from airflow.utils import timezone
import logging
from io import BytesIO
from airflow import configuration as conf
from airflow.utils.helpers import render_log_filename
from flask import send_file


def get_task_logs(dag_id, task_id, execution_date):
    """Return the logs for the task instance identified by the given dag_id, execution_date and task_id."""

    execution_date = timezone.parse(execution_date)

    dagbag = DagBag()

    # Check DAG exists.
    if dag_id not in dagbag.dags:
        error_message = "Dag id {} not found".format(dag_id)
        raise DagNotFound(error_message)

    # Get DAG object and check Task Exists
    dag = dagbag.get_dag(dag_id)
    if not dag.has_task(task_id):
        error_message = 'Task {} not found in dag {}'.format(task_id, dag_id)
        raise TaskNotFound(error_message)

    # Get DagRun object and check that it exists
    dagrun = dag.get_dagrun(execution_date=execution_date)
    if not dagrun:
        error_message = ('Dag Run for date {} not found in dag {}'
                         .format(execution_date, dag_id))
        raise DagRunNotFound(error_message)

    # Get task instance object and check that it exists
    task_instance = dagrun.get_task_instance(task_id)
    if not task_instance:
        error_message = ('Task {} instance for date {} not found'
                         .format(task_id, execution_date))
        raise TaskInstanceNotFound(error_message)

    ti = TaskInstance.find(dag_id=dag_id, task_id=task_id, execution_date=execution_date)[0]

    try_number = ti._try_number
    metadata = {}

    logger = logging.getLogger('airflow.task')
    task_log_reader = conf.get('core', 'task_log_reader')
    handler = next((handler for handler in logger.handlers
                    if handler.name == task_log_reader), None)

    if ti is None:
        logs = ["*** Task instance did not exist in the DB\n"]
        metadata['end_of_log'] = True
    else:
        dag = dagbag.get_dag(dag_id)
        ti.task = dag.get_task(ti.task_id)
        logs, metadatas = handler.read(ti, try_number, metadata=metadata)
        metadata = metadatas[0]

    file_obj = BytesIO(b'\n'.join(
        log.encode('utf-8') for log in logs
    ))
    filename_template = conf.get('core', 'LOG_FILENAME_TEMPLATE')
    attachment_filename = render_log_filename(
        ti=ti,
        try_number="all" if try_number is None else try_number,
        filename_template=filename_template)
    return send_file(file_obj, as_attachment=True, attachment_filename=attachment_filename)
