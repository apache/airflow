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

from airflow.models import DagBag
import logging
from io import BytesIO
from airflow import configuration as conf
from airflow.utils.helpers import render_log_filename
from flask import send_file
from airflow.api.common.experimental.task_instance import get_task_instance


def get_task_logs(dag_id, task_id, execution_date):
    """Return the logs for the task instance identified by the given dag_id, execution_date and task_id."""

    dagbag = DagBag()
    ti = get_task_instance(dag_id, task_id, execution_date)

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
