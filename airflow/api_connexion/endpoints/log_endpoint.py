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
import logging
import os
from io import BytesIO

from flask import request, send_file

from airflow import models, settings
from airflow.api_connexion.exceptions import NotFound
from airflow.configuration import conf
from airflow.models import DagRun
from airflow.settings import STORE_SERIALIZED_DAGS
from airflow.utils.helpers import render_log_filename
from airflow.utils.session import provide_session

if os.environ.get('SKIP_DAGS_PARSING') != 'True':
    dagbag = models.DagBag(settings.DAGS_FOLDER, store_serialized_dags=STORE_SERIALIZED_DAGS)
else:
    dagbag = models.DagBag(os.devnull, include_examples=False)


@provide_session
def get_log(session, dag_id, dag_run_id, task_id, task_try_number,
            full_content=False, token=None):
    """
    Get logs for specific task instance
    """
    if not token:
        token = {}
    # Todo use itsdangerous to decrypt metadata

    metadata = token
    if metadata.get('download_logs', None) and not full_content:
        full_content = True
    if full_content:
        metadata['download_logs'] = True
    query = session.query(DagRun).filter(DagRun.dag_id == dag_id)
    dag_run = query.filter(DagRun.run_id == dag_run_id).first()
    if not dag_run:
        raise NotFound("Specified DagRun not found")

    logger = logging.getLogger('airflow.task')
    task_log_reader = conf.get('logging', 'task_log_reader')
    handler = next((handler for handler in logger.handlers
                    if handler.name == task_log_reader), None)

    ti = dag_run.get_task_instance(task_id, session)
    try:
        if ti is None:
            logs = ["*** Task instance did not exist in the DB\n"]
            metadata['end_of_log'] = True
        else:
            dag = dagbag.get_dag(dag_id)
            ti.task = dag.get_task(ti.task_id)
            logs, metadatas = handler.read(ti, task_try_number, metadata=metadata)
            metadata = metadatas[0]

        if request.headers['Content-Type'] == 'application/json':
            return dict(continuation_token=str(metadata),
                        content=str(logs))

        file_obj = BytesIO(b'\n'.join(
            log.encode('utf-8') for log in logs
        ))
        filename_template = conf.get('core', 'LOG_FILENAME_TEMPLATE')
        attachment_filename = render_log_filename(
            ti=ti,
            try_number="all" if task_try_number is None else task_try_number,
            filename_template=filename_template)
        return send_file(file_obj, as_attachment=True,
                         attachment_filename=attachment_filename)

    except AttributeError as err:
        error_message = ["Task log handler {} does not support read logs.\n{}\n"
                         .format(task_log_reader, str(err))]
        metadata['end_of_log'] = True
        return dict(continuation_token=str(metadata),
                    content=str(error_message))
