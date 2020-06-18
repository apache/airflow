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

import os

from flask import Response, current_app, request
from itsdangerous.exc import BadSignature
from itsdangerous.url_safe import URLSafeSerializer

from airflow import models, settings
from airflow.api_connexion.exceptions import BadRequest, NotFound
from airflow.api_connexion.schemas.log_schema import logs_schema
from airflow.models import DagRun
from airflow.settings import STORE_SERIALIZED_DAGS
from airflow.utils.log.log_reader import TaskLogReader
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
        metadata = {}
    else:
        key = current_app.config["SECRET_KEY"]
        try:
            metadata = URLSafeSerializer(key).loads(token)
        except BadSignature:
            raise BadRequest("Bad Signature. Please sign your token with URLSafeSerializer")

    if metadata.get('download_logs', None) and metadata['download_logs']:
        full_content = True

    if full_content:
        metadata['download_logs'] = True
    else:
        metadata['download_logs'] = False

    task_log_reader = TaskLogReader()

    if not task_log_reader.is_supported:
        metadata = {"end_of_log": True}
        return logs_schema.dump(dict(
            content="[Task log handler does not support read logs.]",
            continuation_token=str(metadata)
        ))

    query = session.query(DagRun).filter(DagRun.dag_id == dag_id)
    dag_run = query.filter(DagRun.run_id == dag_run_id).first()
    if not dag_run:
        raise NotFound("Specified DagRun not found")

    ti = dag_run.get_task_instance(task_id, session)
    if ti is None:
        metadata['end_of_log'] = True
        return logs_schema.dump(
            dict(
                content=str("[*** Task instance did not exist in the DB\n]"),
                continuation_token=str(metadata))
        )
    try:
        dag = dagbag.get_dag(dag_id)
        if dag:
            ti.task = dag.get_task(ti.task_id)

        if request.headers.get('Content-Type', None) == 'application/json':
            logs, metadata = task_log_reader.read_log_chunks(ti, task_try_number, metadata)
            logs = logs[0] if task_try_number is not None else logs

            return logs_schema.dump(dict(continuation_token=str(metadata),
                                         content=logs)
                                    )

        # Defaulting to content type of text/plain
        logs = task_log_reader.read_log_stream(ti, task_try_number, metadata)

        attachment_filename = task_log_reader.render_log_filename(ti, task_try_number)

        return Response(
            logs,
            mimetype="text/plain",
            headers={
                "Content-Disposition": f"attachment; filename={attachment_filename}"
            })

    except AttributeError as err:
        error_message = [
            f"Task log handler {task_log_reader} does not support read logs.\n{str(err)}\n"
        ]
        metadata['end_of_log'] = True
        return logs_schema.dump(
            dict(continuation_token=str(metadata),
                 content=str(error_message))
        )
