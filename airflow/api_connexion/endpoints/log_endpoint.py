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

from flask import Response, current_app, request
from itsdangerous.exc import BadSignature
from itsdangerous.url_safe import URLSafeSerializer

from airflow.api_connexion import security
from airflow.api_connexion.exceptions import BadRequest, NotFound
from airflow.api_connexion.schemas.log_schema import LogResponseObject, logs_schema
from airflow.models import DagRun
from airflow.utils.log.log_reader import TaskLogReader
from airflow.utils.session import provide_session


@security.requires_access([('can_read', 'Dag'), ('can_read', 'DagRun'), ('can_read', 'Task')])
@provide_session
def get_log(session, dag_id, dag_run_id, task_id, task_try_number, full_content=False, token=None):
    """
    Get logs for specific task instance
    """
    key = current_app.config["SECRET_KEY"]
    if not token:
        metadata = {}
    else:
        try:
            metadata = URLSafeSerializer(key).loads(token)
        except BadSignature:
            raise BadRequest("Bad Signature. Please use only the tokens provided by the API.")

    if metadata.get('download_logs') and metadata['download_logs']:
        full_content = True

    if full_content:
        metadata['download_logs'] = True
    else:
        metadata['download_logs'] = False

    task_log_reader = TaskLogReader()
    if not task_log_reader.supports_read:
        raise BadRequest("Task log handler does not support read logs.")

    query = session.query(DagRun).filter(DagRun.dag_id == dag_id)
    dag_run = query.filter(DagRun.run_id == dag_run_id).first()
    if not dag_run:
        raise NotFound("DAG Run not found")

    ti = dag_run.get_task_instance(task_id, session)
    if ti is None:
        metadata['end_of_log'] = True
        raise BadRequest(detail="Task instance did not exist in the DB")

    dag = current_app.dag_bag.get_dag(dag_id)
    if dag:
        ti.task = dag.get_task(ti.task_id)

    return_type = request.accept_mimetypes.best_match(['text/plain', 'application/json'])

    # return_type would be either the above two or None

    if return_type == 'application/json' or return_type is None:  # default
        logs, metadata = task_log_reader.read_log_chunks(ti, task_try_number, metadata)
        logs = logs[0] if task_try_number is not None else logs
        token = URLSafeSerializer(key).dumps(metadata)
        return logs_schema.dump(LogResponseObject(continuation_token=token, content=logs))
    # text/plain. Stream
    logs = task_log_reader.read_log_stream(ti, task_try_number, metadata)

    return Response(logs, headers={"Content-Type": return_type})
