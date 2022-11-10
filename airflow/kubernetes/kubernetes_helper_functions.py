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
from __future__ import annotations

import logging

import pendulum
from slugify import slugify

from airflow.models.taskinstance import TaskInstanceKey

log = logging.getLogger(__name__)


def _strip_unsafe_kubernetes_special_chars(string: str) -> str:
    """
    Kubernetes only supports lowercase alphanumeric characters, "-" and "." in
    the pod name.
    However, there are special rules about how "-" and "." can be used so let's
    only keep
    alphanumeric chars  see here for detail:
    https://kubernetes.io/docs/concepts/overview/working-with-objects/names/

    :param string: The requested Pod name
    :return: Pod name stripped of any unsafe characters
    """
    return slugify(string, separator="", lowercase=True)


def create_pod_id(dag_id: str, task_id: str) -> str:
    """
    Generates the kubernetes safe pod_id. Note that this is
    NOT the full ID that will be launched to k8s. We will add a uuid
    to ensure uniqueness.

    :param dag_id: DAG ID
    :param task_id: Task ID
    :return: The non-unique pod_id for this task/DAG pairing
    """
    safe_dag_id = _strip_unsafe_kubernetes_special_chars(dag_id)
    safe_task_id = _strip_unsafe_kubernetes_special_chars(task_id)
    return safe_dag_id + safe_task_id


def annotations_to_key(annotations: dict[str, str]) -> TaskInstanceKey:
    """Build a TaskInstanceKey based on pod annotations"""
    log.debug("Creating task key for annotations %s", annotations)
    dag_id = annotations["dag_id"]
    task_id = annotations["task_id"]
    try_number = int(annotations["try_number"])
    annotation_run_id = annotations.get("run_id")
    map_index = int(annotations.get("map_index", -1))

    if not annotation_run_id and "execution_date" in annotations:
        # Compat: Look up the run_id from the TI table!
        from airflow.models.dagrun import DagRun
        from airflow.models.taskinstance import TaskInstance
        from airflow.settings import Session

        execution_date = pendulum.parse(annotations["execution_date"])
        # Do _not_ use create-session, we don't want to expunge
        session = Session()

        task_instance_run_id = (
            session.query(TaskInstance.run_id)
            .join(TaskInstance.dag_run)
            .filter(
                TaskInstance.dag_id == dag_id,
                TaskInstance.task_id == task_id,
                DagRun.execution_date == execution_date,
            )
            .scalar()
        )
    else:
        task_instance_run_id = annotation_run_id

    return TaskInstanceKey(
        dag_id=dag_id,
        task_id=task_id,
        run_id=task_instance_run_id,
        try_number=try_number,
        map_index=map_index,
    )
