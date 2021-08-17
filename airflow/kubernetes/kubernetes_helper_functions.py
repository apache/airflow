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
from typing import Dict, Optional

from dateutil import parser

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
    return ''.join(ch.lower() for ch in list(string) if ch.isalnum())


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


def annotations_to_key(annotations: Dict[str, str]) -> Optional[TaskInstanceKey]:
    """Build a TaskInstanceKey based on pod annotations"""
    log.debug("Creating task key for annotations %s", annotations)
    dag_id = annotations['dag_id']
    task_id = annotations['task_id']
    try_number = int(annotations['try_number'])
    execution_date = parser.parse(annotations['execution_date'])

    return TaskInstanceKey(dag_id, task_id, execution_date, try_number)
