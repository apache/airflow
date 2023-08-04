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
import secrets
import string
from typing import TYPE_CHECKING

import pendulum
from slugify import slugify

from airflow.compat.functools import cache
from airflow.configuration import conf

if TYPE_CHECKING:
    from airflow.models.taskinstancekey import TaskInstanceKey

log = logging.getLogger(__name__)

alphanum_lower = string.ascii_lowercase + string.digits


def rand_str(num):
    """Generate random lowercase alphanumeric string of length num.

    :meta private:
    """
    return "".join(secrets.choice(alphanum_lower) for _ in range(num))


def add_pod_suffix(*, pod_name: str, rand_len: int = 8, max_len: int = 80) -> str:
    """Add random string to pod name while staying under max length.

    :param pod_name: name of the pod
    :param rand_len: length of the random string to append
    :param max_len: maximum length of the pod name
    :meta private:
    """
    suffix = "-" + rand_str(rand_len)
    return pod_name[: max_len - len(suffix)].strip("-.") + suffix


def create_pod_id(
    dag_id: str | None = None,
    task_id: str | None = None,
    *,
    max_length: int = 80,
    unique: bool = True,
) -> str:
    """
    Generates unique pod ID given a dag_id and / or task_id.

    The default of 80 for max length is somewhat arbitrary, mainly a balance between
    content and not overwhelming terminal windows of reasonable width. The true
    upper limit is 253, and this is enforced in construct_pod.

    :param dag_id: DAG ID
    :param task_id: Task ID
    :param max_length: max number of characters
    :param unique: whether a random string suffix should be added
    :return: A valid identifier for a kubernetes pod name
    """
    if not (dag_id or task_id):
        raise ValueError("Must supply either dag_id or task_id.")
    name = ""
    if dag_id:
        name += dag_id
    if task_id:
        if name:
            name += "-"
        name += task_id
    base_name = slugify(name, lowercase=True)[:max_length].strip(".-")
    if unique:
        return add_pod_suffix(pod_name=base_name, rand_len=8, max_len=max_length)
    else:
        return base_name


def annotations_to_key(annotations: dict[str, str]) -> TaskInstanceKey:
    """Build a TaskInstanceKey based on pod annotations."""
    log.debug("Creating task key for annotations %s", annotations)
    dag_id = annotations["dag_id"]
    task_id = annotations["task_id"]
    try_number = int(annotations["try_number"])
    annotation_run_id = annotations.get("run_id")
    map_index = int(annotations.get("map_index", -1))

    # Compat: Look up the run_id from the TI table!
    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance, TaskInstanceKey
    from airflow.settings import Session

    if not annotation_run_id and "execution_date" in annotations:
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


@cache
def get_logs_task_metadata() -> bool:
    return conf.getboolean("kubernetes_executor", "logs_task_metadata")


def annotations_for_logging_task_metadata(annotation_set):
    if get_logs_task_metadata():
        annotations_for_logging = annotation_set
    else:
        annotations_for_logging = "<omitted>"
    return annotations_for_logging
