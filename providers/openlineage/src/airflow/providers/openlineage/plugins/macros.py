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

from typing import TYPE_CHECKING

from airflow.providers.openlineage import conf
from airflow.providers.openlineage.plugins.adapter import OpenLineageAdapter
from airflow.providers.openlineage.utils.utils import (
    get_job_name,
    get_parent_information_from_dagrun_conf,
    get_root_information_from_dagrun_conf,
)
from airflow.providers.openlineage.version_compat import AIRFLOW_V_3_0_PLUS

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import TaskInstance


def lineage_job_namespace():
    """
    Macro function which returns Airflow OpenLineage namespace.

    .. seealso::
        For more information take a look at the guide:
        :ref:`howto/macros:openlineage`
    """
    return conf.namespace()


def lineage_job_name(task_instance: TaskInstance):
    """
    Macro function which returns Airflow task name in OpenLineage format (`<dag_id>.<task_id>`).

    .. seealso::
        For more information take a look at the guide:
        :ref:`howto/macros:openlineage`
    """
    return get_job_name(task_instance)


def lineage_run_id(task_instance: TaskInstance):
    """
    Macro function which returns the generated run id (UUID) for a given task.

    This can be used to forward the run id from a task to a child run so the job hierarchy is preserved.

    .. seealso::
        For more information take a look at the guide:
        :ref:`howto/macros:openlineage`
    """
    return OpenLineageAdapter.build_task_instance_run_id(
        dag_id=task_instance.dag_id,
        task_id=task_instance.task_id,
        try_number=task_instance.try_number,
        logical_date=_get_logical_date(task_instance),
        map_index=task_instance.map_index,
    )


def lineage_parent_id(task_instance: TaskInstance):
    """
    Macro function which returns a unique identifier of given task that can be used to create ParentRunFacet.

    This identifier is composed of the namespace, job name, and generated run id for given task, structured
    as '{namespace}/{job_name}/{run_id}'. This can be used to forward task information from a task to a child
    run so the job hierarchy is preserved. Child run can easily create ParentRunFacet from these information.

    .. seealso::
        For more information take a look at the guide:
        :ref:`howto/macros:openlineage`
    """
    return "/".join(
        (
            lineage_job_namespace(),
            lineage_job_name(task_instance),
            lineage_run_id(task_instance),
        )
    )


def lineage_root_parent_id(task_instance: TaskInstance):
    """
    Macro function which returns a unique identifier of given task that can be used to create root information for ParentRunFacet.

    This identifier is composed of the namespace, dag name, and generated run id for given dag, structured
    as '{namespace}/{job_name}/{run_id}'.

    .. seealso::
        For more information take a look at the guide:
        :ref:`howto/macros:openlineage`
    """
    return "/".join(
        (
            lineage_root_job_namespace(task_instance),
            lineage_root_job_name(task_instance),
            lineage_root_run_id(task_instance),
        )
    )


def lineage_root_job_name(task_instance: TaskInstance):
    root_parent_job_name = _get_ol_root_id("root_parent_job_name", task_instance)
    if root_parent_job_name:
        return root_parent_job_name
    return task_instance.dag_id


def lineage_root_run_id(task_instance: TaskInstance):
    root_parent_run_id = _get_ol_root_id("root_parent_run_id", task_instance)
    if root_parent_run_id:
        return root_parent_run_id
    return OpenLineageAdapter.build_dag_run_id(
        dag_id=task_instance.dag_id,
        logical_date=_get_logical_date(task_instance),
        clear_number=_get_dag_run_clear_number(task_instance),
    )


def lineage_root_job_namespace(task_instance: TaskInstance):
    root_parent_job_namespace = _get_ol_root_id("root_parent_job_namespace", task_instance)
    if root_parent_job_namespace:
        return root_parent_job_namespace
    return conf.namespace()


def _get_ol_root_id(id_key: str, task_instance: TaskInstance) -> str | None:
    dr_conf = _get_dag_run_conf(task_instance=task_instance)
    # Check DagRun conf for root info
    ol_root_info = get_root_information_from_dagrun_conf(dr_conf=dr_conf)
    if ol_root_info and ol_root_info.get(id_key):
        return ol_root_info[id_key]
    # Then check DagRun conf for parent into that is used as root in case explicit root is missing
    id_key = id_key.replace("root_", "")
    ol_root_info = get_parent_information_from_dagrun_conf(dr_conf=dr_conf)
    if ol_root_info and ol_root_info.get(id_key):
        return ol_root_info[id_key]
    return None


def _get_dagrun_from_ti(task_instance: TaskInstance):
    context = task_instance.get_template_context()
    if getattr(task_instance, "dag_run", None):
        return task_instance.dag_run
    return context["dag_run"]


def _get_dag_run_conf(task_instance: TaskInstance) -> dict:
    dr = _get_dagrun_from_ti(task_instance=task_instance)
    return dr.conf or {}


def _get_dag_run_clear_number(task_instance: TaskInstance):
    dr = _get_dagrun_from_ti(task_instance=task_instance)
    return dr.clear_number


def _get_logical_date(task_instance):
    if AIRFLOW_V_3_0_PLUS:
        dr = _get_dagrun_from_ti(task_instance=task_instance)
        if getattr(dr, "logical_date", None):
            return dr.logical_date
        return dr.run_after
    if getattr(task_instance, "logical_date", None):
        return task_instance.logical_date
    return task_instance.execution_date
