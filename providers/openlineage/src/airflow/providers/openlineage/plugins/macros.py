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
from airflow.providers.openlineage.utils.utils import get_job_name
from airflow.providers.openlineage.version_compat import AIRFLOW_V_3_0_PLUS

if TYPE_CHECKING:
    from airflow.models import TaskInstance


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
            lineage_job_namespace(),
            lineage_root_job_name(task_instance),
            lineage_root_run_id(task_instance),
        )
    )


def lineage_root_job_name(task_instance: TaskInstance):
    return task_instance.dag_id


def lineage_root_run_id(task_instance: TaskInstance):
    return OpenLineageAdapter.build_dag_run_id(
        dag_id=task_instance.dag_id,
        logical_date=_get_logical_date(task_instance),
        clear_number=_get_dag_run_clear_number(task_instance),
    )


def _get_dag_run_clear_number(task_instance: TaskInstance):
    # todo: remove when min airflow version >= 3.0
    if AIRFLOW_V_3_0_PLUS:
        context = task_instance.get_template_context()
        if hasattr(task_instance, "dag_run"):
            dag_run = task_instance.dag_run
        else:
            dag_run = context["dag_run"]
        return dag_run.clear_number
    return task_instance.dag_run.clear_number


def _get_logical_date(task_instance):
    # todo: remove when min airflow version >= 3.0
    if AIRFLOW_V_3_0_PLUS:
        context = task_instance.get_template_context()
        if hasattr(task_instance, "dag_run"):
            dag_run = task_instance.dag_run
        else:
            dag_run = context["dag_run"]
        if hasattr(dag_run, "logical_date") and dag_run.logical_date:
            date = dag_run.logical_date
        else:
            date = dag_run.run_after
    elif hasattr(task_instance, "logical_date"):
        date = task_instance.logical_date
    else:
        date = task_instance.execution_date
    return date
