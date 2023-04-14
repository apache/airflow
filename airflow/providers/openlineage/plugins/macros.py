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

import os
import typing

from airflow.providers.openlineage.plugins.adapter import OpenLineageAdapter

if typing.TYPE_CHECKING:
    from airflow.models import TaskInstance

_JOB_NAMESPACE = os.getenv("OPENLINEAGE_NAMESPACE", "default")


def lineage_run_id(task_instance: TaskInstance):
    """
    Macro function which returns the generated run id for a given task. This
    can be used to forward the run id from a task to a child run so the job
    hierarchy is preserved.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/macros:openlineage`
    """
    return OpenLineageAdapter.build_task_instance_run_id(
        task_instance.task.task_id, task_instance.execution_date, task_instance.try_number
    )


def lineage_parent_id(run_id: str, task_instance: TaskInstance):
    """
    Macro function which returns the generated job and run id for a given task. This
    can be used to forward the ids from a task to a child run so the job
    hierarchy is preserved. Child run can create ParentRunFacet from those ids.

    .. seealso::
        For more information on how to use this macro, take a look at the guide:
        :ref:`howto/macros:openlineage`
    """
    job_name = OpenLineageAdapter.build_task_instance_run_id(
        task_instance.task.task_id, task_instance.execution_date, task_instance.try_number
    )
    return f"{_JOB_NAMESPACE}/{job_name}/{run_id}"
