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

from pydantic import BaseModel

from airflow.api_fastapi.execution_api.datamodels import taskinstance as ti_datamodel  # noqa: TC001
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from airflow.typing_compat import Self


class CallbackRequest(BaseModel):
    """
    Base Class with information about the callback to be executed.

    :param msg: Additional Message that can be used for logging
    :param processor_subdir: Directory used by Dag Processor when parsed the dag.
    """

    full_filepath: str
    """File Path to use to run the callback"""
    processor_subdir: str | None = None
    """Directory used by Dag Processor when parsed the dag"""
    msg: str | None = None
    """Additional Message that can be used for logging to determine failure/zombie"""

    @classmethod
    def from_json(cls, data: str | bytes | bytearray) -> Self:
        return cls.model_validate_json(data)

    def to_json(self, **kwargs) -> str:
        return self.model_dump_json(**kwargs)


class TaskCallbackRequest(CallbackRequest):
    """
    Task callback status information.

    A Class with information about the success/failure TI callback to be executed. Currently, only failure
    callbacks (when tasks are externally killed) and Zombies are run via DagFileProcessorProcess.
    """

    ti: ti_datamodel.TaskInstance
    """Simplified Task Instance representation"""
    task_callback_type: TaskInstanceState | None = None
    """Whether on success, on failure, on retry"""

    @property
    def is_failure_callback(self) -> bool:
        """Returns True if the callback is a failure callback."""
        if self.task_callback_type is None:
            return True
        return self.task_callback_type in {
            TaskInstanceState.FAILED,
            TaskInstanceState.UP_FOR_RETRY,
            TaskInstanceState.UPSTREAM_FAILED,
        }


class DagCallbackRequest(CallbackRequest):
    """A Class with information about the success/failure DAG callback to be executed."""

    dag_id: str
    run_id: str
    is_failure_callback: bool | None = True
    """Flag to determine whether it is a Failure Callback or Success Callback"""
