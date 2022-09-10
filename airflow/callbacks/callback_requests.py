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

import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.models.taskinstance import SimpleTaskInstance


class CallbackRequest:
    """
    Base Class with information about the callback to be executed.

    :param full_filepath: File Path to use to run the callback
    :param msg: Additional Message that can be used for logging
    :param processor_subdir: Directory used by Dag Processor when parsed the dag.
    """

    def __init__(
        self,
        full_filepath: str,
        processor_subdir: str | None = None,
        msg: str | None = None,
    ):
        self.full_filepath = full_filepath
        self.processor_subdir = processor_subdir
        self.msg = msg

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        return NotImplemented

    def __repr__(self):
        return str(self.__dict__)

    def to_json(self) -> str:
        return json.dumps(self.__dict__)

    @classmethod
    def from_json(cls, json_str: str):
        json_object = json.loads(json_str)
        return cls(**json_object)


class TaskCallbackRequest(CallbackRequest):
    """
    A Class with information about the success/failure TI callback to be executed. Currently, only failure
    callbacks (when tasks are externally killed) and Zombies are run via DagFileProcessorProcess.

    :param full_filepath: File Path to use to run the callback
    :param simple_task_instance: Simplified Task Instance representation
    :param is_failure_callback: Flag to determine whether it is a Failure Callback or Success Callback
    :param msg: Additional Message that can be used for logging to determine failure/zombie
    :param processor_subdir: Directory used by Dag Processor when parsed the dag.
    """

    def __init__(
        self,
        full_filepath: str,
        simple_task_instance: SimpleTaskInstance,
        is_failure_callback: bool | None = True,
        processor_subdir: str | None = None,
        msg: str | None = None,
    ):
        super().__init__(full_filepath=full_filepath, processor_subdir=processor_subdir, msg=msg)
        self.simple_task_instance = simple_task_instance
        self.is_failure_callback = is_failure_callback

    def to_json(self) -> str:
        dict_obj = self.__dict__.copy()
        dict_obj["simple_task_instance"] = self.simple_task_instance.as_dict()
        return json.dumps(dict_obj)

    @classmethod
    def from_json(cls, json_str: str):
        from airflow.models.taskinstance import SimpleTaskInstance

        kwargs = json.loads(json_str)
        simple_ti = SimpleTaskInstance.from_dict(obj_dict=kwargs.pop("simple_task_instance"))
        return cls(simple_task_instance=simple_ti, **kwargs)


class DagCallbackRequest(CallbackRequest):
    """
    A Class with information about the success/failure DAG callback to be executed.

    :param full_filepath: File Path to use to run the callback
    :param dag_id: DAG ID
    :param run_id: Run ID for the DagRun
    :param processor_subdir: Directory used by Dag Processor when parsed the dag.
    :param is_failure_callback: Flag to determine whether it is a Failure Callback or Success Callback
    :param msg: Additional Message that can be used for logging
    """

    def __init__(
        self,
        full_filepath: str,
        dag_id: str,
        run_id: str,
        processor_subdir: str | None,
        is_failure_callback: bool | None = True,
        msg: str | None = None,
    ):
        super().__init__(full_filepath=full_filepath, processor_subdir=processor_subdir, msg=msg)
        self.dag_id = dag_id
        self.run_id = run_id
        self.is_failure_callback = is_failure_callback


class SlaCallbackRequest(CallbackRequest):
    """
    A class with information about the SLA callback to be executed.

    :param full_filepath: File Path to use to run the callback
    :param dag_id: DAG ID
    :param processor_subdir: Directory used by Dag Processor when parsed the dag.
    """

    def __init__(
        self,
        full_filepath: str,
        dag_id: str,
        processor_subdir: str | None,
        msg: str | None = None,
    ):
        super().__init__(full_filepath, processor_subdir=processor_subdir, msg=msg)
        self.dag_id = dag_id
