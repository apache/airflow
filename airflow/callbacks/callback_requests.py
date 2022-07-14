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

import json
from typing import TYPE_CHECKING, List, Optional

from google.protobuf.internal.containers import RepeatedCompositeFieldContainer

from airflow.internal_api.grpc import internal_api_pb2
from airflow.internal_api.grpc.internal_api_pb2 import Callback

if TYPE_CHECKING:
    from airflow.models.taskinstance import SimpleTaskInstance


class CallbackRequest:
    """
    Base Class with information about the callback to be executed.

    :param full_filepath: File Path to use to run the callback
    :param msg: Additional Message that can be used for logging
    """

    def __init__(self, full_filepath: str, msg: Optional[str] = None):
        self.full_filepath = full_filepath
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

    def to_protobuf(
        self,
    ) -> Callback:
        raise NotImplementedError()

    @staticmethod
    def get_callbacks_from_protobuf(
        callbacks: RepeatedCompositeFieldContainer[Callback],
    ) -> List["CallbackRequest"]:
        result_callbacks: List[CallbackRequest] = []
        for callback in callbacks:
            type = callback.WhichOneof('callback_type')
            if type == "task_request":
                result_callbacks.append(TaskCallbackRequest.from_protobuf(callback.task_request))
            elif type == "dag_request":
                result_callbacks.append(DagCallbackRequest.from_protobuf(callback.dag_request))
            elif type == 'sla_request':
                result_callbacks.append(SlaCallbackRequest.from_protobuf(callback.sla_request))
            else:
                raise ValueError(f"Bad type: {type}")
        return result_callbacks


class TaskCallbackRequest(CallbackRequest):
    """
    A Class with information about the success/failure TI callback to be executed. Currently, only failure
    callbacks (when tasks are externally killed) and Zombies are run via DagFileProcessorProcess.

    :param full_filepath: File Path to use to run the callback
    :param simple_task_instance: Simplified Task Instance representation
    :param is_failure_callback: Flag to determine whether it is a Failure Callback or Success Callback
    :param msg: Additional Message that can be used for logging to determine failure/zombie
    """

    def __init__(
        self,
        full_filepath: str,
        simple_task_instance: "SimpleTaskInstance",
        is_failure_callback: Optional[bool] = True,
        msg: Optional[str] = None,
    ):
        super().__init__(full_filepath=full_filepath, msg=msg)
        self.simple_task_instance = simple_task_instance
        self.is_failure_callback = is_failure_callback

    def to_json(self) -> str:
        dict_obj = self.__dict__.copy()
        dict_obj["simple_task_instance"] = dict_obj["simple_task_instance"].__dict__
        return json.dumps(dict_obj)

    @classmethod
    def from_json(cls, json_str: str):
        from airflow.models.taskinstance import SimpleTaskInstance

        kwargs = json.loads(json_str)
        simple_ti = SimpleTaskInstance.from_dict(obj_dict=kwargs.pop("simple_task_instance"))
        return cls(simple_task_instance=simple_ti, **kwargs)

    @classmethod
    def from_protobuf(cls, request: internal_api_pb2.TaskCallbackRequest) -> "TaskCallbackRequest":
        from airflow.models.taskinstance import SimpleTaskInstance

        return cls(
            full_filepath=request.full_filepath,
            simple_task_instance=SimpleTaskInstance.from_protobuf(request.task_instance),
            is_failure_callback=request.is_failure_callback,
            msg=request.message,
        )

    def to_protobuf(self) -> Callback:
        return Callback(
            task_request=internal_api_pb2.TaskCallbackRequest(
                full_filepath=self.full_filepath,
                task_instance=self.simple_task_instance.to_protobuf(),
                is_failure_callback=self.is_failure_callback,
                message=self.msg,
            )
        )


class DagCallbackRequest(CallbackRequest):
    """
    A Class with information about the success/failure DAG callback to be executed.

    :param full_filepath: File Path to use to run the callback
    :param dag_id: DAG ID
    :param run_id: Run ID for the DagRun
    :param is_failure_callback: Flag to determine whether it is a Failure Callback or Success Callback
    :param msg: Additional Message that can be used for logging
    """

    def __init__(
        self,
        full_filepath: str,
        dag_id: str,
        run_id: str,
        is_failure_callback: Optional[bool] = True,
        msg: Optional[str] = None,
    ):
        super().__init__(full_filepath=full_filepath, msg=msg)
        self.dag_id = dag_id
        self.run_id = run_id
        self.is_failure_callback = is_failure_callback

    @classmethod
    def from_protobuf(cls, request: internal_api_pb2.DagCallbackRequest) -> "DagCallbackRequest":
        return cls(
            full_filepath=request.full_filepath,
            dag_id=request.dag_id,
            run_id=request.run_id,
            is_failure_callback=request.is_failure_callback,
            msg=request.message,
        )

    def to_protobuf(self) -> Callback:
        return Callback(
            dag_request=internal_api_pb2.DagCallbackRequest(
                full_filepath=self.full_filepath,
                dag_id=self.dag_id,
                run_id=self.run_id,
                is_failure_callback=self.is_failure_callback,
                message=self.msg,
            )
        )


class SlaCallbackRequest(CallbackRequest):
    """
    A class with information about the SLA callback to be executed.

    :param full_filepath: File Path to use to run the callback
    :param dag_id: DAG ID
    :param msg: Additional Message that can be used for logging
    """

    def __init__(self, full_filepath: str, dag_id: str, msg: Optional[str] = None):
        super().__init__(full_filepath, msg)
        self.dag_id = dag_id

    @classmethod
    def from_protobuf(cls, request: internal_api_pb2.SlaCallbackRequest) -> "SlaCallbackRequest":
        return cls(full_filepath=request.full_filepath, dag_id=request.dag_id, msg=request.message)

    def to_protobuf(self) -> Callback:
        return Callback(
            sla_request=internal_api_pb2.SlaCallbackRequest(
                full_filepath=self.full_filepath,
                dag_id=self.dag_id,
                message=self.msg,
            )
        )
