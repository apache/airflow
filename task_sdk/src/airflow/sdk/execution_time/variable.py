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

import structlog

from airflow.sdk.exceptions import AirflowRuntimeError

if TYPE_CHECKING:
    from airflow.sdk.definitions.variable import Variable
    from airflow.sdk.execution_time.comms import VariableResult


def _convert_variable_result_to_variable(var_result: VariableResult, deserialize_json: bool) -> Variable:
    from airflow.sdk.definitions.variable import Variable

    if deserialize_json:
        import json

        var_result.value = json.loads(var_result.value)  # type: ignore
    return Variable(**var_result.model_dump(exclude={"type"}))


def _get_variable(key: str, deserialize_json: bool = False) -> Variable:
    from airflow.sdk.execution_time.comms import ErrorResponse, GetVariable

    try:
        # We check the hypothesis if the request for variable came from task.
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS as COMMS  # type: ignore
    except ImportError:
        # If not, hypothesis is false and this request is from dag level.
        from airflow.dag_processing.processor import COMMS_DECODER as COMMS  # type: ignore

    log = structlog.get_logger(logger_name="task")
    COMMS.send_request(log=log, msg=GetVariable(key=key))
    msg = COMMS.get_message()
    if isinstance(msg, ErrorResponse):
        raise AirflowRuntimeError(msg)

    if TYPE_CHECKING:
        assert isinstance(msg, VariableResult)
    return _convert_variable_result_to_variable(msg, deserialize_json)
