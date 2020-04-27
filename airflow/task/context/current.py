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

import contextlib

from airflow.exceptions import AirflowException
from airflow.task.context.execution_context import ExecutionContext

_CURRENT_CONTEXT = []


@contextlib.contextmanager
def set_current_context(context):
    """
    Sets the current execution context to the provided context object.
    This method should be called once per execution, before calling operator.execute
    """
    _CURRENT_CONTEXT.append(context)
    try:
        yield context
    finally:
        stack_state = _CURRENT_CONTEXT.pop()
        if stack_state != context:
            print("TODO WARNING")


def get_current_context() -> ExecutionContext:
    """
    Obtain the execution context for the currently executing operator without
    altering user method's signature.
    This is the simplest method of retrieving the execution context dictionary.
    ** Old style:
        def my_task(**context):
            ti = context["ti"]
    ** New style:
        def my_task():
            from airflow.task.context import get_current_context
            context = get_current_context()
            ti = context["ti"]

    Current context will only have value if this method was called after an operator
    was starting to execute.
    """
    if not _CURRENT_CONTEXT:
        raise AirflowException("Current context was requested but no context was found! "
                               "Are you running within an airflow task?")
    return _CURRENT_CONTEXT[-1]
