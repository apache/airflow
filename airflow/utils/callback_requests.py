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

from datetime import datetime
from typing import Optional

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
        if isinstance(other, CallbackRequest):
            return self.__dict__ == other.__dict__
        return False

    def __repr__(self):
        return str(self.__dict__)


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
        simple_task_instance: SimpleTaskInstance,
        is_failure_callback: Optional[bool] = True,
        msg: Optional[str] = None,
    ):
        super().__init__(full_filepath=full_filepath, msg=msg)
        self.simple_task_instance = simple_task_instance
        self.is_failure_callback = is_failure_callback


class DagCallbackRequest(CallbackRequest):
    """
    A Class with information about the success/failure DAG callback to be executed.

    :param full_filepath: File Path to use to run the callback
    :param dag_id: DAG ID
    :param execution_date: Execution Date for the DagRun
    :param is_failure_callback: Flag to determine whether it is a Failure Callback or Success Callback
    :param msg: Additional Message that can be used for logging
    """

    def __init__(
        self,
        full_filepath: str,
        dag_id: str,
        execution_date: datetime,
        is_failure_callback: Optional[bool] = True,
        msg: Optional[str] = None,
    ):
        super().__init__(full_filepath=full_filepath, msg=msg)
        self.dag_id = dag_id
        self.execution_date = execution_date
        self.is_failure_callback = is_failure_callback


class SlaCallbackRequest(CallbackRequest):
    """
    A class with information about the SLA callback to be executed.

    :param full_filepath: File Path to use to run the callback
    :param dag_id: DAG ID
    """

    def __init__(self, full_filepath: str, dag_id: str):
        super().__init__(full_filepath)
        self.dag_id = dag_id
