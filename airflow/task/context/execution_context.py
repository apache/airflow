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

from types import ModuleType
from typing import Any, NamedTuple

from pendulum import Pendulum

from airflow.exceptions import AirflowException


class ExecutionContext(NamedTuple):
    """
    Defines all stateful properties during execution.
    This context can be passed to any task either via task.context.current.get_current_context, or via
    requesting specific properties through the task's ctor, as in:

        def f(execution_date):
            print(execution_date) # This property is filled automatically by airflow
    """
    conf: Any
    dag: Any
    dag_run: str
    ds: str
    ds_nodash: str
    execution_date: Pendulum
    inlets: list
    macros: ModuleType
    next_ds: str
    next_ds_nodash: str
    next_execution_date: Pendulum
    outlets: list
    params: dict
    prev_ds: str
    prev_ds_nodash: str
    prev_execution_date: Pendulum
    prev_execution_date_success: Pendulum
    prev_start_date_success: Pendulum
    run_id: str
    task: Any
    task_instance: Any
    task_instance_key_str: str
    test_mode: bool
    ti: Any
    tomorrow_ds: str
    tomorrow_ds_nodash: str
    ts: str
    ts_nodash: str
    ts_nodash_with_tz: str
    var: dict
    yesterday_ds: str
    yesterday_ds_nodash: str

    def __getitem__(self, *args, **kwargs):
        """
        Allow slicing syntax for backwards compatibility to old-style context (dict access style)
        """
        if len(args) == 1 and isinstance(args[0], str):
            return getattr(self, args[0])
        raise AirflowException("Can only retrieve execution context properties via single string argument")

    def get(self, key):
        """
        Retrieve property via key (dict access style)
        """
        try:
            return getattr(self, key)
        except Exception as e:
            raise AirflowException(f"Key {key} not found in execution context!", e)
