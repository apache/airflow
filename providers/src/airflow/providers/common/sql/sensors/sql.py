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

from operator import itemgetter
from typing import TYPE_CHECKING, Any, Callable, Mapping, Sequence

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SqlSensor(BaseSensorOperator):
    """
    Run a SQL statement repeatedly until a criteria is met.

    This will keep trying until success or failure criteria are met, or if the
    first cell is not either ``0``, ``'0'``, ``''``, or ``None``. Optional
    success and failure callables are called with the first cell returned as the
    argument.

    If success callable is defined, the sensor will keep retrying until the
    criteria is met. If failure callable is defined, and the criteria is met,
    the sensor will raise AirflowException. Failure criteria is evaluated before
    success criteria. A fail_on_empty boolean can also be passed to the sensor
    in which case it will fail if no rows have been returned.

    :param conn_id: The connection to run the sensor against
    :param sql: The SQL to run. To pass, it needs to return at least one cell
        that contains a non-zero / empty string value.
    :param parameters: The parameters to render the SQL query with (optional).
    :param success: Success criteria for the sensor is a Callable that takes the output
        of selector as the only argument, and returns a boolean (optional).
    :param failure: Failure criteria for the sensor is a Callable that takes the output
        of selector as the only argument and returns a boolean (optional).
    :param selector: Function which takes the resulting row and transforms it before
        it is passed to success or failure (optional). Takes the first cell by default.
    :param fail_on_empty: Explicitly fail on no rows returned.
    :param hook_params: Extra config params to be passed to the underlying hook.
            Should match the desired hook constructor params.
    """

    template_fields: Sequence[str] = ("sql", "hook_params", "parameters")
    template_ext: Sequence[str] = (".hql", ".sql")
    ui_color = "#7c7287"

    def __init__(
        self,
        *,
        conn_id: str,
        sql: str,
        parameters: Mapping[str, Any] | None = None,
        success: Callable[[Any], bool] | None = None,
        failure: Callable[[Any], bool] | None = None,
        selector: Callable[[tuple[Any]], Any] = itemgetter(0),
        fail_on_empty: bool = False,
        hook_params: Mapping[str, Any] | None = None,
        **kwargs,
    ) -> None:
        self.conn_id = conn_id
        self.sql = sql
        self.parameters = parameters
        self.success = success
        self.failure = failure
        self.selector = selector
        self.fail_on_empty = fail_on_empty
        self.hook_params = hook_params
        super().__init__(**kwargs)

    def _get_hook(self) -> DbApiHook:
        conn = BaseHook.get_connection(self.conn_id)
        hook = conn.get_hook(hook_params=self.hook_params)
        if not isinstance(hook, DbApiHook):
            raise AirflowException(
                f"The connection type is not supported by {self.__class__.__name__}. "
                f"The associated hook should be a subclass of `DbApiHook`. Got {hook.__class__.__name__}"
            )
        return hook

    def poke(self, context: Context) -> bool:
        hook = self._get_hook()

        self.log.info("Poking: %s (with parameters %s)", self.sql, self.parameters)
        records = hook.get_records(self.sql, self.parameters)
        if not records:
            if self.fail_on_empty:
                message = "No rows returned, raising as per fail_on_empty flag"
                raise AirflowException(message)
            else:
                return False

        condition = self.selector(records[0])
        if self.failure is not None:
            if callable(self.failure):
                if self.failure(condition):
                    message = (
                        f"Failure criteria met. self.failure({condition}) returned True"
                    )
                    raise AirflowException(message)
            else:
                message = f"self.failure is present, but not callable -> {self.failure}"
                raise AirflowException(message)

        if self.success is not None:
            if callable(self.success):
                return self.success(condition)
            else:
                message = f"self.success is present, but not callable -> {self.success}"
                raise AirflowException(message)
        return bool(condition)
