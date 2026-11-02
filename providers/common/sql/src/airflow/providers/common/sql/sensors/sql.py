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

from collections.abc import Callable, Mapping, Sequence
from datetime import timedelta
from operator import itemgetter
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import AirflowException, BaseHook, BaseSensorOperator, conf
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.common.sql.triggers.sql import NO_ROWS_MESSAGE, SqlSensorTrigger
from airflow.providers.common.sql.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_2_2_PLUS

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context

_first_cell = itemgetter(0)

# Values that survive Airflow 2's JSON storage of trigger arguments unchanged.
_JSON_SCALARS = (str, int, float, bool, type(None))


def _is_first_cell(selector: Callable[[tuple[Any]], Any]) -> bool:
    # Compare by value rather than identity: a deep copy of the sensor, or an explicit
    # ``selector=itemgetter(0)``, must still count as the default condition.
    return isinstance(selector, itemgetter) and selector.__reduce__() == _first_cell.__reduce__()


def _survives_json(value: Any) -> bool:
    if isinstance(value, Mapping):
        return all(isinstance(k, str) and _survives_json(v) for k, v in value.items())
    if isinstance(value, (list, tuple)):
        return all(_survives_json(v) for v in value)
    return isinstance(value, _JSON_SCALARS)


class SqlSensorTriggerError(AirflowException):
    """Raised when the deferred ``SqlSensor`` trigger reports a failure."""


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
    :param deferrable: Wait in the triggerer instead of on a worker. The triggerer only
        checks the default condition (the first cell is truthy), so the sensor logs a
        warning and runs in its usual ``mode`` instead when ``success``, ``failure``,
        ``selector``, ``silent_fail``, ``never_fail`` or ``exponential_backoff`` is set,
        when a subclass overrides ``poke`` or ``_get_hook``, on Airflow 3 before 3.2.2, or
        on Airflow 2 when ``parameters`` holds values JSON cannot keep, such as dates.
    """

    template_fields: Sequence[str] = ("sql", "hook_params", "parameters", "conn_id")
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
        selector: Callable[[tuple[Any]], Any] = _first_cell,
        fail_on_empty: bool = False,
        hook_params: Mapping[str, Any] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
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
        self.deferrable = deferrable
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
                raise AirflowException(NO_ROWS_MESSAGE)
            return False

        condition = self.selector(records[0])
        if self.failure is not None:
            if callable(self.failure):
                if self.failure(condition):
                    message = f"Failure criteria met. self.failure({condition}) returned True"
                    raise AirflowException(message)
            else:
                message = f"self.failure is present, but not callable -> {self.failure}"
                raise AirflowException(message)

        if self.success is not None:
            if callable(self.success):
                return self.success(condition)
            message = f"self.success is present, but not callable -> {self.success}"
            raise AirflowException(message)
        return bool(condition)

    def execute(self, context: Context) -> Any:
        if not self.deferrable:
            return super().execute(context)
        if reason := self._reason_not_to_defer():
            self.log.warning("Cannot defer because %s; running in %s mode instead.", reason, self.mode)
            return super().execute(context)
        # silent_fail and never_fail fall back above, so an error here propagates exactly as
        # it would from the first poke of super().execute().
        if self.poke(context):
            self.log.info("Success criteria met. Exiting.")
            return None
        self.defer(
            trigger=SqlSensorTrigger(
                sql=self.sql,
                conn_id=self.conn_id,
                poke_interval=self.poke_interval,
                parameters=self.parameters,
                hook_params=dict(self.hook_params) if self.hook_params is not None else None,
                fail_on_empty=self.fail_on_empty,
            ),
            method_name=self.execute_complete.__name__,
            timeout=timedelta(seconds=self.timeout),
        )

    def _reason_not_to_defer(self) -> str | None:
        if AIRFLOW_V_3_0_PLUS and not AIRFLOW_V_3_2_2_PLUS:
            return "the triggerer before Airflow 3.2.2 cannot look up connections from a thread safely"
        if self.success is not None or self.failure is not None or not _is_first_cell(self.selector):
            return "success, failure and selector only run on a worker"
        if type(self).poke is not SqlSensor.poke or type(self)._get_hook is not SqlSensor._get_hook:
            return f"{type(self).__name__} overrides poke or _get_hook, which the triggerer does not run"
        if self.silent_fail or self.never_fail or self.exponential_backoff:
            return "silent_fail, never_fail and exponential_backoff only apply when poking on a worker"
        if not AIRFLOW_V_3_0_PLUS and not _survives_json(self.parameters):
            return (
                "Airflow 2 stores trigger arguments as JSON, which would hand the triggerer "
                "date, datetime or Decimal parameters as strings"
            )
        return None

    def execute_complete(self, context: Context, event: dict[str, Any]) -> None:
        if event["status"] == "failure":
            raise SqlSensorTriggerError(event["message"])
        self.log.info("Success criteria met. Exiting.")
