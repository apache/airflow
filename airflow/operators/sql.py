#
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
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, SupportsAbs, Union

from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator, SkipMixin
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.utils.context import Context


def parse_boolean(val: str) -> Union[str, bool]:
    """Try to parse a string into boolean.

    Raises ValueError if the input is not a valid true- or false-like string value.
    """
    val = val.lower()
    if val in ('y', 'yes', 't', 'true', 'on', '1'):
        return True
    if val in ('n', 'no', 'f', 'false', 'off', '0'):
        return False
    raise ValueError(f"{val!r} is not a boolean-like string value")


class BaseSQLOperator(BaseOperator):
    """
    This is a base class for generic SQL Operator to get a DB Hook

    The provided method is .get_db_hook(). The default behavior will try to
    retrieve the DB hook based on connection type.
    You can custom the behavior by overriding the .get_db_hook() method.
    """

    def __init__(
        self,
        *,
        conn_id: Optional[str] = None,
        database: Optional[str] = None,
        hook_params: Optional[Dict] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.database = database
        self.hook_params = {} if hook_params is None else hook_params

    @cached_property
    def _hook(self):
        """Get DB Hook based on connection type"""
        self.log.debug("Get connection for %s", self.conn_id)
        conn = BaseHook.get_connection(self.conn_id)

        hook = conn.get_hook(hook_params=self.hook_params)
        if not isinstance(hook, DbApiHook):
            raise AirflowException(
                f'The connection type is not supported by {self.__class__.__name__}. '
                f'The associated hook should be a subclass of `DbApiHook`. Got {hook.__class__.__name__}'
            )

        if self.database:
            hook.schema = self.database

        return hook

    def get_db_hook(self) -> DbApiHook:
        """
        Get the database hook for the connection.

        :return: the database hook object.
        :rtype: DbApiHook
        """
        return self._hook


class SQLCheckOperator(BaseSQLOperator):
    """
    Performs checks against a db. The ``SQLCheckOperator`` expects
    a sql query that will return a single row. Each value on that
    first row is evaluated using python ``bool`` casting. If any of the
    values return ``False`` the check is failed and errors out.

    Note that Python bool casting evals the following as ``False``:

    * ``False``
    * ``0``
    * Empty string (``""``)
    * Empty list (``[]``)
    * Empty dictionary or set (``{}``)

    Given a query like ``SELECT COUNT(*) FROM foo``, it will fail only if
    the count ``== 0``. You can craft much more complex query that could,
    for instance, check that the table has the same number of rows as
    the source table upstream, or that the count of today's partition is
    greater than yesterday's partition, or that a set of metrics are less
    than 3 standard deviation for the 7 day average.

    This operator can be used as a data quality check in your pipeline, and
    depending on where you put it in your DAG, you have the choice to
    stop the critical path, preventing from
    publishing dubious data, or on the side and receive email alerts
    without stopping the progress of the DAG.

    :param sql: the sql to be executed. (templated)
    :param conn_id: the connection ID used to connect to the database.
    :param database: name of database which overwrite the defined one in connection
    """

    template_fields: Sequence[str] = ("sql",)
    template_ext: Sequence[str] = (
        ".hql",
        ".sql",
    )
    template_fields_renderers = {"sql": "sql"}
    ui_color = "#fff7e6"

    def __init__(
        self, *, sql: str, conn_id: Optional[str] = None, database: Optional[str] = None, **kwargs
    ) -> None:
        super().__init__(conn_id=conn_id, database=database, **kwargs)
        self.sql = sql

    def execute(self, context: Context):
        self.log.info("Executing SQL check: %s", self.sql)
        records = self.get_db_hook().get_first(self.sql)

        self.log.info("Record: %s", records)
        if not records:
            raise AirflowException("The query returned None")
        elif not all(bool(r) for r in records):
            raise AirflowException(f"Test failed.\nQuery:\n{self.sql}\nResults:\n{records!s}")

        self.log.info("Success.")


def _convert_to_float_if_possible(s):
    """
    A small helper function to convert a string to a numeric value
    if appropriate

    :param s: the string to be converted
    """
    try:
        ret = float(s)
    except (ValueError, TypeError):
        ret = s
    return ret


class SQLValueCheckOperator(BaseSQLOperator):
    """
    Performs a simple value check using sql code.

    :param sql: the sql to be executed. (templated)
    :param conn_id: the connection ID used to connect to the database.
    :param database: name of database which overwrite the defined one in connection
    """

    __mapper_args__ = {"polymorphic_identity": "SQLValueCheckOperator"}
    template_fields: Sequence[str] = (
        "sql",
        "pass_value",
    )
    template_ext: Sequence[str] = (
        ".hql",
        ".sql",
    )
    template_fields_renderers = {"sql": "sql"}
    ui_color = "#fff7e6"

    def __init__(
        self,
        *,
        sql: str,
        pass_value: Any,
        tolerance: Any = None,
        conn_id: Optional[str] = None,
        database: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(conn_id=conn_id, database=database, **kwargs)
        self.sql = sql
        self.pass_value = str(pass_value)
        tol = _convert_to_float_if_possible(tolerance)
        self.tol = tol if isinstance(tol, float) else None
        self.has_tolerance = self.tol is not None

    def execute(self, context=None):
        self.log.info("Executing SQL check: %s", self.sql)
        records = self.get_db_hook().get_first(self.sql)

        if not records:
            raise AirflowException("The query returned None")

        pass_value_conv = _convert_to_float_if_possible(self.pass_value)
        is_numeric_value_check = isinstance(pass_value_conv, float)

        tolerance_pct_str = str(self.tol * 100) + "%" if self.has_tolerance else None
        error_msg = (
            "Test failed.\nPass value:{pass_value_conv}\n"
            "Tolerance:{tolerance_pct_str}\n"
            "Query:\n{sql}\nResults:\n{records!s}"
        ).format(
            pass_value_conv=pass_value_conv,
            tolerance_pct_str=tolerance_pct_str,
            sql=self.sql,
            records=records,
        )

        if not is_numeric_value_check:
            tests = self._get_string_matches(records, pass_value_conv)
        elif is_numeric_value_check:
            try:
                numeric_records = self._to_float(records)
            except (ValueError, TypeError):
                raise AirflowException(f"Converting a result to float failed.\n{error_msg}")
            tests = self._get_numeric_matches(numeric_records, pass_value_conv)
        else:
            tests = []

        if not all(tests):
            raise AirflowException(error_msg)

    def _to_float(self, records):
        return [float(record) for record in records]

    def _get_string_matches(self, records, pass_value_conv):
        return [str(record) == pass_value_conv for record in records]

    def _get_numeric_matches(self, numeric_records, numeric_pass_value_conv):
        if self.has_tolerance:
            return [
                numeric_pass_value_conv * (1 - self.tol) <= record <= numeric_pass_value_conv * (1 + self.tol)
                for record in numeric_records
            ]

        return [record == numeric_pass_value_conv for record in numeric_records]


class SQLIntervalCheckOperator(BaseSQLOperator):
    """
    Checks that the values of metrics given as SQL expressions are within
    a certain tolerance of the ones from days_back before.

    :param table: the table name
    :param conn_id: the connection ID used to connect to the database.
    :param database: name of database which will overwrite the defined one in connection
    :param days_back: number of days between ds and the ds we want to check
        against. Defaults to 7 days
    :param date_filter_column: The column name for the dates to filter on. Defaults to 'ds'
    :param ratio_formula: which formula to use to compute the ratio between
        the two metrics. Assuming cur is the metric of today and ref is
        the metric to today - days_back.

        max_over_min: computes max(cur, ref) / min(cur, ref)
        relative_diff: computes abs(cur-ref) / ref

        Default: 'max_over_min'
    :param ignore_zero: whether we should ignore zero metrics
    :param metrics_thresholds: a dictionary of ratios indexed by metrics
    """

    __mapper_args__ = {"polymorphic_identity": "SQLIntervalCheckOperator"}
    template_fields: Sequence[str] = ("sql1", "sql2")
    template_ext: Sequence[str] = (
        ".hql",
        ".sql",
    )
    template_fields_renderers = {"sql1": "sql", "sql2": "sql"}
    ui_color = "#fff7e6"

    ratio_formulas = {
        "max_over_min": lambda cur, ref: float(max(cur, ref)) / min(cur, ref),
        "relative_diff": lambda cur, ref: float(abs(cur - ref)) / ref,
    }

    def __init__(
        self,
        *,
        table: str,
        metrics_thresholds: Dict[str, int],
        date_filter_column: Optional[str] = "ds",
        days_back: SupportsAbs[int] = -7,
        ratio_formula: Optional[str] = "max_over_min",
        ignore_zero: bool = True,
        conn_id: Optional[str] = None,
        database: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(conn_id=conn_id, database=database, **kwargs)
        if ratio_formula not in self.ratio_formulas:
            msg_template = "Invalid diff_method: {diff_method}. Supported diff methods are: {diff_methods}"

            raise AirflowException(
                msg_template.format(diff_method=ratio_formula, diff_methods=self.ratio_formulas)
            )
        self.ratio_formula = ratio_formula
        self.ignore_zero = ignore_zero
        self.table = table
        self.metrics_thresholds = metrics_thresholds
        self.metrics_sorted = sorted(metrics_thresholds.keys())
        self.date_filter_column = date_filter_column
        self.days_back = -abs(days_back)
        sqlexp = ", ".join(self.metrics_sorted)
        sqlt = f"SELECT {sqlexp} FROM {table} WHERE {date_filter_column}="

        self.sql1 = sqlt + "'{{ ds }}'"
        self.sql2 = sqlt + "'{{ macros.ds_add(ds, " + str(self.days_back) + ") }}'"

    def execute(self, context=None):
        hook = self.get_db_hook()
        self.log.info("Using ratio formula: %s", self.ratio_formula)
        self.log.info("Executing SQL check: %s", self.sql2)
        row2 = hook.get_first(self.sql2)
        self.log.info("Executing SQL check: %s", self.sql1)
        row1 = hook.get_first(self.sql1)

        if not row2:
            raise AirflowException(f"The query {self.sql2} returned None")
        if not row1:
            raise AirflowException(f"The query {self.sql1} returned None")

        current = dict(zip(self.metrics_sorted, row1))
        reference = dict(zip(self.metrics_sorted, row2))

        ratios = {}
        test_results = {}

        for metric in self.metrics_sorted:
            cur = current[metric]
            ref = reference[metric]
            threshold = self.metrics_thresholds[metric]
            if cur == 0 or ref == 0:
                ratios[metric] = None
                test_results[metric] = self.ignore_zero
            else:
                ratios[metric] = self.ratio_formulas[self.ratio_formula](current[metric], reference[metric])
                test_results[metric] = ratios[metric] < threshold

            self.log.info(
                (
                    "Current metric for %s: %s\n"
                    "Past metric for %s: %s\n"
                    "Ratio for %s: %s\n"
                    "Threshold: %s\n"
                ),
                metric,
                cur,
                metric,
                ref,
                metric,
                ratios[metric],
                threshold,
            )

        if not all(test_results.values()):
            failed_tests = [it[0] for it in test_results.items() if not it[1]]
            self.log.warning(
                "The following %s tests out of %s failed:",
                len(failed_tests),
                len(self.metrics_sorted),
            )
            for k in failed_tests:
                self.log.warning(
                    "'%s' check failed. %s is above %s",
                    k,
                    ratios[k],
                    self.metrics_thresholds[k],
                )
            raise AirflowException(f"The following tests have failed:\n {', '.join(sorted(failed_tests))}")

        self.log.info("All tests have passed")


class SQLThresholdCheckOperator(BaseSQLOperator):
    """
    Performs a value check using sql code against a minimum threshold
    and a maximum threshold. Thresholds can be in the form of a numeric
    value OR a sql statement that results a numeric.

    :param sql: the sql to be executed. (templated)
    :param conn_id: the connection ID used to connect to the database.
    :param database: name of database which overwrite the defined one in connection
    :param min_threshold: numerical value or min threshold sql to be executed (templated)
    :param max_threshold: numerical value or max threshold sql to be executed (templated)
    """

    template_fields: Sequence[str] = ("sql", "min_threshold", "max_threshold")
    template_ext: Sequence[str] = (
        ".hql",
        ".sql",
    )
    template_fields_renderers = {"sql": "sql"}

    def __init__(
        self,
        *,
        sql: str,
        min_threshold: Any,
        max_threshold: Any,
        conn_id: Optional[str] = None,
        database: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(conn_id=conn_id, database=database, **kwargs)
        self.sql = sql
        self.min_threshold = _convert_to_float_if_possible(min_threshold)
        self.max_threshold = _convert_to_float_if_possible(max_threshold)

    def execute(self, context=None):
        hook = self.get_db_hook()
        result = hook.get_first(self.sql)[0]

        if isinstance(self.min_threshold, float):
            lower_bound = self.min_threshold
        else:
            lower_bound = hook.get_first(self.min_threshold)[0]

        if isinstance(self.max_threshold, float):
            upper_bound = self.max_threshold
        else:
            upper_bound = hook.get_first(self.max_threshold)[0]

        meta_data = {
            "result": result,
            "task_id": self.task_id,
            "min_threshold": lower_bound,
            "max_threshold": upper_bound,
            "within_threshold": lower_bound <= result <= upper_bound,
        }

        self.push(meta_data)
        if not meta_data["within_threshold"]:
            error_msg = (
                f'Threshold Check: "{meta_data.get("task_id")}" failed.\n'
                f'DAG: {self.dag_id}\nTask_id: {meta_data.get("task_id")}\n'
                f'Check description: {meta_data.get("description")}\n'
                f"SQL: {self.sql}\n"
                f'Result: {round(meta_data.get("result"), 2)} is not within thresholds '
                f'{meta_data.get("min_threshold")} and {meta_data.get("max_threshold")}'
            )
            raise AirflowException(error_msg)

        self.log.info("Test %s Successful.", self.task_id)

    def push(self, meta_data):
        """
        Optional: Send data check info and metadata to an external database.
        Default functionality will log metadata.
        """
        info = "\n".join(f"""{key}: {item}""" for key, item in meta_data.items())
        self.log.info("Log from %s:\n%s", self.dag_id, info)


class BranchSQLOperator(BaseSQLOperator, SkipMixin):
    """
    Allows a DAG to "branch" or follow a specified path based on the results of a SQL query.

    :param sql: The SQL code to be executed, should return true or false (templated)
       Template reference are recognized by str ending in '.sql'.
       Expected SQL query to return Boolean (True/False), integer (0 = False, Otherwise = 1)
       or string (true/y/yes/1/on/false/n/no/0/off).
    :param follow_task_ids_if_true: task id or task ids to follow if query returns true
    :param follow_task_ids_if_false: task id or task ids to follow if query returns false
    :param conn_id: the connection ID used to connect to the database.
    :param database: name of database which overwrite the defined one in connection
    :param parameters: (optional) the parameters to render the SQL query with.
    """

    template_fields: Sequence[str] = ("sql",)
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {"sql": "sql"}
    ui_color = "#a22034"
    ui_fgcolor = "#F7F7F7"

    def __init__(
        self,
        *,
        sql: str,
        follow_task_ids_if_true: List[str],
        follow_task_ids_if_false: List[str],
        conn_id: str = "default_conn_id",
        database: Optional[str] = None,
        parameters: Optional[Union[Mapping, Iterable]] = None,
        **kwargs,
    ) -> None:
        super().__init__(conn_id=conn_id, database=database, **kwargs)
        self.sql = sql
        self.parameters = parameters
        self.follow_task_ids_if_true = follow_task_ids_if_true
        self.follow_task_ids_if_false = follow_task_ids_if_false

    def execute(self, context: Context):
        self.log.info(
            "Executing: %s (with parameters %s) with connection: %s",
            self.sql,
            self.parameters,
            self.conn_id,
        )
        record = self.get_db_hook().get_first(self.sql, self.parameters)
        if not record:
            raise AirflowException(
                "No rows returned from sql query. Operator expected True or False return value."
            )

        if isinstance(record, list):
            if isinstance(record[0], list):
                query_result = record[0][0]
            else:
                query_result = record[0]
        elif isinstance(record, tuple):
            query_result = record[0]
        else:
            query_result = record

        self.log.info("Query returns %s, type '%s'", query_result, type(query_result))

        follow_branch = None
        try:
            if isinstance(query_result, bool):
                if query_result:
                    follow_branch = self.follow_task_ids_if_true
            elif isinstance(query_result, str):
                # return result is not Boolean, try to convert from String to Boolean
                if parse_boolean(query_result):
                    follow_branch = self.follow_task_ids_if_true
            elif isinstance(query_result, int):
                if bool(query_result):
                    follow_branch = self.follow_task_ids_if_true
            else:
                raise AirflowException(
                    f"Unexpected query return result '{query_result}' type '{type(query_result)}'"
                )

            if follow_branch is None:
                follow_branch = self.follow_task_ids_if_false
        except ValueError:
            raise AirflowException(
                f"Unexpected query return result '{query_result}' type '{type(query_result)}'"
            )

        self.skip_all_except(context["ti"], follow_branch)
