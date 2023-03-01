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
from __future__ import annotations

import ast
import re
from typing import TYPE_CHECKING, Any, Callable, Iterable, Mapping, NoReturn, Sequence, SupportsAbs

from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator, SkipMixin
from airflow.providers.common.sql.hooks.sql import DbApiHook, fetch_all_handler, return_single_query_results

if TYPE_CHECKING:
    from airflow.utils.context import Context


def _convert_to_float_if_possible(s: str) -> float | str:
    try:
        return float(s)
    except (ValueError, TypeError):
        return s


def _parse_boolean(val: str) -> str | bool:
    """Try to parse a string into boolean.

    Raises ValueError if the input is not a valid true- or false-like string value.
    """
    val = val.lower()
    if val in ("y", "yes", "t", "true", "on", "1"):
        return True
    if val in ("n", "no", "f", "false", "off", "0"):
        return False
    raise ValueError(f"{val!r} is not a boolean-like string value")


def _get_failed_checks(checks, col=None):
    """
    IMPORTANT!!! Keep it for compatibility with released 8.4.0 version of google provider.

    Unfortunately the provider used _get_failed_checks and parse_boolean as imports and we should
    keep those methods to avoid 8.4.0 version from failing.
    """
    if col:
        return [
            f"Column: {col}\nCheck: {check},\nCheck Values: {check_values}\n"
            for check, check_values in checks.items()
            if not check_values["success"]
        ]
    return [
        f"\tCheck: {check},\n\tCheck Values: {check_values}\n"
        for check, check_values in checks.items()
        if not check_values["success"]
    ]


parse_boolean = _parse_boolean
"""
IMPORTANT!!! Keep it for compatibility with released 8.4.0 version of google provider.

Unfortunately the provider used _get_failed_checks and parse_boolean as imports and we should
keep those methods to avoid 8.4.0 version from failing.
"""


_PROVIDERS_MATCHER = re.compile(r"airflow\.providers\.(.*)\.hooks.*")

_MIN_SUPPORTED_PROVIDERS_VERSION = {
    "amazon": "4.1.0",
    "apache.drill": "2.1.0",
    "apache.druid": "3.1.0",
    "apache.hive": "3.1.0",
    "apache.pinot": "3.1.0",
    "databricks": "3.1.0",
    "elasticsearch": "4.1.0",
    "exasol": "3.1.0",
    "google": "8.2.0",
    "jdbc": "3.1.0",
    "mssql": "3.1.0",
    "mysql": "3.1.0",
    "odbc": "3.1.0",
    "oracle": "3.1.0",
    "postgres": "5.1.0",
    "presto": "3.1.0",
    "qubole": "3.1.0",
    "slack": "5.1.0",
    "snowflake": "3.1.0",
    "sqlite": "3.1.0",
    "trino": "3.1.0",
    "vertica": "3.1.0",
}


class BaseSQLOperator(BaseOperator):
    """
    This is a base class for generic SQL Operator to get a DB Hook

    The provided method is .get_db_hook(). The default behavior will try to
    retrieve the DB hook based on connection type.
    You can customize the behavior by overriding the .get_db_hook() method.

    :param conn_id: reference to a specific database
    """

    def __init__(
        self,
        *,
        conn_id: str | None = None,
        database: str | None = None,
        hook_params: dict | None = None,
        retry_on_failure: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.database = database
        self.hook_params = {} if hook_params is None else hook_params
        self.retry_on_failure = retry_on_failure

    @cached_property
    def _hook(self):
        """Get DB Hook based on connection type"""
        self.log.debug("Get connection for %s", self.conn_id)
        conn = BaseHook.get_connection(self.conn_id)
        hook = conn.get_hook(hook_params=self.hook_params)
        if not isinstance(hook, DbApiHook):
            from airflow.hooks.dbapi_hook import DbApiHook as _DbApiHook

            if isinstance(hook, _DbApiHook):
                # This case might happen if user installed common.sql provider but did not upgrade the
                # Other provider's versions to a version that supports common.sql provider
                class_module = hook.__class__.__module__
                match = _PROVIDERS_MATCHER.match(class_module)
                if match:
                    provider = match.group(1)
                    min_version = _MIN_SUPPORTED_PROVIDERS_VERSION.get(provider)
                    if min_version:
                        raise AirflowException(
                            f"You are trying to use common-sql with {hook.__class__.__name__},"
                            f" but the Hook class comes from provider {provider} that does not support it."
                            f" Please upgrade provider {provider} to at least {min_version}."
                        )
            raise AirflowException(
                f"You are trying to use `common-sql` with {hook.__class__.__name__},"
                " but its provider does not support it. Please upgrade the provider to a version that"
                " supports `common-sql`. The hook class should be a subclass of"
                " `airflow.providers.common.sql.hooks.sql.DbApiHook`."
                f" Got {hook.__class__.__name__} Hook with class hierarchy: {hook.__class__.mro()}"
            )

        if self.database:
            hook.schema = self.database

        return hook

    def get_db_hook(self) -> DbApiHook:
        """
        Get the database hook for the connection.

        :return: the database hook object.
        """
        return self._hook

    def _raise_exception(self, exception_string: str) -> NoReturn:
        if self.retry_on_failure:
            raise AirflowException(exception_string)
        raise AirflowFailException(exception_string)


class SQLExecuteQueryOperator(BaseSQLOperator):
    """
    Executes SQL code in a specific database
    :param sql: the SQL code or string pointing to a template file to be executed (templated).
    File must have a '.sql' extensions.

    When implementing a specific Operator, you can also implement `_process_output` method in the
    hook to perform additional processing of values returned by the DB Hook of yours. For example, you
    can join description retrieved from the cursors of your statements with returned values, or save
    the output of your operator to a file.

    :param autocommit: (optional) if True, each command is automatically committed (default: False).
    :param parameters: (optional) the parameters to render the SQL query with.
    :param handler: (optional) the function that will be applied to the cursor (default: fetch_all_handler).
    :param split_statements: (optional) if split single SQL string into statements. By default, defers
        to the default value in the ``run`` method of the configured hook.
    :param return_last: (optional) return the result of only last statement (default: True).

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SQLExecuteQueryOperator`
    """

    template_fields: Sequence[str] = ("conn_id", "sql", "parameters")
    template_ext: Sequence[str] = (".sql", ".json")
    template_fields_renderers = {"sql": "sql", "parameters": "json"}
    ui_color = "#cdaaed"

    def __init__(
        self,
        *,
        sql: str | list[str],
        autocommit: bool = False,
        parameters: Mapping | Iterable | None = None,
        handler: Callable[[Any], Any] = fetch_all_handler,
        split_statements: bool | None = None,
        return_last: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.autocommit = autocommit
        self.parameters = parameters
        self.handler = handler
        self.split_statements = split_statements
        self.return_last = return_last

    def _process_output(self, results: list[Any], descriptions: list[Sequence[Sequence] | None]) -> list[Any]:
        """
        Processes output before it is returned by the operator.

        It can be overridden by the subclass in case some extra processing is needed. Note that unlike
        DBApiHook return values returned - the results passed and returned by ``_process_output`` should
        always be lists of results - each element of the list is a result from a single SQL statement
        (typically this will be list of Rows). You have to make sure that this is the same for returned
        values = there should be one element in the list for each statement executed by the hook..

        The "process_output" method can override the returned output - augmenting or processing the
        output as needed - the output returned will be returned as execute return value and if
        do_xcom_push is set to True, it will be set as XCom returned.

        :param results: results in the form of list of rows.
        :param descriptions: list of descriptions returned by ``cur.description`` in the Python DBAPI
        """
        return results

    def execute(self, context):
        self.log.info("Executing: %s", self.sql)
        hook = self.get_db_hook()
        if self.split_statements is not None:
            extra_kwargs = {"split_statements": self.split_statements}
        else:
            extra_kwargs = {}
        output = hook.run(
            sql=self.sql,
            autocommit=self.autocommit,
            parameters=self.parameters,
            handler=self.handler if self.do_xcom_push else None,
            return_last=self.return_last,
            **extra_kwargs,
        )
        if not self.do_xcom_push:
            return None
        if return_single_query_results(self.sql, self.return_last, self.split_statements):
            # For simplicity, we pass always list as input to _process_output, regardless if
            # single query results are going to be returned, and we return the first element
            # of the list in this case from the (always) list returned by _process_output
            return self._process_output([output], hook.descriptions)[-1]
        return self._process_output(output, hook.descriptions)

    def prepare_template(self) -> None:
        """Parse template file for attribute parameters."""
        if isinstance(self.parameters, str):
            self.parameters = ast.literal_eval(self.parameters)


class SQLColumnCheckOperator(BaseSQLOperator):
    """
    Performs one or more of the templated checks in the column_checks dictionary.
    Checks are performed on a per-column basis specified by the column_mapping.

    Each check can take one or more of the following options:
    - equal_to: an exact value to equal, cannot be used with other comparison options
    - greater_than: value that result should be strictly greater than
    - less_than: value that results should be strictly less than
    - geq_to: value that results should be greater than or equal to
    - leq_to: value that results should be less than or equal to
    - tolerance: the percentage that the result may be off from the expected value
    - partition_clause: an extra clause passed into a WHERE statement to partition data

    :param table: the table to run checks on
    :param column_mapping: the dictionary of columns and their associated checks, e.g.

    .. code-block:: python

        {
            "col_name": {
                "null_check": {
                    "equal_to": 0,
                    "partition_clause": "foreign_key IS NOT NULL",
                },
                "min": {
                    "greater_than": 5,
                    "leq_to": 10,
                    "tolerance": 0.2,
                },
                "max": {"less_than": 1000, "geq_to": 10, "tolerance": 0.01},
            }
        }

    :param partition_clause: a partial SQL statement that is added to a WHERE clause in the query built by
        the operator that creates partition_clauses for the checks to run on, e.g.

    .. code-block:: python

        "date = '1970-01-01'"

    :param conn_id: the connection ID used to connect to the database
    :param database: name of database which overwrite the defined one in connection
    :param accept_none: whether or not to accept None values returned by the query. If true, converts None
        to 0.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SQLColumnCheckOperator`
    """

    template_fields = ("partition_clause", "table", "sql")
    template_fields_renderers = {"sql": "sql"}

    sql_check_template = """
        SELECT '{column}' AS col_name, '{check}' AS check_type, {column}_{check} AS check_result
        FROM (SELECT {check_statement} AS {column}_{check} FROM {table} {partition_clause}) AS sq
    """

    column_checks = {
        "null_check": "SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END)",
        "distinct_check": "COUNT(DISTINCT({column}))",
        "unique_check": "COUNT({column}) - COUNT(DISTINCT({column}))",
        "min": "MIN({column})",
        "max": "MAX({column})",
    }

    def __init__(
        self,
        *,
        table: str,
        column_mapping: dict[str, dict[str, Any]],
        partition_clause: str | None = None,
        conn_id: str | None = None,
        database: str | None = None,
        accept_none: bool = True,
        **kwargs,
    ):
        super().__init__(conn_id=conn_id, database=database, **kwargs)

        self.table = table
        self.column_mapping = column_mapping
        self.partition_clause = partition_clause
        self.accept_none = accept_none

        def _build_checks_sql():
            for column, checks in self.column_mapping.items():
                for check, check_values in checks.items():
                    self._column_mapping_validation(check, check_values)
                yield self._generate_sql_query(column, checks)

        checks_sql = "UNION ALL".join(_build_checks_sql())

        self.sql = f"SELECT col_name, check_type, check_result FROM ({checks_sql}) AS check_columns"

    def execute(self, context: Context):
        hook = self.get_db_hook()
        records = hook.get_records(self.sql)

        if not records:
            self._raise_exception(f"The following query returned zero rows: {self.sql}")

        self.log.info("Record: %s", records)

        for column, check, result in records:
            tolerance = self.column_mapping[column][check].get("tolerance")

            self.column_mapping[column][check]["result"] = result
            self.column_mapping[column][check]["success"] = self._get_match(
                self.column_mapping[column][check], result, tolerance
            )

        failed_tests = [
            f"Column: {col}\n\tCheck: {check},\n\tCheck Values: {check_values}\n"
            for col, checks in self.column_mapping.items()
            for check, check_values in checks.items()
            if not check_values["success"]
        ]
        if failed_tests:
            exception_string = (
                f"Test failed.\nResults:\n{records!s}\n"
                f"The following tests have failed:\n{''.join(failed_tests)}"
            )
            self._raise_exception(exception_string)

        self.log.info("All tests have passed")

    def _generate_sql_query(self, column, checks):
        def _generate_partition_clause(check):
            if self.partition_clause and "partition_clause" not in checks[check]:
                return f"WHERE {self.partition_clause}"
            elif not self.partition_clause and "partition_clause" in checks[check]:
                return f"WHERE {checks[check]['partition_clause']}"
            elif self.partition_clause and "partition_clause" in checks[check]:
                return f"WHERE {self.partition_clause} AND {checks[check]['partition_clause']}"
            else:
                return ""

        checks_sql = "UNION ALL".join(
            self.sql_check_template.format(
                check_statement=self.column_checks[check].format(column=column),
                check=check,
                table=self.table,
                column=column,
                partition_clause=_generate_partition_clause(check),
            )
            for check in checks
        )
        return checks_sql

    def _get_match(self, check_values, record, tolerance=None) -> bool:
        if record is None and self.accept_none:
            record = 0
        match_boolean = True
        if "geq_to" in check_values:
            if tolerance is not None:
                match_boolean = record >= check_values["geq_to"] * (1 - tolerance)
            else:
                match_boolean = record >= check_values["geq_to"]
        elif "greater_than" in check_values:
            if tolerance is not None:
                match_boolean = record > check_values["greater_than"] * (1 - tolerance)
            else:
                match_boolean = record > check_values["greater_than"]
        if "leq_to" in check_values:
            if tolerance is not None:
                match_boolean = record <= check_values["leq_to"] * (1 + tolerance) and match_boolean
            else:
                match_boolean = record <= check_values["leq_to"] and match_boolean
        elif "less_than" in check_values:
            if tolerance is not None:
                match_boolean = record < check_values["less_than"] * (1 + tolerance) and match_boolean
            else:
                match_boolean = record < check_values["less_than"] and match_boolean
        if "equal_to" in check_values:
            if tolerance is not None:
                match_boolean = (
                    check_values["equal_to"] * (1 - tolerance)
                    <= record
                    <= check_values["equal_to"] * (1 + tolerance)
                ) and match_boolean
            else:
                match_boolean = record == check_values["equal_to"] and match_boolean
        return match_boolean

    def _column_mapping_validation(self, check, check_values):
        if check not in self.column_checks:
            raise AirflowException(f"Invalid column check: {check}.")
        if (
            "greater_than" not in check_values
            and "geq_to" not in check_values
            and "less_than" not in check_values
            and "leq_to" not in check_values
            and "equal_to" not in check_values
        ):
            raise ValueError(
                "Please provide one or more of: less_than, leq_to, "
                "greater_than, geq_to, or equal_to in the check's dict."
            )

        if "greater_than" in check_values and "less_than" in check_values:
            if check_values["greater_than"] >= check_values["less_than"]:
                raise ValueError(
                    "greater_than should be strictly less than "
                    "less_than. Use geq_to or leq_to for "
                    "overlapping equality."
                )

        if "greater_than" in check_values and "leq_to" in check_values:
            if check_values["greater_than"] >= check_values["leq_to"]:
                raise ValueError(
                    "greater_than must be strictly less than leq_to. "
                    "Use geq_to with leq_to for overlapping equality."
                )

        if "geq_to" in check_values and "less_than" in check_values:
            if check_values["geq_to"] >= check_values["less_than"]:
                raise ValueError(
                    "geq_to should be strictly less than less_than. "
                    "Use leq_to with geq_to for overlapping equality."
                )

        if "geq_to" in check_values and "leq_to" in check_values:
            if check_values["geq_to"] > check_values["leq_to"]:
                raise ValueError("geq_to should be less than or equal to leq_to.")

        if "greater_than" in check_values and "geq_to" in check_values:
            raise ValueError("Only supply one of greater_than or geq_to.")

        if "less_than" in check_values and "leq_to" in check_values:
            raise ValueError("Only supply one of less_than or leq_to.")

        if (
            "greater_than" in check_values
            or "geq_to" in check_values
            or "less_than" in check_values
            or "leq_to" in check_values
        ) and "equal_to" in check_values:
            raise ValueError(
                "equal_to cannot be passed with a greater or less than "
                "function. To specify 'greater than or equal to' or "
                "'less than or equal to', use geq_to or leq_to."
            )


class SQLTableCheckOperator(BaseSQLOperator):
    """
    Performs one or more of the checks provided in the checks dictionary.
    Checks should be written to return a boolean result.

    :param table: the table to run checks on
    :param checks: the dictionary of checks, where check names are followed by a dictionary containing at
        least a check statement, and optionally a partition clause, e.g.:

    .. code-block:: python

        {
            "row_count_check": {"check_statement": "COUNT(*) = 1000"},
            "column_sum_check": {"check_statement": "col_a + col_b < col_c"},
            "third_check": {"check_statement": "MIN(col) = 1", "partition_clause": "col IS NOT NULL"},
        }


    :param partition_clause: a partial SQL statement that is added to a WHERE clause in the query built by
        the operator that creates partition_clauses for the checks to run on, e.g.

    .. code-block:: python

        "date = '1970-01-01'"

    :param conn_id: the connection ID used to connect to the database
    :param database: name of database which overwrite the defined one in connection

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SQLTableCheckOperator`
    """

    template_fields = ("partition_clause", "table", "sql", "conn_id")

    template_fields_renderers = {"sql": "sql"}

    sql_check_template = """
    SELECT '{check_name}' AS check_name, MIN({check_name}) AS check_result
    FROM (SELECT CASE WHEN {check_statement} THEN 1 ELSE 0 END AS {check_name}
          FROM {table} {partition_clause}) AS sq
    """

    def __init__(
        self,
        *,
        table: str,
        checks: dict[str, dict[str, Any]],
        partition_clause: str | None = None,
        conn_id: str | None = None,
        database: str | None = None,
        **kwargs,
    ):
        super().__init__(conn_id=conn_id, database=database, **kwargs)

        self.table = table
        self.checks = checks
        self.partition_clause = partition_clause
        self.sql = f"SELECT check_name, check_result FROM ({self._generate_sql_query()}) AS check_table"

    def execute(self, context: Context):
        hook = self.get_db_hook()
        records = hook.get_records(self.sql)

        if not records:
            self._raise_exception(f"The following query returned zero rows: {self.sql}")

        self.log.info("Record:\n%s", records)

        for row in records:
            check, result = row
            self.checks[check]["success"] = _parse_boolean(str(result))

        failed_tests = [
            f"\tCheck: {check},\n\tCheck Values: {check_values}\n"
            for check, check_values in self.checks.items()
            if not check_values["success"]
        ]
        if failed_tests:
            exception_string = (
                f"Test failed.\nQuery:\n{self.sql}\nResults:\n{records!s}\n"
                f"The following tests have failed:\n{', '.join(failed_tests)}"
            )
            self._raise_exception(exception_string)

        self.log.info("All tests have passed")

    def _generate_sql_query(self):
        self.log.info("Partition clause: %s", self.partition_clause)

        def _generate_partition_clause(check_name):
            if self.partition_clause and "partition_clause" not in self.checks[check_name]:
                return f"WHERE {self.partition_clause}"
            elif not self.partition_clause and "partition_clause" in self.checks[check_name]:
                return f"WHERE {self.checks[check_name]['partition_clause']}"
            elif self.partition_clause and "partition_clause" in self.checks[check_name]:
                return f"WHERE {self.partition_clause} AND {self.checks[check_name]['partition_clause']}"
            else:
                return ""

        return "UNION ALL".join(
            self.sql_check_template.format(
                check_statement=value["check_statement"],
                check_name=check_name,
                table=self.table,
                partition_clause=_generate_partition_clause(check_name),
            )
            for check_name, value in self.checks.items()
        )


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
    :param parameters: (optional) the parameters to render the SQL query with.
    """

    template_fields: Sequence[str] = ("sql",)
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
        conn_id: str | None = None,
        database: str | None = None,
        parameters: Iterable | Mapping | None = None,
        **kwargs,
    ) -> None:
        super().__init__(conn_id=conn_id, database=database, **kwargs)
        self.sql = sql
        self.parameters = parameters

    def execute(self, context: Context):
        self.log.info("Executing SQL check: %s", self.sql)
        records = self.get_db_hook().get_first(self.sql, self.parameters)

        self.log.info("Record: %s", records)
        if not records:
            self._raise_exception(f"The following query returned zero rows: {self.sql}")
        elif not all(bool(r) for r in records):
            self._raise_exception(f"Test failed.\nQuery:\n{self.sql}\nResults:\n{records!s}")

        self.log.info("Success.")


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
        conn_id: str | None = None,
        database: str | None = None,
        **kwargs,
    ):
        super().__init__(conn_id=conn_id, database=database, **kwargs)
        self.sql = sql
        self.pass_value = str(pass_value)
        tol = _convert_to_float_if_possible(tolerance)
        self.tol = tol if isinstance(tol, float) else None
        self.has_tolerance = self.tol is not None

    def execute(self, context: Context):
        self.log.info("Executing SQL check: %s", self.sql)
        records = self.get_db_hook().get_first(self.sql)

        if not records:
            self._raise_exception(f"The following query returned zero rows: {self.sql}")

        pass_value_conv = _convert_to_float_if_possible(self.pass_value)
        is_numeric_value_check = isinstance(pass_value_conv, float)

        tolerance_pct_str = str(self.tol * 100) + "%" if self.tol is not None else None
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
            self._raise_exception(error_msg)

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
        metrics_thresholds: dict[str, int],
        date_filter_column: str | None = "ds",
        days_back: SupportsAbs[int] = -7,
        ratio_formula: str | None = "max_over_min",
        ignore_zero: bool = True,
        conn_id: str | None = None,
        database: str | None = None,
        **kwargs,
    ):
        super().__init__(conn_id=conn_id, database=database, **kwargs)
        if ratio_formula not in self.ratio_formulas:
            msg_template = "Invalid diff_method: {diff_method}. Supported diff methods are: {diff_methods}"

            raise AirflowFailException(
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

    def execute(self, context: Context):
        hook = self.get_db_hook()
        self.log.info("Using ratio formula: %s", self.ratio_formula)
        self.log.info("Executing SQL check: %s", self.sql2)
        row2 = hook.get_first(self.sql2)
        self.log.info("Executing SQL check: %s", self.sql1)
        row1 = hook.get_first(self.sql1)

        if not row2:
            self._raise_exception(f"The following query returned zero rows: {self.sql2}")
        if not row1:
            self._raise_exception(f"The following query returned zero rows: {self.sql1}")

        current = dict(zip(self.metrics_sorted, row1))
        reference = dict(zip(self.metrics_sorted, row2))

        ratios: dict[str, int | None] = {}
        test_results = {}

        for metric in self.metrics_sorted:
            cur = current[metric]
            ref = reference[metric]
            threshold = self.metrics_thresholds[metric]
            if cur == 0 or ref == 0:
                ratios[metric] = None
                test_results[metric] = self.ignore_zero
            else:
                ratio_metric = self.ratio_formulas[self.ratio_formula](current[metric], reference[metric])
                ratios[metric] = ratio_metric
                if ratio_metric is not None:
                    test_results[metric] = ratio_metric < threshold
                else:
                    test_results[metric] = self.ignore_zero

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
            self._raise_exception(f"The following tests have failed:\n {', '.join(sorted(failed_tests))}")

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
        conn_id: str | None = None,
        database: str | None = None,
        **kwargs,
    ):
        super().__init__(conn_id=conn_id, database=database, **kwargs)
        self.sql = sql
        self.min_threshold = min_threshold
        self.max_threshold = max_threshold

    def execute(self, context: Context):
        hook = self.get_db_hook()
        result = hook.get_first(self.sql)[0]
        if not result:
            self._raise_exception(f"The following query returned zero rows: {self.sql}")

        min_threshold = _convert_to_float_if_possible(self.min_threshold)
        max_threshold = _convert_to_float_if_possible(self.max_threshold)

        if isinstance(min_threshold, float):
            lower_bound = min_threshold
        else:
            lower_bound = hook.get_first(min_threshold)[0]

        if isinstance(max_threshold, float):
            upper_bound = max_threshold
        else:
            upper_bound = hook.get_first(max_threshold)[0]

        meta_data = {
            "result": result,
            "task_id": self.task_id,
            "min_threshold": lower_bound,
            "max_threshold": upper_bound,
            "within_threshold": lower_bound <= result <= upper_bound,
        }

        self.push(meta_data)
        if not meta_data["within_threshold"]:
            result = (
                round(meta_data.get("result"), 2)  # type: ignore[arg-type]
                if meta_data.get("result") is not None
                else "<None>"
            )
            error_msg = (
                f'Threshold Check: "{meta_data.get("task_id")}" failed.\n'
                f'DAG: {self.dag_id}\nTask_id: {meta_data.get("task_id")}\n'
                f'Check description: {meta_data.get("description")}\n'
                f"SQL: {self.sql}\n"
                f"Result: {result} is not within thresholds "
                f'{meta_data.get("min_threshold")} and {meta_data.get("max_threshold")}'
            )
            self._raise_exception(error_msg)

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
        follow_task_ids_if_true: list[str],
        follow_task_ids_if_false: list[str],
        conn_id: str = "default_conn_id",
        database: str | None = None,
        parameters: Iterable | Mapping | None = None,
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
                if _parse_boolean(query_result):
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
