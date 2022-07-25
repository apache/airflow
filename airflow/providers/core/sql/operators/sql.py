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
from typing import Any, Dict, Optional, Union

from airflow.exceptions import AirflowException
from airflow.operators.sql import BaseSQLOperator


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


def _get_failed_tests(checks):
    return [
        f"\tCheck: {check},\n\tCheck Values: {check_values}\n"
        for check, check_values in checks.items()
        if not check_values["success"]
    ]


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

    :param table: the table to run checks on
    :param column_mapping: the dictionary of columns and their associated checks, e.g.

    .. code-block:: python

        {
            "col_name": {
                "null_check": {
                    "equal_to": 0,
                },
                "min": {
                    "greater_than": 5,
                    "leq_to": 10,
                    "tolerance": 0.2,
                },
                "max": {"less_than": 1000, "geq_to": 10, "tolerance": 0.01},
            }
        }

    :param conn_id: the connection ID used to connect to the database
    :param database: name of database which overwrite the defined one in connection

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SQLColumnCheckOperator`
    """

    column_checks = {
        "null_check": "SUM(CASE WHEN column IS NULL THEN 1 ELSE 0 END) AS column_null_check",
        "distinct_check": "COUNT(DISTINCT(column)) AS column_distinct_check",
        "unique_check": "COUNT(column) - COUNT(DISTINCT(column)) AS column_unique_check",
        "min": "MIN(column) AS column_min",
        "max": "MAX(column) AS column_max",
    }

    def __init__(
        self,
        *,
        table: str,
        column_mapping: Dict[str, Dict[str, Any]],
        conn_id: Optional[str] = None,
        database: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(conn_id=conn_id, database=database, **kwargs)
        for checks in column_mapping.values():
            for check, check_values in checks.items():
                self._column_mapping_validation(check, check_values)

        self.table = table
        self.column_mapping = column_mapping
        # OpenLineage needs a valid SQL query with the input/output table(s) to parse
        self.sql = f"SELECT * FROM {self.table};"

    def execute(self, context=None):
        hook = self.get_db_hook()
        failed_tests = []
        for column in self.column_mapping:
            checks = [*self.column_mapping[column]]
            checks_sql = ",".join([self.column_checks[check].replace("column", column) for check in checks])

            self.sql = f"SELECT {checks_sql} FROM {self.table};"
            records = hook.get_first(self.sql)

            if not records:
                raise AirflowException(f"The following query returned zero rows: {self.sql}")

            self.log.info(f"Record: {records}")

            for idx, result in enumerate(records):
                tolerance = self.column_mapping[column][checks[idx]].get("tolerance")

                self.column_mapping[column][checks[idx]]["result"] = result
                self.column_mapping[column][checks[idx]]["success"] = self._get_match(
                    self.column_mapping[column][checks[idx]], result, tolerance
                )

            failed_tests.extend(_get_failed_tests(self.column_mapping[column]))
        if failed_tests:
            raise AirflowException(
                f"Test failed.\nQuery:\n{self.sql}\nResults:\n{records!s}\n"
                "The following tests have failed:"
                f"\n{''.join(failed_tests)}"
            )

        self.log.info("All tests have passed")

    def _get_match(self, check_values, record, tolerance=None) -> bool:
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
    :param checks: the dictionary of checks, e.g.:

    .. code-block:: python

        {
            "row_count_check": {"check_statement": "COUNT(*) = 1000"},
            "column_sum_check": {"check_statement": "col_a + col_b < col_c"},
        }

    :param conn_id: the connection ID used to connect to the database
    :param database: name of database which overwrite the defined one in connection

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SQLTableCheckOperator`
    """

    sql_check_template = "CASE WHEN check_statement THEN 1 ELSE 0 END AS check_name"
    sql_min_template = "MIN(check_name)"

    def __init__(
        self,
        *,
        table: str,
        checks: Dict[str, Dict[str, Any]],
        conn_id: Optional[str] = None,
        database: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(conn_id=conn_id, database=database, **kwargs)

        self.table = table
        self.checks = checks
        # OpenLineage needs a valid SQL query with the input/output table(s) to parse
        self.sql = f"SELECT * FROM {self.table};"

    def execute(self, context=None):
        hook = self.get_db_hook()

        check_names = [*self.checks]
        check_mins_sql = ",".join(
            self.sql_min_template.replace("check_name", check_name) for check_name in check_names
        )
        checks_sql = ",".join(
            [
                self.sql_check_template.replace("check_statement", value["check_statement"]).replace(
                    "check_name", check_name
                )
                for check_name, value in self.checks.items()
            ]
        )

        self.sql = f"SELECT {check_mins_sql} FROM (SELECT {checks_sql} FROM {self.table});"
        records = hook.get_first(self.sql)

        if not records:
            raise AirflowException(f"The following query returned zero rows: {self.sql}")

        self.log.info(f"Record: {records}")

        for check in self.checks.keys():
            for result in records:
                self.checks[check]["success"] = parse_boolean(str(result))

        failed_tests = _get_failed_tests(self.checks)
        if failed_tests:
            raise AirflowException(
                f"Test failed.\nQuery:\n{self.sql}\nResults:\n{records!s}\n"
                "The following tests have failed:"
                f"\n{', '.join(failed_tests)}"
            )

        self.log.info("All tests have passed")
