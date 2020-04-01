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

from typing import Any, Dict, Iterable, Optional, SupportsAbs

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CheckOperator(BaseOperator):
    """
    Performs checks against a db. The ``CheckOperator`` expects
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

    Note that this is an abstract class and get_db_hook
    needs to be defined. Whereas a get_db_hook is hook that gets a
    single record from an external source.

    :param sql: the sql to be executed. (templated)
    :type sql: str
    """

    template_fields = ('sql',)  # type: Iterable[str]
    template_ext = ('.hql', '.sql',)  # type: Iterable[str]
    ui_color = '#fff7e6'

    @apply_defaults
    def __init__(
        self,
        sql: str,
        conn_id: Optional[str] = None,
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.sql = sql

    def execute(self, context=None):
        self.log.info('Executing SQL check: %s', self.sql)
        records = self.get_db_hook().get_first(self.sql)

        self.log.info('Record: %s', records)
        if not records:
            raise AirflowException("The query returned None")
        elif not all([bool(r) for r in records]):
            raise AirflowException("Test failed.\nQuery:\n{query}\nResults:\n{records!s}".format(
                query=self.sql, records=records))

        self.log.info("Success.")

    def get_db_hook(self):
        """
        Get the database hook for the connection.

        :return: the database hook object.
        :rtype: DbApiHook
        """
        return BaseHook.get_hook(conn_id=self.conn_id)


def _convert_to_float_if_possible(s):
    """
    A small helper function to convert a string to a numeric value
    if appropriate

    :param s: the string to be converted
    :type s: str
    """
    try:
        ret = float(s)
    except (ValueError, TypeError):
        ret = s
    return ret


class ValueCheckOperator(BaseOperator):
    """
    Performs a simple value check using sql code.

    Note that this is an abstract class and get_db_hook
    needs to be defined. Whereas a get_db_hook is hook that gets a
    single record from an external source.

    :param sql: the sql to be executed. (templated)
    :type sql: str
    """

    __mapper_args__ = {
        'polymorphic_identity': 'ValueCheckOperator'
    }
    template_fields = ('sql', 'pass_value',)  # type: Iterable[str]
    template_ext = ('.hql', '.sql',)  # type: Iterable[str]
    ui_color = '#fff7e6'

    @apply_defaults
    def __init__(
        self,
        sql: str,
        pass_value: Any,
        tolerance: Any = None,
        conn_id: Optional[str] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.conn_id = conn_id
        self.pass_value = str(pass_value)
        tol = _convert_to_float_if_possible(tolerance)
        self.tol = tol if isinstance(tol, float) else None
        self.has_tolerance = self.tol is not None

    def execute(self, context=None):
        self.log.info('Executing SQL check: %s', self.sql)
        records = self.get_db_hook().get_first(self.sql)

        if not records:
            raise AirflowException("The query returned None")

        pass_value_conv = _convert_to_float_if_possible(self.pass_value)
        is_numeric_value_check = isinstance(pass_value_conv, float)

        tolerance_pct_str = str(self.tol * 100) + '%' if self.has_tolerance else None
        error_msg = ("Test failed.\nPass value:{pass_value_conv}\n"
                     "Tolerance:{tolerance_pct_str}\n"
                     "Query:\n{sql}\nResults:\n{records!s}").format(
            pass_value_conv=pass_value_conv,
            tolerance_pct_str=tolerance_pct_str,
            sql=self.sql,
            records=records
        )

        if not is_numeric_value_check:
            tests = self._get_string_matches(records, pass_value_conv)
        elif is_numeric_value_check:
            try:
                numeric_records = self._to_float(records)
            except (ValueError, TypeError):
                raise AirflowException("Converting a result to float failed.\n{}".format(error_msg))
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

    def get_db_hook(self):
        """
        Get the database hook for the connection.

        :return: the database hook object.
        :rtype: DbApiHook
        """
        return BaseHook.get_hook(conn_id=self.conn_id)


class IntervalCheckOperator(BaseOperator):
    """
    Checks that the values of metrics given as SQL expressions are within
    a certain tolerance of the ones from days_back before.

    Note that this is an abstract class and get_db_hook
    needs to be defined. Whereas a get_db_hook is hook that gets a
    single record from an external source.

    :param table: the table name
    :type table: str
    :param days_back: number of days between ds and the ds we want to check
        against. Defaults to 7 days
    :type days_back: int
    :param ratio_formula: which formula to use to compute the ratio between
        the two metrics. Assuming cur is the metric of today and ref is
        the metric to today - days_back.

        max_over_min: computes max(cur, ref) / min(cur, ref)
        relative_diff: computes abs(cur-ref) / ref

        Default: 'max_over_min'
    :type ratio_formula: str
    :param ignore_zero: whether we should ignore zero metrics
    :type ignore_zero: bool
    :param metrics_threshold: a dictionary of ratios indexed by metrics
    :type metrics_threshold: dict
    """

    __mapper_args__ = {
        'polymorphic_identity': 'IntervalCheckOperator'
    }
    template_fields = ('sql1', 'sql2')  # type: Iterable[str]
    template_ext = ('.hql', '.sql',)  # type: Iterable[str]
    ui_color = '#fff7e6'

    ratio_formulas = {
        'max_over_min': lambda cur, ref: float(max(cur, ref)) / min(cur, ref),
        'relative_diff': lambda cur, ref: float(abs(cur - ref)) / ref,
    }

    @apply_defaults
    def __init__(
        self,
        table: str,
        metrics_thresholds: Dict[str, int],
        date_filter_column: Optional[str] = 'ds',
        days_back: SupportsAbs[int] = -7,
        ratio_formula: Optional[str] = 'max_over_min',
        ignore_zero: Optional[bool] = True,
        conn_id: Optional[str] = None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        if ratio_formula not in self.ratio_formulas:
            msg_template = "Invalid diff_method: {diff_method}. " \
                           "Supported diff methods are: {diff_methods}"

            raise AirflowException(
                msg_template.format(diff_method=ratio_formula,
                                    diff_methods=self.ratio_formulas)
            )
        self.ratio_formula = ratio_formula
        self.ignore_zero = ignore_zero
        self.table = table
        self.metrics_thresholds = metrics_thresholds
        self.metrics_sorted = sorted(metrics_thresholds.keys())
        self.date_filter_column = date_filter_column
        self.days_back = -abs(days_back)
        self.conn_id = conn_id
        sqlexp = ', '.join(self.metrics_sorted)
        sqlt = "SELECT {sqlexp} FROM {table} WHERE {date_filter_column}=".format(
            sqlexp=sqlexp, table=table, date_filter_column=date_filter_column
        )

        self.sql1 = sqlt + "'{{ ds }}'"
        self.sql2 = sqlt + "'{{ macros.ds_add(ds, " + str(self.days_back) + ") }}'"

    def execute(self, context=None):
        hook = self.get_db_hook()
        self.log.info('Using ratio formula: %s', self.ratio_formula)
        self.log.info('Executing SQL check: %s', self.sql2)
        row2 = hook.get_first(self.sql2)
        self.log.info('Executing SQL check: %s', self.sql1)
        row1 = hook.get_first(self.sql1)

        if not row2:
            raise AirflowException("The query {} returned None".format(self.sql2))
        if not row1:
            raise AirflowException("The query {} returned None".format(self.sql1))

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
                ), metric, cur, metric, ref, metric, ratios[metric], threshold)

        if not all(test_results.values()):
            failed_tests = [it[0] for it in test_results.items() if not it[1]]
            self.log.warning("The following %s tests out of %s failed:",
                             len(failed_tests), len(self.metrics_sorted))
            for k in failed_tests:
                self.log.warning(
                    "'%s' check failed. %s is above %s", k, ratios[k], self.metrics_thresholds[k]
                )
            raise AirflowException("The following tests have failed:\n {0}".format(", ".join(
                sorted(failed_tests))))

        self.log.info("All tests have passed")

    def get_db_hook(self):
        """
        Get the database hook for the connection.

        :return: the database hook object.
        :rtype: DbApiHook
        """
        return BaseHook.get_hook(conn_id=self.conn_id)


class ThresholdCheckOperator(BaseOperator):
    """
    Performs a value check using sql code against a mininmum threshold
    and a maximum threshold. Thresholds can be in the form of a numeric
    value OR a sql statement that results a numeric.

    Note that this is an abstract class and get_db_hook
    needs to be defined. Whereas a get_db_hook is hook that gets a
    single record from an external source.

    :param sql: the sql to be executed. (templated)
    :type sql: str
    :param min_threshold: numerical value or min threshold sql to be executed (templated)
    :type min_threshold: numeric or str
    :param max_threshold: numerical value or max threshold sql to be executed (templated)
    :type max_threshold: numeric or str
    """

    template_fields = ('sql', 'min_threshold', 'max_threshold')  # type: Iterable[str]
    template_ext = ('.hql', '.sql',)  # type: Iterable[str]

    @apply_defaults
    def __init__(
        self,
        sql: str,
        min_threshold: Any,
        max_threshold: Any,
        conn_id: Optional[str] = None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.conn_id = conn_id
        self.min_threshold = _convert_to_float_if_possible(min_threshold)
        self.max_threshold = _convert_to_float_if_possible(max_threshold)

    def execute(self, context=None):
        hook = self.get_db_hook()
        result = hook.get_first(self.sql)[0][0]

        if isinstance(self.min_threshold, float):
            lower_bound = self.min_threshold
        else:
            lower_bound = hook.get_first(self.min_threshold)[0][0]

        if isinstance(self.max_threshold, float):
            upper_bound = self.max_threshold
        else:
            upper_bound = hook.get_first(self.max_threshold)[0][0]

        meta_data = {
            "result": result,
            "task_id": self.task_id,
            "min_threshold": lower_bound,
            "max_threshold": upper_bound,
            "within_threshold": lower_bound <= result <= upper_bound
        }

        self.push(meta_data)
        if not meta_data["within_threshold"]:
            error_msg = (f'Threshold Check: "{meta_data.get("task_id")}" failed.\n'
                         f'DAG: {self.dag_id}\nTask_id: {meta_data.get("task_id")}\n'
                         f'Check description: {meta_data.get("description")}\n'
                         f'SQL: {self.sql}\n'
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

        info = "\n".join([f"""{key}: {item}""" for key, item in meta_data.items()])
        self.log.info("Log from %s:\n%s", self.dag_id, info)

    def get_db_hook(self):
        """
        Returns DB hook
        """
        return BaseHook.get_hook(conn_id=self.conn_id)
