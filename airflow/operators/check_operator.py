# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from builtins import zip
from builtins import str
import logging

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

    :param sql: the sql to be executed
    :type sql: string
    """

    template_fields = ('sql',)
    template_ext = ('.hql', '.sql',)
    ui_color = '#fff7e6'

    @apply_defaults
    def __init__(
            self, sql,
            conn_id=None,
            *args, **kwargs):
        super(CheckOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.sql = sql

    def execute(self, context=None):
        logging.info('Executing SQL check: ' + self.sql)
        records = self.get_db_hook().get_first(self.sql)
        logging.info("Record: " + str(records))
        if not records:
            raise AirflowException("The query returned None")
        elif not all([bool(r) for r in records]):
            exceptstr = "Test failed.\nQuery:\n{q}\nResults:\n{r!s}"
            raise AirflowException(exceptstr.format(q=self.sql, r=records))
        logging.info("Success.")

    def get_db_hook(self):
        return BaseHook.get_hook(conn_id=self.conn_id)


def _convert_to_float_if_possible(s):
    '''
    A small helper function to convert a string to a numeric value
    if appropriate

    :param s: the string to be converted
    :type s: str
    '''
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

    :param sql: the sql to be executed
    :type sql: string
    """

    __mapper_args__ = {
        'polymorphic_identity': 'ValueCheckOperator'
    }
    template_fields = ('sql',)
    template_ext = ('.hql', '.sql',)
    ui_color = '#fff7e6'

    @apply_defaults
    def __init__(
            self, sql, pass_value, tolerance=None,
            conn_id=None,
            *args, **kwargs):
        super(ValueCheckOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.conn_id = conn_id
        self.pass_value = _convert_to_float_if_possible(pass_value)
        tol = _convert_to_float_if_possible(tolerance)
        self.tol = tol if isinstance(tol, float) else None
        self.is_numeric_value_check = isinstance(self.pass_value, float)
        self.has_tolerance = self.tol is not None

    def execute(self, context=None):
        logging.info('Executing SQL check: ' + self.sql)
        records = self.get_db_hook().get_first(self.sql)
        if not records:
            raise AirflowException("The query returned None")
        test_results = []
        except_temp = ("Test failed.\nPass value:{self.pass_value}\n"
                       "Query:\n{self.sql}\nResults:\n{records!s}")
        if not self.is_numeric_value_check:
            tests = [str(r) == self.pass_value for r in records]
        elif self.is_numeric_value_check:
            try:
                num_rec = [float(r) for r in records]
            except (ValueError, TypeError) as e:
                cvestr = "Converting a result to float failed.\n"
                raise AirflowException(cvestr+except_temp.format(**locals()))
            if self.has_tolerance:
                tests = [
                    r / (1 + self.tol) <= self.pass_value <= r / (1 - self.tol)
                    for r in num_rec]
            else:
                tests = [r == self.pass_value for r in num_rec]
        if not all(tests):
            raise AirflowException(except_temp.format(**locals()))

    def get_db_hook(self):
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
    :param metrics_threshold: a dictionary of ratios indexed by metrics
    :type metrics_threshold: dict
    """

    __mapper_args__ = {
        'polymorphic_identity': 'IntervalCheckOperator'
    }
    template_fields = ('sql1', 'sql2')
    template_ext = ('.hql', '.sql',)
    ui_color = '#fff7e6'

    @apply_defaults
    def __init__(
            self, table, metrics_thresholds,
            date_filter_column='ds', days_back=-7,
            conn_id=None,
            *args, **kwargs):
        super(IntervalCheckOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.metrics_thresholds = metrics_thresholds
        self.metrics_sorted = sorted(metrics_thresholds.keys())
        self.date_filter_column = date_filter_column
        self.days_back = -abs(days_back)
        self.conn_id = conn_id
        sqlexp = ', '.join(self.metrics_sorted)
        sqlt = ("SELECT {sqlexp} FROM {table}"
                " WHERE {date_filter_column}=").format(**locals())
        self.sql1 = sqlt + "'{{ ds }}'"
        self.sql2 = sqlt + "'{{ macros.ds_add(ds, "+str(self.days_back)+") }}'"

    def execute(self, context=None):
        hook = self.get_db_hook()
        logging.info('Executing SQL check: ' + self.sql2)
        row2 = hook.get_first(self.sql2)
        logging.info('Executing SQL check: ' + self.sql1)
        row1 = hook.get_first(self.sql1)
        if not row2:
            raise AirflowException("The query {q} returned None".format(q=self.sql2))
        if not row1:
            raise AirflowException("The query {q} returned None".format(q=self.sql1))
        current = dict(zip(self.metrics_sorted, row1))
        reference = dict(zip(self.metrics_sorted, row2))
        ratios = {}
        test_results = {}
        rlog = "Ratio for {0}: {1} \n Ratio threshold : {2}"
        fstr = "'{k}' check failed. {r} is above {tr}"
        estr = "The following tests have failed:\n {0}"
        countstr = "The following {j} tests out of {n} failed:"
        for m in self.metrics_sorted:
            if current[m] == 0 or reference[m] == 0:
                ratio = None
            else:
                ratio = float(max(current[m], reference[m])) / \
                    min(current[m], reference[m])
            logging.info(rlog.format(m, ratio, self.metrics_thresholds[m]))
            ratios[m] = ratio
            test_results[m] = ratio < self.metrics_thresholds[m]
        if not all(test_results.values()):
            failed_tests = [it[0] for it in test_results.items() if not it[1]]
            j = len(failed_tests)
            n = len(self.metrics_sorted)
            logging.warning(countstr.format(**locals()))
            for k in failed_tests:
                logging.warning(fstr.format(k=k, r=ratios[k],
                                tr=self.metrics_thresholds[k]))
            raise AirflowException(estr.format(", ".join(failed_tests)))
        logging.info("All tests have passed")

    def get_db_hook(self):
        return BaseHook.get_hook(conn_id=self.conn_id)
