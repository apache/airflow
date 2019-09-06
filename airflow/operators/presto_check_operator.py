# -*- coding: utf-8 -*-
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
from typing import Any, Dict

from airflow.hooks.presto_hook import PrestoHook
from airflow.operators.check_operator import CheckOperator, \
    ValueCheckOperator, IntervalCheckOperator
from airflow.utils.decorators import apply_defaults


class PrestoCheckOperator(CheckOperator):
    """
    Performs checks against Presto. The ``PrestoCheckOperator`` expects
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

    :param sql: the sql to be executed
    :type sql: str
    :param presto_conn_id: reference to the Presto database
    :type presto_conn_id: str
    """

    @apply_defaults
    def __init__(
            self,
            sql: str,
            presto_conn_id: str = 'presto_default',
            *args, **kwargs) -> None:
        super().__init__(sql=sql, *args, **kwargs)

        self.presto_conn_id = presto_conn_id
        self.sql = sql

    def get_db_hook(self):
        return PrestoHook(presto_conn_id=self.presto_conn_id)


class PrestoValueCheckOperator(ValueCheckOperator):
    """
    Performs a simple value check using sql code.

    :param sql: the sql to be executed
    :type sql: str
    :param presto_conn_id: reference to the Presto database
    :type presto_conn_id: str
    """

    @apply_defaults
    def __init__(
            self,
            sql: str,
            pass_value: Any,
            tolerance: Any = None,
            presto_conn_id: str = 'presto_default',
            *args, **kwargs):
        super().__init__(
            sql=sql, pass_value=pass_value, tolerance=tolerance,
            *args, **kwargs)
        self.presto_conn_id = presto_conn_id

    def get_db_hook(self):
        return PrestoHook(presto_conn_id=self.presto_conn_id)


class PrestoIntervalCheckOperator(IntervalCheckOperator):
    """
    Checks that the values of metrics given as SQL expressions are within
    a certain tolerance of the ones from days_back before.

    :param table: the table name
    :type table: str
    :param days_back: number of days between ds and the ds we want to check
        against. Defaults to 7 days
    :type days_back: int
    :param metrics_threshold: a dictionary of ratios indexed by metrics
    :type metrics_threshold: dict
    :param presto_conn_id: reference to the Presto database
    :type presto_conn_id: str
    """

    @apply_defaults
    def __init__(
            self,
            table: str,
            metrics_thresholds: Dict,
            date_filter_column: str = 'ds',
            days_back: int = -7,
            presto_conn_id: str = 'presto_default',
            *args, **kwargs):
        super().__init__(
            table=table, metrics_thresholds=metrics_thresholds,
            date_filter_column=date_filter_column, days_back=days_back,
            *args, **kwargs)
        self.presto_conn_id = presto_conn_id

    def get_db_hook(self):
        return PrestoHook(presto_conn_id=self.presto_conn_id)
