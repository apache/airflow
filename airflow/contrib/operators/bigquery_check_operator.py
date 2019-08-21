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
"""
This module contains Google BigQuery check operator.
"""
import warnings

from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.operators.check_operator import \
    CheckOperator, ValueCheckOperator, IntervalCheckOperator
from airflow.utils.decorators import apply_defaults


class BigQueryCheckOperator(CheckOperator):
    """
    Performs checks against BigQuery. The ``BigQueryCheckOperator`` expects
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
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param bigquery_conn_id: (Deprecated) The connection ID used to connect to Google Cloud Platform.
        This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :type bigquery_conn_id: str
    :param use_legacy_sql: Whether to use legacy SQL (true)
        or standard SQL (false).
    :type use_legacy_sql: bool
    """

    template_fields = ('sql', 'gcp_conn_id', )
    template_ext = ('.sql', )

    @apply_defaults
    def __init__(self,
                 sql,
                 gcp_conn_id='google_cloud_default',
                 bigquery_conn_id=None,
                 use_legacy_sql=True,
                 *args, **kwargs):
        super().__init__(sql=sql, *args, **kwargs)
        if not bigquery_conn_id:
            warnings.warn(
                "The bigquery_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.", DeprecationWarning, stacklevel=3)
            gcp_conn_id = bigquery_conn_id

        self.gcp_conn_id = gcp_conn_id
        self.sql = sql
        self.use_legacy_sql = use_legacy_sql

    def get_db_hook(self):
        return BigQueryHook(bigquery_conn_id=self.gcp_conn_id,
                            use_legacy_sql=self.use_legacy_sql)


class BigQueryValueCheckOperator(ValueCheckOperator):
    """
    Performs a simple value check using sql code.

    :param sql: the sql to be executed
    :type sql: str
    :param use_legacy_sql: Whether to use legacy SQL (true)
        or standard SQL (false).
    :type use_legacy_sql: bool
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param bigquery_conn_id: (Deprecated) The connection ID used to connect to Google Cloud Platform.
        This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :type bigquery_conn_id: str
    """

    template_fields = ('sql', 'gcp_conn_id', 'pass_value', )
    template_ext = ('.sql', )

    @apply_defaults
    def __init__(self, sql,
                 pass_value,
                 tolerance=None,
                 gcp_conn_id='google_cloud_default',
                 bigquery_conn_id=None,
                 use_legacy_sql=True,
                 *args, **kwargs):
        super().__init__(
            sql=sql, pass_value=pass_value, tolerance=tolerance,
            *args, **kwargs)

        if bigquery_conn_id:
            warnings.warn(
                "The bigquery_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.", DeprecationWarning, stacklevel=3)
            gcp_conn_id = bigquery_conn_id

        self.gcp_conn_id = gcp_conn_id
        self.use_legacy_sql = use_legacy_sql

    def get_db_hook(self):
        return BigQueryHook(bigquery_conn_id=self.gcp_conn_id,
                            use_legacy_sql=self.use_legacy_sql)


class BigQueryIntervalCheckOperator(IntervalCheckOperator):
    """
    Checks that the values of metrics given as SQL expressions are within
    a certain tolerance of the ones from days_back before.

    This method constructs a query like so ::

        SELECT {metrics_threshold_dict_key} FROM {table}
        WHERE {date_filter_column}=<date>

    :param table: the table name
    :type table: str
    :param days_back: number of days between ds and the ds we want to check
        against. Defaults to 7 days
    :type days_back: int
    :param metrics_threshold: a dictionary of ratios indexed by metrics, for
        example 'COUNT(*)': 1.5 would require a 50 percent or less difference
        between the current day, and the prior days_back.
    :type metrics_threshold: dict
    :param use_legacy_sql: Whether to use legacy SQL (true)
        or standard SQL (false).
    :type use_legacy_sql: bool
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param bigquery_conn_id: (Deprecated) The connection ID used to connect to Google Cloud Platform.
        This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :type bigquery_conn_id: str
    """

    template_fields = ('table', 'gcp_conn_id', )

    @apply_defaults
    def __init__(self,
                 table,
                 metrics_thresholds,
                 date_filter_column='ds',
                 days_back=-7,
                 gcp_conn_id='google_cloud_default',
                 bigquery_conn_id=None,
                 use_legacy_sql=True, *args, **kwargs):
        super().__init__(
            table=table, metrics_thresholds=metrics_thresholds,
            date_filter_column=date_filter_column, days_back=days_back,
            *args, **kwargs)

        if bigquery_conn_id:
            warnings.warn(
                "The bigquery_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.", DeprecationWarning, stacklevel=3)
            gcp_conn_id = bigquery_conn_id

        self.gcp_conn_id = gcp_conn_id
        self.use_legacy_sql = use_legacy_sql

    def get_db_hook(self):
        return BigQueryHook(bigquery_conn_id=self.gcp_conn_id,
                            use_legacy_sql=self.use_legacy_sql)
