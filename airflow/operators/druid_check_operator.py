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

from airflow.exceptions import AirflowException
from airflow.hooks.druid_hook import DruidDbApiHook
from airflow.operators.check_operator import CheckOperator
from airflow.utils.decorators import apply_defaults


class DruidCheckOperator(CheckOperator):
    """
    Performs checks against Druid. The ``DruidCheckOperator`` expects
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
    :param druid_broker_conn_id: reference to the druid broker
    :type druid_broker_conn_id: str
    """

    @apply_defaults
    def __init__(
            self,
            sql: str,
            druid_broker_conn_id: str = 'druid_broker_default',
            *args, **kwargs) -> None:
        super().__init__(sql=sql, *args, **kwargs)
        self.druid_broker_conn_id = druid_broker_conn_id
        self.sql = sql

    def get_db_hook(self):
        """
        Return the druid db api hook.
        """
        return DruidDbApiHook(druid_broker_conn_id=self.druid_broker_conn_id)

    def get_first(self, sql):
        """
        Executes the druid sql to druid broker and returns the first resulting row.

        :param sql: the sql statement to be executed (str)
        :type sql: str
        """
        with self.get_db_hook().get_conn() as cur:
            cur.execute(sql)
            return cur.fetchone()

    def execute(self, context=None):
        self.log.info('Executing SQL check: %s', self.sql)
        record = self.get_first(self.sql)
        self.log.info("Record: %s", str(record))
        if not record:
            raise AirflowException("The query returned None")
        self.log.info("Success.")
