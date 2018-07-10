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
from airflow.hooks.jdbc_hook import JdbcHook
from airflow.operators.jdbc_operator import JdbcOperator
from airflow.utils.decorators import apply_defaults


class JdbcGetFirstOperator(JdbcOperator):
    """
    Executes sql code in a database using jdbc driver.
    Requires jaydebeapi.

    Intended for queries which select row(s).
    Fetches just the first row of data from the database.
    Row value is both logged and xcom'd (by default)

    :param print_row: specifies whether row should be printed to the logs.
    :type print_row: boolean
    :default print_row: True

    :param xcom_push: specifies whether row is xcom'd with key 'return_value'
    :type xcom_push: boolean
    :default xcom_push: True
    """
    @apply_defaults
    def __init__(
            self,
            print_row=True,
            xcom_push=True,
            *args,
            **kwargs):
        super(JdbcGetFirstOperator, self).__init__(*args, **kwargs)
        self.print_row = print_row
        self.xcom_push = xcom_push

    def execute(self, context):
        self.log.info('Executing: %s', self.sql)
        self.hook = JdbcHook(jdbc_conn_id=self.jdbc_conn_id)
        row = self.hook.get_first(self.sql, parameters=self.parameters)
        if self.print_row:
            self.log.info('Row: ' + str(row))
        if self.xcom_push:
            return row
