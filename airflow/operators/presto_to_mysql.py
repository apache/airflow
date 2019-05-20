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

from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.presto_hook import PrestoHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class PrestoToMySqlTransfer(BaseOperator):
    """
    Moves data from Presto to MySQL, note that for now the data is loaded
    into memory before being pushed to MySQL, so this operator should
    be used for smallish amount of data.

    :param sql: SQL query to execute against Presto. (templated)
    :type sql: str
    :param mysql_table: target MySQL table, use dot notation to target a
        specific database. (templated)
    :type mysql_table: str
    :param mysql_conn_id: source mysql connection
    :type mysql_conn_id: str
    :param presto_conn_id: source presto connection
    :type presto_conn_id: str
    :param mysql_preoperator: sql statement to run against mysql prior to
        import, typically use to truncate of delete in place
        of the data coming in, allowing the task to be idempotent (running
        the task twice won't double load data). (templated)
    :type mysql_preoperator: str
    """

    template_fields = ('sql', 'mysql_table', 'mysql_preoperator')
    template_ext = ('.sql',)
    ui_color = '#a0e08c'

    @apply_defaults
    def __init__(self,
                 sql,
                 mysql_table,
                 presto_conn_id='presto_default',
                 mysql_conn_id='mysql_default',
                 mysql_preoperator=None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.mysql_table = mysql_table
        self.mysql_conn_id = mysql_conn_id
        self.mysql_preoperator = mysql_preoperator
        self.presto_conn_id = presto_conn_id

    def execute(self, context):
        presto = PrestoHook(presto_conn_id=self.presto_conn_id)
        self.log.info("Extracting data from Presto: %s", self.sql)
        results = presto.get_records(self.sql)

        mysql = MySqlHook(mysql_conn_id=self.mysql_conn_id)
        if self.mysql_preoperator:
            self.log.info("Running MySQL preoperator")
            self.log.info(self.mysql_preoperator)
            mysql.run(self.mysql_preoperator)

        self.log.info("Inserting rows into MySQL")
        mysql.insert_rows(table=self.mysql_table, rows=results)
