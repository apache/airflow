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

import logging

from airflow.contrib.hooks.vertica_hook import VerticaHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from contextlib import closing

import unicodecsv as csv
from tempfile import NamedTemporaryFile


class VerticaToMySqlTransfer(BaseOperator):
    """
    Moves data from Vertica to MySQL.

    :param sql: SQL query to execute against the Vertica database
    :type sql: str
    :param vertica_conn_id: source Vertica connection
    :type vertica_conn_id: str
    :param mysql_table: target MySQL table, use dot notation to target a
        specific database
    :type mysql_table: str
    :param mysql_conn_id: source mysql connection
    :type mysql_conn_id: str
    :param mysql_preoperator: sql statement to run against MySQL prior to
        import, typically use to truncate of delete in place of the data
        coming in, allowing the task to be idempotent (running the task
        twice won't double load data)
    :type mysql_preoperator: str
    :param mysql_postoperator: sql statement to run against MySQL after the
        import, typically used to move data from staging to production
        and issue cleanup commands.
    :type mysql_postoperator: str
    :param bulk_load: flag to use bulk_load option.  This loads MySQL directly
        from a tab-delimited text file using the LOAD DATA LOCAL INFILE command.
        This option requires an extra connection parameter for the
        destination MySQL connection: {'local_infile': true}.
    :type bulk_load: bool
    """

    template_fields = ('sql', 'mysql_table', 'mysql_preoperator',
        'mysql_postoperator')
    template_ext = ('.sql',)
    ui_color = '#a0e08c'

    @apply_defaults
    def __init__(
            self,
            sql,
            mysql_table,
            vertica_conn_id='vertica_default',
            mysql_conn_id='mysql_default',
            mysql_preoperator=None,
            mysql_postoperator=None,
            bulk_load=False,
            *args, **kwargs):
        super(VerticaToMySqlTransfer, self).__init__(*args, **kwargs)
        self.sql = sql
        self.mysql_table = mysql_table
        self.mysql_conn_id = mysql_conn_id
        self.mysql_preoperator = mysql_preoperator
        self.mysql_postoperator = mysql_postoperator
        self.vertica_conn_id = vertica_conn_id
        self.bulk_load = bulk_load

    def execute(self, context):
        vertica = VerticaHook(vertica_conn_id=self.vertica_conn_id)
        mysql = MySqlHook(mysql_conn_id=self.mysql_conn_id)

        tmpfile = None
        result = None

        selected_columns = []

        count = 0
        with closing(vertica.get_conn()) as conn:
            with closing(conn.cursor()) as cursor:
                cursor.execute(self.sql)
                selected_columns = [d.name for d in cursor.description]

                if self.bulk_load:
                    tmpfile = NamedTemporaryFile("w")

                    logging.info("Selecting rows from Vertica to local file " + str(tmpfile.name) + "...")
                    logging.info(self.sql)

                    csv_writer = csv.writer(tmpfile, delimiter='\t', encoding='utf-8')
                    for row in cursor.iterate():
                        csv_writer.writerow(row)
                        count += 1

                    tmpfile.flush()
                else:
                    logging.info("Selecting rows from Vertica...")
                    logging.info(self.sql)

                    result = cursor.fetchall()
                    count = len(result)

                logging.info("Selected rows from Vertica " + str(count))

        if self.mysql_preoperator:
            logging.info("Running MySQL preoperator...")
            mysql.run(self.mysql_preoperator)

        try:
            if self.bulk_load:
                logging.info("Bulk inserting rows into MySQL...")
                with closing(mysql.get_conn()) as conn:
                    with closing(conn.cursor()) as cursor:
                        cursor.execute("LOAD DATA LOCAL INFILE '%s' INTO TABLE %s LINES TERMINATED BY '\r\n' (%s)" % (tmpfile.name, self.mysql_table, ", ".join(selected_columns)))
                        conn.commit()
                tmpfile.close()
            else:
                logging.info("Inserting rows into MySQL...")
                mysql.insert_rows(table=self.mysql_table, rows=result, target_fields=selected_columns)
            logging.info("Inserted rows into MySQL " + str(count))
        except:
            logging.error("Inserted rows into MySQL 0")
            raise

        if self.mysql_postoperator:
            logging.info("Running MySQL postoperator...")
            mysql.run(self.mysql_postoperator)

        logging.info("Done")
