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

from contextlib import closing
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Sequence

import MySQLdb
import unicodecsv as csv

from airflow.models import BaseOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.vertica.hooks.vertica import VerticaHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class VerticaToMySqlOperator(BaseOperator):
    """
    Moves data from Vertica to MySQL.

    :param sql: SQL query to execute against the Vertica database. (templated)
    :param vertica_conn_id: source Vertica connection
    :param mysql_table: target MySQL table, use dot notation to target a
        specific database. (templated)
    :param mysql_conn_id: Reference to :ref:`mysql connection id <howto/connection:mysql>`.
    :param mysql_preoperator: sql statement to run against MySQL prior to
        import, typically use to truncate of delete in place of the data
        coming in, allowing the task to be idempotent (running the task
        twice won't double load data). (templated)
    :param mysql_postoperator: sql statement to run against MySQL after the
        import, typically used to move data from staging to production
        and issue cleanup commands. (templated)
    :param bulk_load: flag to use bulk_load option.  This loads MySQL directly
        from a tab-delimited text file using the LOAD DATA LOCAL INFILE command.
        This option requires an extra connection parameter for the
        destination MySQL connection: {'local_infile': true}.
    """

    template_fields: Sequence[str] = ("sql", "mysql_table", "mysql_preoperator", "mysql_postoperator")
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {
        "sql": "sql",
        "mysql_preoperator": "mysql",
        "mysql_postoperator": "mysql",
    }
    ui_color = "#a0e08c"

    def __init__(
        self,
        sql: str,
        mysql_table: str,
        vertica_conn_id: str = "vertica_default",
        mysql_conn_id: str = "mysql_default",
        mysql_preoperator: str | None = None,
        mysql_postoperator: str | None = None,
        bulk_load: bool = False,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.mysql_table = mysql_table
        self.mysql_conn_id = mysql_conn_id
        self.mysql_preoperator = mysql_preoperator
        self.mysql_postoperator = mysql_postoperator
        self.vertica_conn_id = vertica_conn_id
        self.bulk_load = bulk_load

    def execute(self, context: Context):
        vertica = VerticaHook(vertica_conn_id=self.vertica_conn_id)
        mysql = MySqlHook(mysql_conn_id=self.mysql_conn_id)

        if self.bulk_load:
            self._bulk_load_transfer(mysql, vertica)
        else:
            self._non_bulk_load_transfer(mysql, vertica)

        if self.mysql_postoperator:
            self.log.info("Running MySQL postoperator...")
            mysql.run(self.mysql_postoperator)

        self.log.info("Done")

    def _non_bulk_load_transfer(self, mysql, vertica):
        with closing(vertica.get_conn()) as conn:
            with closing(conn.cursor()) as cursor:
                cursor.execute(self.sql)
                selected_columns = [d.name for d in cursor.description]
                self.log.info("Selecting rows from Vertica...")
                self.log.info(self.sql)

                result = cursor.fetchall()
                count = len(result)

                self.log.info("Selected rows from Vertica %s", count)
        self._run_preoperator(mysql)
        try:
            self.log.info("Inserting rows into MySQL...")
            mysql.insert_rows(table=self.mysql_table, rows=result, target_fields=selected_columns)
            self.log.info("Inserted rows into MySQL %s", count)
        except (MySQLdb.Error, MySQLdb.Warning):
            self.log.info("Inserted rows into MySQL 0")
            raise

    def _bulk_load_transfer(self, mysql, vertica):
        count = 0
        with closing(vertica.get_conn()) as conn:
            with closing(conn.cursor()) as cursor:
                cursor.execute(self.sql)
                selected_columns = [d.name for d in cursor.description]
                with NamedTemporaryFile("w") as tmpfile:
                    self.log.info("Selecting rows from Vertica to local file %s...", tmpfile.name)
                    self.log.info(self.sql)

                    csv_writer = csv.writer(tmpfile, delimiter="\t", encoding="utf-8")
                    for row in cursor.iterate():
                        csv_writer.writerow(row)
                        count += 1

                    tmpfile.flush()
        self._run_preoperator(mysql)
        try:
            self.log.info("Bulk inserting rows into MySQL...")
            with closing(mysql.get_conn()) as conn:
                with closing(conn.cursor()) as cursor:
                    cursor.execute(
                        f"LOAD DATA LOCAL INFILE '{tmpfile.name}' "
                        f"INTO TABLE {self.mysql_table} "
                        f"LINES TERMINATED BY '\r\n' ({', '.join(selected_columns)})"
                    )
                    conn.commit()
            tmpfile.close()
            self.log.info("Inserted rows into MySQL %s", count)
        except (MySQLdb.Error, MySQLdb.Warning):
            self.log.info("Inserted rows into MySQL 0")
            raise

    def _run_preoperator(self, mysql):
        if self.mysql_preoperator:
            self.log.info("Running MySQL preoperator...")
            mysql.run(self.mysql_preoperator)
