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

import unittest
from collections import OrderedDict
from unittest import mock

import pytest

from airflow.models.dag import DAG
from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook
from airflow.providers.apache.hive.operators.mysql_to_hive import MySqlToHiveTransferOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]
TEST_DAG_ID = 'unit_test_dag'


@pytest.mark.backend("mysql")
class TestTransfer(unittest.TestCase):
    def setUp(self):
        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        dag = DAG(TEST_DAG_ID, default_args=args)
        self.dag = dag

        rows = [
            (1880, "John", 0.081541, "boy"),
            (1880, "William", 0.080511, "boy"),
            (1880, "James", 0.050057, "boy"),
            (1880, "Charles", 0.045167, "boy"),
            (1880, "George", 0.043292, "boy"),
            (1880, "Frank", 0.02738, "boy"),
            (1880, "Joseph", 0.022229, "boy"),
            (1880, "Thomas", 0.021401, "boy"),
            (1880, "Henry", 0.020641, "boy"),
            (1880, "Robert", 0.020404, "boy"),
            (1880, "Edward", 0.019965, "boy"),
            (1880, "Harry", 0.018175, "boy"),
            (1880, "Walter", 0.014822, "boy"),
            (1880, "Arthur", 0.013504, "boy"),
            (1880, "Fred", 0.013251, "boy"),
            (1880, "Albert", 0.012609, "boy"),
            (1880, "Samuel", 0.008648, "boy"),
            (1880, "David", 0.007339, "boy"),
            (1880, "Louis", 0.006993, "boy"),
            (1880, "Joe", 0.006174, "boy"),
            (1880, "Charlie", 0.006165, "boy"),
            (1880, "Clarence", 0.006165, "boy"),
            (1880, "Richard", 0.006148, "boy"),
            (1880, "Andrew", 0.005439, "boy"),
            (1880, "Daniel", 0.00543, "boy"),
            (1880, "Ernest", 0.005194, "boy"),
            (1880, "Will", 0.004966, "boy"),
            (1880, "Jesse", 0.004805, "boy"),
            (1880, "Oscar", 0.004594, "boy"),
            (1880, "Lewis", 0.004366, "boy"),
            (1880, "Peter", 0.004189, "boy"),
            (1880, "Benjamin", 0.004138, "boy"),
            (1880, "Frederick", 0.004079, "boy"),
            (1880, "Willie", 0.00402, "boy"),
            (1880, "Alfred", 0.003961, "boy"),
            (1880, "Sam", 0.00386, "boy"),
            (1880, "Roy", 0.003716, "boy"),
            (1880, "Herbert", 0.003581, "boy"),
            (1880, "Jacob", 0.003412, "boy"),
            (1880, "Tom", 0.00337, "boy"),
            (1880, "Elmer", 0.00315, "boy"),
            (1880, "Carl", 0.003142, "boy"),
            (1880, "Lee", 0.003049, "boy"),
            (1880, "Howard", 0.003015, "boy"),
            (1880, "Martin", 0.003015, "boy"),
            (1880, "Michael", 0.00299, "boy"),
            (1880, "Bert", 0.002939, "boy"),
            (1880, "Herman", 0.002931, "boy"),
            (1880, "Jim", 0.002914, "boy"),
            (1880, "Francis", 0.002905, "boy"),
            (1880, "Harvey", 0.002905, "boy"),
            (1880, "Earl", 0.002829, "boy"),
            (1880, "Eugene", 0.00277, "boy"),
        ]

        with MySqlHook().get_conn() as cur:
            cur.execute('''
            CREATE TABLE IF NOT EXISTS baby_names (
              org_year integer(4),
              baby_name VARCHAR(25),
              rate FLOAT(7,6),
              sex VARCHAR(4)
            )
            ''')

        for row in rows:
            cur.execute("INSERT INTO baby_names VALUES(%s, %s, %s, %s);", row)

    def tearDown(self):
        with MySqlHook().get_conn() as cur:
            cur.execute("DROP TABLE IF EXISTS baby_names CASCADE;")

    def test_mysql_to_hive(self):
        sql = "SELECT * FROM baby_names LIMIT 1000;"
        op = MySqlToHiveTransferOperator(
            task_id='test_m2h',
            hive_cli_conn_id='hive_cli_default',
            sql=sql,
            hive_table='test_mysql_to_hive',
            recreate=True,
            delimiter=",",
            dag=self.dag)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_mysql_to_hive_partition(self):
        sql = "SELECT * FROM baby_names LIMIT 1000;"
        op = MySqlToHiveTransferOperator(
            task_id='test_m2h',
            hive_cli_conn_id='hive_cli_default',
            sql=sql,
            hive_table='test_mysql_to_hive_part',
            partition={'ds': DEFAULT_DATE_DS},
            recreate=False,
            create=True,
            delimiter=",",
            dag=self.dag)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_mysql_to_hive_tblproperties(self):
        sql = "SELECT * FROM baby_names LIMIT 1000;"
        op = MySqlToHiveTransferOperator(
            task_id='test_m2h',
            hive_cli_conn_id='hive_cli_default',
            sql=sql,
            hive_table='test_mysql_to_hive',
            recreate=True,
            delimiter=",",
            tblproperties={'test_property': 'test_value'},
            dag=self.dag)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    @mock.patch('airflow.providers.apache.hive.hooks.hive.HiveCliHook.load_file')
    def test_mysql_to_hive_type_conversion(self, mock_load_file):
        mysql_table = 'test_mysql_to_hive'

        hook = MySqlHook()

        try:
            with hook.get_conn() as conn:
                conn.execute("DROP TABLE IF EXISTS {}".format(mysql_table))
                conn.execute("""
                    CREATE TABLE {} (
                        c0 TINYINT,
                        c1 SMALLINT,
                        c2 MEDIUMINT,
                        c3 INT,
                        c4 BIGINT,
                        c5 TIMESTAMP
                    )
                """.format(mysql_table))

            op = MySqlToHiveTransferOperator(
                task_id='test_m2h',
                hive_cli_conn_id='hive_cli_default',
                sql="SELECT * FROM {}".format(mysql_table),
                hive_table='test_mysql_to_hive',
                dag=self.dag)
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

            assert mock_load_file.call_count == 1
            ordered_dict = OrderedDict()
            ordered_dict["c0"] = "SMALLINT"
            ordered_dict["c1"] = "INT"
            ordered_dict["c2"] = "INT"
            ordered_dict["c3"] = "BIGINT"
            ordered_dict["c4"] = "DECIMAL(38,0)"
            ordered_dict["c5"] = "TIMESTAMP"
            self.assertEqual(mock_load_file.call_args[1]["field_dict"], ordered_dict)
        finally:
            with hook.get_conn() as conn:
                conn.execute("DROP TABLE IF EXISTS {}".format(mysql_table))

    def test_mysql_to_hive_verify_csv_special_char(self):
        mysql_table = 'test_mysql_to_hive'
        hive_table = 'test_mysql_to_hive'

        hook = MySqlHook()

        try:
            db_record = (
                'c0',
                '["true"]'
            )
            with hook.get_conn() as conn:
                conn.execute("DROP TABLE IF EXISTS {}".format(mysql_table))
                conn.execute("""
                    CREATE TABLE {} (
                        c0 VARCHAR(25),
                        c1 VARCHAR(25)
                    )
                """.format(mysql_table))
                conn.execute("""
                    INSERT INTO {} VALUES (
                        '{}', '{}'
                    )
                """.format(mysql_table, *db_record))

            import unicodecsv as csv
            op = MySqlToHiveTransferOperator(
                task_id='test_m2h',
                hive_cli_conn_id='hive_cli_default',
                sql="SELECT * FROM {}".format(mysql_table),
                hive_table=hive_table,
                recreate=True,
                delimiter=",",
                quoting=csv.QUOTE_NONE,
                quotechar='',
                escapechar='@',
                dag=self.dag)
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

            hive_hook = HiveServer2Hook()
            result = hive_hook.get_records("SELECT * FROM {}".format(hive_table))
            self.assertEqual(result[0], db_record)
        finally:
            with hook.get_conn() as conn:
                conn.execute("DROP TABLE IF EXISTS {}".format(mysql_table))

    def test_mysql_to_hive_verify_loaded_values(self):
        mysql_table = 'test_mysql_to_hive'
        hive_table = 'test_mysql_to_hive'

        hook = MySqlHook()

        try:
            minmax = (
                255,
                65535,
                16777215,
                4294967295,
                18446744073709551615,
                -128,
                -32768,
                -8388608,
                -2147483648,
                -9223372036854775808
            )

            with hook.get_conn() as conn:
                conn.execute("DROP TABLE IF EXISTS {}".format(mysql_table))
                conn.execute("""
                    CREATE TABLE {} (
                        c0 TINYINT   UNSIGNED,
                        c1 SMALLINT  UNSIGNED,
                        c2 MEDIUMINT UNSIGNED,
                        c3 INT       UNSIGNED,
                        c4 BIGINT    UNSIGNED,
                        c5 TINYINT,
                        c6 SMALLINT,
                        c7 MEDIUMINT,
                        c8 INT,
                        c9 BIGINT
                    )
                """.format(mysql_table))
                conn.execute("""
                    INSERT INTO {} VALUES (
                        {}, {}, {}, {}, {}, {}, {}, {}, {}, {}
                    )
                """.format(mysql_table, *minmax))

            op = MySqlToHiveTransferOperator(
                task_id='test_m2h',
                hive_cli_conn_id='hive_cli_default',
                sql="SELECT * FROM {}".format(mysql_table),
                hive_table=hive_table,
                recreate=True,
                delimiter=",",
                dag=self.dag)
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

            hive_hook = HiveServer2Hook()
            result = hive_hook.get_records("SELECT * FROM {}".format(hive_table))
            self.assertEqual(result[0], minmax)
        finally:
            with hook.get_conn() as conn:
                conn.execute("DROP TABLE IF EXISTS {}".format(mysql_table))
