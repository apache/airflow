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

import textwrap
from collections import OrderedDict
from contextlib import closing
from unittest import mock

import pytest

from airflow.providers.apache.hive.hooks.hive import HiveCliHook
from airflow.providers.apache.hive.transfers.mysql_to_hive import MySqlToHiveOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]


@pytest.mark.backend("mysql")
class TestTransfer:
    env_vars = {
        "AIRFLOW_CTX_DAG_ID": "test_dag_id",
        "AIRFLOW_CTX_TASK_ID": "test_task_id",
        "AIRFLOW_CTX_EXECUTION_DATE": "2015-01-01T00:00:00+00:00",
        "AIRFLOW_CTX_DAG_RUN_ID": "55",
        "AIRFLOW_CTX_DAG_OWNER": "airflow",
        "AIRFLOW_CTX_DAG_EMAIL": "test@airflow.com",
    }

    @pytest.fixture
    def spy_on_hive(self):
        """Patch HiveCliHook.load_file and capture the contents of the CSV file"""

        class Capturer:
            def __enter__(self):
                self._patch = mock.patch.object(HiveCliHook, "load_file", side_effect=self.capture_file)
                self.load_file = self._patch.start()
                return self

            def __exit__(self, *args):
                self._patch.stop()

            def capture_file(self, file, *args, **kwargs):
                with open(file) as fh:
                    self.csv_contents = fh.read()

        with Capturer() as c:
            yield c

    @pytest.fixture
    def baby_names_table(self):
        rows = [
            (1880, "John", 0.081541, "boy"),
            (1880, "William", 0.080511, "boy"),
            (1880, "James", 0.050057, "boy"),
            (1880, "Charles", 0.045167, "boy"),
            (1880, "George", 0.043292, "boy"),
        ]

        with closing(MySqlHook().get_conn()) as conn:
            with closing(conn.cursor()) as cur:
                cur.execute(
                    """
                CREATE TABLE IF NOT EXISTS baby_names (
                  org_year integer(4),
                  baby_name VARCHAR(25),
                  rate FLOAT(7,6),
                  sex VARCHAR(4)
                )
                """
                )
                for row in rows:
                    cur.execute("INSERT INTO baby_names VALUES(%s, %s, %s, %s);", row)

                conn.commit()

        yield

        with closing(MySqlHook().get_conn()) as conn:
            with closing(conn.cursor()) as cur:
                cur.execute("DROP TABLE IF EXISTS baby_names CASCADE;")

    @pytest.mark.parametrize(
        ("params", "expected", "csv"),
        [
            pytest.param(
                {"recreate": True, "delimiter": ","},
                {
                    "field_dict": {
                        "org_year": "BIGINT",
                        "baby_name": "STRING",
                        "rate": "DOUBLE",
                        "sex": "STRING",
                    },
                    "create": True,
                    "partition": {},
                    "delimiter": ",",
                    "recreate": True,
                    "tblproperties": None,
                },
                textwrap.dedent(
                    """\
                    1880,John,0.081541,boy
                    1880,William,0.080511,boy
                    1880,James,0.050057,boy
                    1880,Charles,0.045167,boy
                    1880,George,0.043292,boy
                    """
                ),
                id="recreate-delimiter",
            ),
            pytest.param(
                {"partition": {"ds": DEFAULT_DATE_DS}},
                {
                    "field_dict": {
                        "org_year": "BIGINT",
                        "baby_name": "STRING",
                        "rate": "DOUBLE",
                        "sex": "STRING",
                    },
                    "create": True,
                    "partition": {"ds": DEFAULT_DATE_DS},
                    "delimiter": "\x01",
                    "recreate": False,
                    "tblproperties": None,
                },
                textwrap.dedent(
                    """\
                    1880\x01John\x010.081541\x01boy
                    1880\x01William\x010.080511\x01boy
                    1880\x01James\x010.050057\x01boy
                    1880\x01Charles\x010.045167\x01boy
                    1880\x01George\x010.043292\x01boy
                    """
                ),
                id="partition",
            ),
            pytest.param(
                {"tblproperties": {"test_property": "test_value"}},
                {
                    "field_dict": {
                        "org_year": "BIGINT",
                        "baby_name": "STRING",
                        "rate": "DOUBLE",
                        "sex": "STRING",
                    },
                    "create": True,
                    "partition": {},
                    "delimiter": "\x01",
                    "recreate": False,
                    "tblproperties": {"test_property": "test_value"},
                },
                textwrap.dedent(
                    """\
                    1880\x01John\x010.081541\x01boy
                    1880\x01William\x010.080511\x01boy
                    1880\x01James\x010.050057\x01boy
                    1880\x01Charles\x010.045167\x01boy
                    1880\x01George\x010.043292\x01boy
                    """
                ),
                id="tblproperties",
            ),
        ],
    )
    @pytest.mark.usefixtures("baby_names_table")
    def test_mysql_to_hive(self, spy_on_hive, params, expected, csv):

        sql = "SELECT * FROM baby_names LIMIT 1000;"
        op = MySqlToHiveOperator(
            task_id="test_m2h",
            hive_cli_conn_id="hive_cli_default",
            sql=sql,
            hive_table="test_mysql_to_hive",
            **params,
        )
        op.execute({})

        spy_on_hive.load_file.assert_called_with(mock.ANY, "test_mysql_to_hive", **expected)

        assert spy_on_hive.csv_contents == csv

    def test_mysql_to_hive_type_conversion(self, spy_on_hive):
        mysql_table = "test_mysql_to_hive"

        hook = MySqlHook()

        try:
            with closing(hook.get_conn()) as conn:
                with closing(conn.cursor()) as cursor:
                    cursor.execute(f"DROP TABLE IF EXISTS {mysql_table}")
                    cursor.execute(
                        f"""
                        CREATE TABLE {mysql_table} (
                            c0 TINYINT,
                            c1 SMALLINT,
                            c2 MEDIUMINT,
                            c3 INT,
                            c4 BIGINT,
                            c5 TIMESTAMP
                        )
                    """
                    )

            op = MySqlToHiveOperator(
                task_id="test_m2h",
                hive_cli_conn_id="hive_cli_default",
                sql=f"SELECT * FROM {mysql_table}",
                hive_table="test_mysql_to_hive",
            )
            op.execute({})

            assert spy_on_hive.load_file.call_count == 1
            ordered_dict = OrderedDict()
            ordered_dict["c0"] = "SMALLINT"
            ordered_dict["c1"] = "INT"
            ordered_dict["c2"] = "INT"
            ordered_dict["c3"] = "BIGINT"
            ordered_dict["c4"] = "DECIMAL(38,0)"
            ordered_dict["c5"] = "TIMESTAMP"
            assert spy_on_hive.load_file.call_args[1]["field_dict"] == ordered_dict
        finally:
            with closing(hook.get_conn()) as conn:
                with closing(conn.cursor()) as cursor:
                    cursor.execute(f"DROP TABLE IF EXISTS {mysql_table}")

    def test_mysql_to_hive_verify_csv_special_char(self, spy_on_hive):

        mysql_table = "test_mysql_to_hive"
        hive_table = "test_mysql_to_hive"

        hook = MySqlHook()

        try:
            db_record = ("c0", '["true",1]')
            with closing(hook.get_conn()) as conn:
                with closing(conn.cursor()) as cursor:
                    cursor.execute(f"DROP TABLE IF EXISTS {mysql_table}")
                    cursor.execute(
                        f"""
                        CREATE TABLE {mysql_table} (
                            c0 VARCHAR(25),
                            c1 VARCHAR(25)
                        )
                    """
                    )
                    cursor.execute(
                        """
                        INSERT INTO {} VALUES (
                            '{}', '{}'
                        )
                    """.format(
                            mysql_table, *db_record
                        )
                    )
                    conn.commit()

            import unicodecsv as csv

            op = MySqlToHiveOperator(
                task_id="test_m2h",
                hive_cli_conn_id="hive_cli_default",
                sql=f"SELECT * FROM {mysql_table}",
                hive_table=hive_table,
                recreate=True,
                delimiter=",",
                quoting=csv.QUOTE_NONE,
                quotechar="",
                escapechar="@",
            )

            op.execute({})

            spy_on_hive.load_file.assert_called()
            assert spy_on_hive.csv_contents == 'c0,["true"@,1]\n'
        finally:
            with closing(hook.get_conn()) as conn:
                with closing(conn.cursor()) as cursor:
                    cursor.execute(f"DROP TABLE IF EXISTS {mysql_table}")
