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
from unittest import TestCase

from airflow.upgrade.rules.postgres_mysql_sqlite_version_upgrade_check import DatabaseVersionCheckRule
from tests.compat import patch
from tests.test_utils.config import conf_vars

SQLITE_CONN = "sqlite:////home/user/airflow.db"
POSTGRES_CONN = "postgresql+psycopg2://username:password@localhost:5432/airflow"
MYSQL_CONN = "mysql+mysqldb://username:password@localhost/airflow"

MOCK_MSG = "See link below for more details: https://github.com/apache/airflow#requirements"


@patch('airflow.settings.Session')
class TestDatabaseVersionCheckRule(TestCase):

    @conf_vars({("core", "sql_alchemy_conn"): SQLITE_CONN})
    def test_valid_sqlite_check(self, MockSession):
        session = MockSession()
        session.execute().scalar.return_value = '3.34.1'

        rule = DatabaseVersionCheckRule()

        assert isinstance(rule.title, str)
        assert isinstance(rule.description, str)

        msg = rule.check(session=session)
        assert msg is None

    @conf_vars({("core", "sql_alchemy_conn"): SQLITE_CONN})
    def test_invalid_sqlite_check(self, MockSession):
        session = MockSession()
        session.execute().scalar.return_value = '2.25.2'

        rule = DatabaseVersionCheckRule()

        assert isinstance(rule.title, str)
        assert isinstance(rule.description, str)

        expected = "From Airflow 2.0, SQLite version below 3.15 is no longer supported. \n" + MOCK_MSG

        msg = rule.check(session=session)
        assert msg == expected

    @conf_vars({("core", "sql_alchemy_conn"): POSTGRES_CONN})
    def test_valid_postgres_check(self, MockSession):
        session = MockSession()
        session.execute().scalar.return_value = '12.3'

        rule = DatabaseVersionCheckRule()

        assert isinstance(rule.title, str)
        assert isinstance(rule.description, str)

        msg = rule.check(session=session)
        assert msg is None

    @conf_vars({("core", "sql_alchemy_conn"): POSTGRES_CONN})
    def test_invalid_postgres_check(self, MockSession):
        session = MockSession()
        session.execute().scalar.return_value = '9.5'

        rule = DatabaseVersionCheckRule()

        assert isinstance(rule.title, str)
        assert isinstance(rule.description, str)

        expected = "From Airflow 2.0, PostgreSQL version below 9.6 is no longer supported. \n" + MOCK_MSG

        msg = rule.check(session=session)
        assert msg == expected

    @conf_vars({("core", "sql_alchemy_conn"): MYSQL_CONN})
    def test_valid_mysql_check(self, MockSession):
        session = MockSession()
        session.execute().scalar.return_value = '8.0.23'

        rule = DatabaseVersionCheckRule()

        assert isinstance(rule.title, str)
        assert isinstance(rule.description, str)

        msg = rule.check(session=session)
        assert msg is None

    @conf_vars({("core", "sql_alchemy_conn"): MYSQL_CONN})
    def test_invalid_mysql_check(self, MockSession):
        session = MockSession()
        session.execute().scalar.return_value = '5.6.11'

        rule = DatabaseVersionCheckRule()

        assert isinstance(rule.title, str)
        assert isinstance(rule.description, str)

        expected = "From Airflow 2.0, MySQL version below 5.7 is no longer supported. \n" + MOCK_MSG

        msg = rule.check(session=session)
        assert msg == expected
