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

from packaging.version import Version

from airflow.configuration import conf
from airflow.upgrade.rules.base_rule import BaseRule
from airflow.utils.db import provide_session


class DatabaseVersionCheckRule(BaseRule):
    title = "Check versions of PostgreSQL, MySQL, and SQLite to ease upgrade to Airflow 2.0"

    description = """\
From Airflow 2.0, the following database versions are supported:
PostgreSQl - 9.6, 10, 11, 12, 13;
MySQL - 5.7, 8;
SQLite - 3.15+
    """

    @provide_session
    def check(self, session=None):

        more_info = "See link below for more details: https://github.com/apache/airflow#requirements"

        conn_str = conf.get(section="core", key="sql_alchemy_conn")

        if "sqlite" in conn_str:
            min_req_sqlite_version = Version('3.15')
            installed_sqlite_version = Version(session.execute('select sqlite_version();').scalar())
            if installed_sqlite_version < min_req_sqlite_version:
                return "From Airflow 2.0, SQLite version below 3.15 is no longer supported. \n" + more_info

        elif "postgres" in conn_str:
            min_req_postgres_version = Version('9.6')
            installed_postgres_version = Version(session.execute('SHOW server_version;').scalar())
            if installed_postgres_version < min_req_postgres_version:
                return "From Airflow 2.0, PostgreSQL version below 9.6 is no longer supported. \n" + more_info

        elif "mysql" in conn_str:
            min_req_mysql_version = Version('5.7')
            installed_mysql_version = Version(session.execute('SELECT VERSION();').scalar())
            if installed_mysql_version < min_req_mysql_version:
                return "From Airflow 2.0, MySQL version below 5.7 is no longer supported. \n" + more_info
