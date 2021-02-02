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

from airflow.models import Connection
from airflow.upgrade.rules.spark_jdbc_operator_conn_id_rule import SparkJDBCOperatorConnIdRule
from airflow.utils.db import create_session
from tests.test_utils.db import clear_db_connections


class TestSparkJDBCOperatorDefaultConnIdRule(TestCase):
    def tearDown(self):
        clear_db_connections()

    def test_check(self):
        rule = SparkJDBCOperatorConnIdRule()

        assert isinstance(rule.title, str)
        assert isinstance(rule.description, str)

        with create_session() as session:
            conn = Connection(conn_id='spark_default')
            session.merge(conn)

        msgs = rule.check(session=session)
        assert msgs is None

    def test_invalid_check(self):
        rule = SparkJDBCOperatorConnIdRule()

        assert isinstance(rule.title, str)
        assert isinstance(rule.description, str)

        with create_session() as session:
            conn = Connection(conn_id='spark-default')
            session.merge(conn)

        expected = (
            "Deprecation Warning: From Airflow 2.0, the default value of 'conn_id' argument of "
            "SparkJDBCOperator class has been changed to 'spark_default' to conform with the naming "
            "conventions of all other connection names. Please rename the connection with "
            "id 'spark-default' to 'spark_default' or explicitly pass 'spark-default' "
            "to the operator. See the link below for details: "
            "https://github.com/apache/airflow/blob/2.0.0/UPDATING.md#sparkjdbchook-default-connection"
        )

        assert expected == rule.check(session=session)
