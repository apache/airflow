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

import pytest

from airflow.models import Connection
from airflow.providers.apache.spark.hooks.spark_connect import SparkConnectHook
from airflow.utils import db

pytestmark = pytest.mark.db_test


class TestSparkConnectHook:
    def setup_method(self):
        db.merge_conn(
            Connection(
                conn_id="spark-default",
                conn_type="spark_connect",
                host="sc://spark-host",
                port=1000,
                login="spark-user",
                password="1234",
                extra='{"queue": "root.etl", "deploy-mode": "cluster"}',
            )
        )

        db.merge_conn(
            Connection(
                conn_id="spark-test",
                conn_type="spark_connect",
                host="nowhere",
                login="spark-user",
            )
        )

        db.merge_conn(
            Connection(
                conn_id="spark-app",
                conn_type="spark_connect",
                host="sc://cluster/app",
                login="spark-user",
            )
        )

    def test_get_connection_url(self):
        expected_url = "sc://spark-host:1000/;user_id=spark-user;token=1234"
        hook = SparkConnectHook(conn_id="spark-default")
        assert hook.get_connection_url() == expected_url

        expected_url = "sc://nowhere/;user_id=spark-user"
        hook = SparkConnectHook(conn_id="spark-test")
        assert hook.get_connection_url() == expected_url

        hook = SparkConnectHook(conn_id="spark-app")
        with pytest.raises(ValueError):
            hook.get_connection_url()
