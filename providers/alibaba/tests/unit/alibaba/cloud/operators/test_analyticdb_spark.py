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

from unittest import mock

import pytest

from airflow.providers.alibaba.cloud.operators.analyticdb_spark import (
    AnalyticDBSparkBaseOperator,
    AnalyticDBSparkBatchOperator,
    AnalyticDBSparkSQLOperator,
)
from airflow.providers.common.compat.sdk import AirflowException

ADB_SPARK_OPERATOR_STRING = "airflow.providers.alibaba.cloud.operators.analyticdb_spark.{}"

MOCK_FILE = "oss://test.py"
MOCK_CLUSTER_ID = "mock_cluster_id"
MOCK_RG_NAME = "mock_rg_name"
MOCK_ADB_SPARK_CONN_ID = "mock_adb_spark_conn_id"
MOCK_REGION = "mock_region"
MOCK_TASK_ID = "mock_task_id"
MOCK_APP_ID = "mock_app_id"
MOCK_SQL = "SELECT 1;"


class TestAnalyticDBSparkBaseOperator:
    def setup_method(self):
        self.operator = AnalyticDBSparkBaseOperator(
            adb_spark_conn_id=MOCK_ADB_SPARK_CONN_ID,
            region=MOCK_REGION,
            task_id=MOCK_TASK_ID,
        )

    @mock.patch(ADB_SPARK_OPERATOR_STRING.format("AnalyticDBSparkHook"))
    def test_get_hook(self, mock_hook):
        """Test get_hook function works as expected."""
        self.operator.hook
        mock_hook.assert_called_once_with(adb_spark_conn_id=MOCK_ADB_SPARK_CONN_ID, region=MOCK_REGION)

    @mock.patch(ADB_SPARK_OPERATOR_STRING.format("AnalyticDBSparkBaseOperator.hook"))
    def test_poll_for_termination(self, mock_hook):
        """Test poll_for_termination works as expected with COMPLETED application."""
        # Given
        mock_hook.get_spark_state.return_value = "COMPLETED"

        # When
        self.operator.poll_for_termination(MOCK_APP_ID)

    @mock.patch(ADB_SPARK_OPERATOR_STRING.format("AnalyticDBSparkBaseOperator.hook"))
    def test_poll_for_termination_with_exception(self, mock_hook):
        """Test poll_for_termination raises AirflowException with FATAL application."""
        # Given
        mock_hook.get_spark_state.return_value = "FATAL"

        # When
        with pytest.raises(AirflowException, match="Application mock_app_id did not succeed"):
            self.operator.poll_for_termination(MOCK_APP_ID)


class TestAnalyticDBSparkBatchOperator:
    @mock.patch(ADB_SPARK_OPERATOR_STRING.format("AnalyticDBSparkHook"))
    def test_execute(self, mock_hook):
        """Test submit AnalyticDB Spark Batch Application works as expected."""
        operator = AnalyticDBSparkBatchOperator(
            file=MOCK_FILE,
            cluster_id=MOCK_CLUSTER_ID,
            rg_name=MOCK_RG_NAME,
            adb_spark_conn_id=MOCK_ADB_SPARK_CONN_ID,
            region=MOCK_REGION,
            task_id=MOCK_TASK_ID,
        )

        operator.execute(None)

        mock_hook.assert_called_once_with(adb_spark_conn_id=MOCK_ADB_SPARK_CONN_ID, region=MOCK_REGION)
        mock_hook.return_value.submit_spark_app.assert_called_once_with(
            cluster_id=MOCK_CLUSTER_ID,
            rg_name=MOCK_RG_NAME,
            file=MOCK_FILE,
            class_name=None,
            args=None,
            conf=None,
            jars=None,
            py_files=None,
            files=None,
            driver_resource_spec=None,
            executor_resource_spec=None,
            num_executors=None,
            archives=None,
            name=None,
        )

    @mock.patch(ADB_SPARK_OPERATOR_STRING.format("AnalyticDBSparkBaseOperator.hook"))
    def test_execute_with_exception(self, mock_hook):
        """Test submit AnalyticDB Spark Batch Application raises ValueError with invalid parameter."""
        # Given
        mock_hook.submit_spark_app.side_effect = ValueError("List of strings expected")

        # When
        operator = AnalyticDBSparkBatchOperator(
            file=MOCK_FILE,
            args=(True, False),
            cluster_id=MOCK_CLUSTER_ID,
            rg_name=MOCK_RG_NAME,
            adb_spark_conn_id=MOCK_ADB_SPARK_CONN_ID,
            region=MOCK_REGION,
            task_id=MOCK_TASK_ID,
        )

        with pytest.raises(ValueError, match="List of strings expected"):
            operator.execute(None)


class TestAnalyticDBSparklSQLOperator:
    @mock.patch(ADB_SPARK_OPERATOR_STRING.format("AnalyticDBSparkHook"))
    def test_execute(self, mock_hook):
        """Test submit AnalyticDB Spark SQL Application works as expected."""
        operator = AnalyticDBSparkSQLOperator(
            sql=MOCK_SQL,
            cluster_id=MOCK_CLUSTER_ID,
            rg_name=MOCK_RG_NAME,
            adb_spark_conn_id=MOCK_ADB_SPARK_CONN_ID,
            region=MOCK_REGION,
            task_id=MOCK_TASK_ID,
        )

        operator.execute(None)

        mock_hook.assert_called_once_with(adb_spark_conn_id=MOCK_ADB_SPARK_CONN_ID, region=MOCK_REGION)
        mock_hook.return_value.submit_spark_sql.assert_called_once_with(
            cluster_id=MOCK_CLUSTER_ID,
            rg_name=MOCK_RG_NAME,
            sql=MOCK_SQL,
            conf=None,
            driver_resource_spec=None,
            executor_resource_spec=None,
            num_executors=None,
            name=None,
        )

    @mock.patch(ADB_SPARK_OPERATOR_STRING.format("AnalyticDBSparkBaseOperator.hook"))
    def test_execute_with_exception(self, mock_hook):
        """Test submit AnalyticDB Spark SQL Application raises ValueError with invalid parameter."""
        # Given
        mock_hook.submit_spark_sql.side_effect = ValueError("List of strings expected")

        # When
        operator = AnalyticDBSparkSQLOperator(
            sql=MOCK_SQL,
            conf={"spark.eventLog.enabled": True},
            cluster_id=MOCK_CLUSTER_ID,
            rg_name=MOCK_RG_NAME,
            adb_spark_conn_id=MOCK_ADB_SPARK_CONN_ID,
            region=MOCK_REGION,
            task_id=MOCK_TASK_ID,
        )

        with pytest.raises(ValueError, match="List of strings expected"):
            operator.execute(None)
