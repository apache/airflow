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

from alibabacloud_adb20211201.models import (
    GetSparkAppLogResponse,
    GetSparkAppLogResponseBody,
    GetSparkAppLogResponseBodyData,
    GetSparkAppStateResponse,
    GetSparkAppStateResponseBody,
    GetSparkAppStateResponseBodyData,
    GetSparkAppWebUiAddressResponse,
    GetSparkAppWebUiAddressResponseBody,
    GetSparkAppWebUiAddressResponseBodyData,
    KillSparkAppResponse,
    SubmitSparkAppResponse,
)

from airflow.providers.alibaba.cloud.hooks.analyticdb_spark import AnalyticDBSparkHook

from providers.tests.alibaba.cloud.utils.analyticdb_spark_mock import mock_adb_spark_hook_default_project_id

ADB_SPARK_STRING = "airflow.providers.alibaba.cloud.hooks.analyticdb_spark.{}"
MOCK_ADB_SPARK_CONN_ID = "mock_id"
MOCK_ADB_CLUSTER_ID = "mock_adb_cluster_id"
MOCK_ADB_RG_NAME = "mock_adb_rg_name"
MOCK_ADB_SPARK_ID = "mock_adb_spark_id"


class TestAnalyticDBSparkHook:
    def setup_method(self):
        with mock.patch(
            ADB_SPARK_STRING.format("AnalyticDBSparkHook.__init__"),
            new=mock_adb_spark_hook_default_project_id,
        ):
            self.hook = AnalyticDBSparkHook(adb_spark_conn_id=MOCK_ADB_SPARK_CONN_ID)

    def test_build_submit_app_data(self):
        """Test build submit application data for analyticDB spark as expected."""
        res_data = self.hook.build_submit_app_data(
            file="oss://test_file",
            class_name="com.aliyun.spark.SparkPi",
            args=[1000, "test-args"],
            conf={"spark.executor.instances": 1, "spark.eventLog.enabled": "true"},
            jars=["oss://1.jar", "oss://2.jar"],
            py_files=["oss://1.py", "oss://2.py"],
            files=["oss://1.file", "oss://2.file"],
            driver_resource_spec="medium",
            executor_resource_spec="medium",
            num_executors=2,
            archives=["oss://1.zip", "oss://2.zip"],
            name="test",
        )
        except_data = {
            "file": "oss://test_file",
            "className": "com.aliyun.spark.SparkPi",
            "args": ["1000", "test-args"],
            "conf": {
                "spark.executor.instances": 1,
                "spark.eventLog.enabled": "true",
                "spark.driver.resourceSpec": "medium",
                "spark.executor.resourceSpec": "medium",
            },
            "jars": ["oss://1.jar", "oss://2.jar"],
            "pyFiles": ["oss://1.py", "oss://2.py"],
            "files": ["oss://1.file", "oss://2.file"],
            "archives": ["oss://1.zip", "oss://2.zip"],
            "name": "test",
        }
        assert res_data == except_data

    def test_build_submit_sql_data(self):
        """Test build submit sql data for analyticDB spark as expected."""
        res_data = self.hook.build_submit_sql_data(
            sql="""
            set spark.executor.instances=1;
            show databases;
            """,
            conf={"spark.executor.instances": 2},
            driver_resource_spec="medium",
            executor_resource_spec="medium",
            num_executors=3,
            name="test",
        )
        except_data = (
            "set spark.driver.resourceSpec = medium;set spark.executor.resourceSpec = medium;set "
            "spark.executor.instances = 2;set spark.app.name = test;\n            set "
            "spark.executor.instances=1;\n            show databases;"
        )
        assert res_data == except_data

    @mock.patch(ADB_SPARK_STRING.format("AnalyticDBSparkHook.get_adb_spark_client"))
    def test_submit_spark_app(self, mock_service):
        """Test submit_spark_app function works as expected."""
        # Given
        mock_client = mock_service.return_value
        exists_method = mock_client.submit_spark_app
        exists_method.return_value = SubmitSparkAppResponse(status_code=200)

        # When
        res = self.hook.submit_spark_app(MOCK_ADB_CLUSTER_ID, MOCK_ADB_RG_NAME, "oss://test.py")

        # Then
        assert isinstance(res, SubmitSparkAppResponse)
        mock_service.assert_called_once_with()

    @mock.patch(ADB_SPARK_STRING.format("AnalyticDBSparkHook.get_adb_spark_client"))
    def test_submit_spark_sql(self, mock_service):
        """Test submit_spark_app function works as expected."""
        # Given
        mock_client = mock_service.return_value
        exists_method = mock_client.submit_spark_app
        exists_method.return_value = SubmitSparkAppResponse(status_code=200)

        # When
        res = self.hook.submit_spark_sql(MOCK_ADB_CLUSTER_ID, MOCK_ADB_RG_NAME, "SELECT 1")

        # Then
        assert isinstance(res, SubmitSparkAppResponse)
        mock_service.assert_called_once_with()

    @mock.patch(ADB_SPARK_STRING.format("AnalyticDBSparkHook.get_adb_spark_client"))
    def test_get_spark_state(self, mock_service):
        """Test get_spark_state function works as expected."""
        # Given
        mock_client = mock_service.return_value
        exists_method = mock_client.get_spark_app_state
        exists_method.return_value = GetSparkAppStateResponse(
            body=GetSparkAppStateResponseBody(data=GetSparkAppStateResponseBodyData(state="RUNNING"))
        )

        # When
        res = self.hook.get_spark_state(MOCK_ADB_SPARK_ID)

        # Then
        assert res == "RUNNING"
        mock_service.assert_called_once_with()

    @mock.patch(ADB_SPARK_STRING.format("AnalyticDBSparkHook.get_adb_spark_client"))
    def test_get_spark_web_ui_address(self, mock_service):
        """Test get_spark_web_ui_address function works as expected."""
        # Given
        mock_client = mock_service.return_value
        exists_method = mock_client.get_spark_app_web_ui_address
        exists_method.return_value = GetSparkAppWebUiAddressResponse(
            body=GetSparkAppWebUiAddressResponseBody(
                data=GetSparkAppWebUiAddressResponseBodyData(web_ui_address="https://mock-web-ui-address.com")
            )
        )

        # When
        res = self.hook.get_spark_web_ui_address(MOCK_ADB_SPARK_ID)

        # Then
        assert res == "https://mock-web-ui-address.com"
        mock_service.assert_called_once_with()

    @mock.patch(ADB_SPARK_STRING.format("AnalyticDBSparkHook.get_adb_spark_client"))
    def test_get_spark_log(self, mock_service):
        """Test get_spark_log function works as expected."""
        # Given
        mock_client = mock_service.return_value
        exists_method = mock_client.get_spark_app_log
        exists_method.return_value = GetSparkAppLogResponse(
            body=GetSparkAppLogResponseBody(data=GetSparkAppLogResponseBodyData(log_content="Pi is 3.14"))
        )

        # When
        res = self.hook.get_spark_log(MOCK_ADB_SPARK_ID)

        # Then
        assert res == "Pi is 3.14"
        mock_service.assert_called_once_with()

    @mock.patch(ADB_SPARK_STRING.format("AnalyticDBSparkHook.get_adb_spark_client"))
    def test_kill_spark_app(self, mock_service):
        """Test kill_spark_app function works as expected."""
        # Given
        mock_client = mock_service.return_value
        exists_method = mock_client.kill_spark_app
        exists_method.return_value = KillSparkAppResponse()

        # When
        self.hook.kill_spark_app(MOCK_ADB_SPARK_ID)

        # Then
        mock_service.assert_called_once_with()
