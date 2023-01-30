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

import datetime
from unittest import mock

import pytest

from airflow.models.dag import DAG
from airflow.providers.microsoft.azure.transfers.local_to_wasb import LocalFilesystemToWasbOperator


class TestLocalFilesystemToWasbOperator:

    _config = {
        "file_path": "file",
        "container_name": "container",
        "blob_name": "blob",
        "wasb_conn_id": "wasb_default",
        "retries": 3,
    }

    def setup_method(self):
        args = {"owner": "airflow", "start_date": datetime.datetime(2017, 1, 1)}
        self.dag = DAG("test_dag_id", default_args=args)

    def test_init(self):
        operator = LocalFilesystemToWasbOperator(task_id="wasb_operator_1", dag=self.dag, **self._config)
        assert operator.file_path == self._config["file_path"]
        assert operator.container_name == self._config["container_name"]
        assert operator.blob_name == self._config["blob_name"]
        assert operator.wasb_conn_id == self._config["wasb_conn_id"]
        assert operator.load_options == {}
        assert operator.retries == self._config["retries"]

        operator = LocalFilesystemToWasbOperator(
            task_id="wasb_operator_2", dag=self.dag, load_options={"timeout": 2}, **self._config
        )
        assert operator.load_options == {"timeout": 2}

    @pytest.mark.parametrize(argnames="create_container", argvalues=[True, False])
    @mock.patch("airflow.providers.microsoft.azure.transfers.local_to_wasb.WasbHook", autospec=True)
    def test_execute(self, mock_hook, create_container):
        mock_instance = mock_hook.return_value
        operator = LocalFilesystemToWasbOperator(
            task_id="wasb_sensor",
            dag=self.dag,
            create_container=create_container,
            load_options={"timeout": 2},
            **self._config,
        )
        operator.execute(None)
        mock_instance.load_file.assert_called_once_with(
            "file", "container", "blob", create_container, timeout=2
        )
