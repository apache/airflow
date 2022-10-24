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
import unittest
from unittest import mock

from airflow.models.dag import DAG
from airflow.providers.microsoft.azure.operators.wasb_delete_blob import WasbDeleteBlobOperator


class TestWasbDeleteBlobOperator(unittest.TestCase):

    _config = {
        "container_name": "container",
        "blob_name": "blob",
    }

    def setUp(self):
        args = {"owner": "airflow", "start_date": datetime.datetime(2017, 1, 1)}
        self.dag = DAG("test_dag_id", default_args=args)

    def test_init(self):
        operator = WasbDeleteBlobOperator(task_id="wasb_operator_1", dag=self.dag, **self._config)
        assert operator.container_name == self._config["container_name"]
        assert operator.blob_name == self._config["blob_name"]
        assert operator.is_prefix is False
        assert operator.ignore_if_missing is False

        operator = WasbDeleteBlobOperator(
            task_id="wasb_operator_2", dag=self.dag, is_prefix=True, ignore_if_missing=True, **self._config
        )
        assert operator.is_prefix is True
        assert operator.ignore_if_missing is True

    @mock.patch("airflow.providers.microsoft.azure.operators.wasb_delete_blob.WasbHook", autospec=True)
    def test_execute(self, mock_hook):
        mock_instance = mock_hook.return_value
        operator = WasbDeleteBlobOperator(
            task_id="wasb_operator", dag=self.dag, is_prefix=True, ignore_if_missing=True, **self._config
        )
        operator.execute(None)
        mock_instance.delete_file.assert_called_once_with("container", "blob", True, True)
