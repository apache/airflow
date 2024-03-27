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

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.providers.google.cloud.hooks.automl import CloudAutoMLHook


class TestAutoMLHook:
    def setup_method(self):
        self.hook = CloudAutoMLHook()

    def test_init(self):
        with pytest.warns(AirflowProviderDeprecationWarning):
            CloudAutoMLHook()

    def test_extract_object_id(self):
        with pytest.warns(AirflowProviderDeprecationWarning, match="'extract_object_id'"):
            object_id = CloudAutoMLHook.extract_object_id(obj={"name": "x/y"})
            assert object_id == "y"

    def test_get_conn(self):
        with pytest.raises(AirflowException):
            self.hook.get_conn()

    def test_wait_for_operation(self):
        with pytest.raises(AirflowException):
            self.hook.wait_for_operation()

    def test_prediction_client(self):
        with pytest.raises(AirflowException):
            self.hook.prediction_client()

    def test_create_model(self):
        with pytest.raises(AirflowException):
            self.hook.create_model()

    def test_batch_predict(self):
        with pytest.raises(AirflowException):
            self.hook.batch_predict()

    def test_predict(self):
        with pytest.raises(AirflowException):
            self.hook.predict()

    def test_create_dataset(self):
        with pytest.raises(AirflowException):
            self.hook.create_dataset()

    def test_import_data(self):
        with pytest.raises(AirflowException):
            self.hook.import_data()

    def test_list_column_specs(self):
        with pytest.raises(AirflowException):
            self.hook.list_column_specs()

    def test_get_model(self):
        with pytest.raises(AirflowException):
            self.hook.get_model()

    def test_delete_model(self):
        with pytest.raises(AirflowException):
            self.hook.delete_model()

    def test_update_dataset(self):
        with pytest.raises(AirflowException):
            self.hook.update_dataset()

    def test_deploy_model(self):
        with pytest.raises(AirflowException):
            self.hook.deploy_model()

    def test_list_table_specs(self):
        with pytest.raises(AirflowException):
            self.hook.list_table_specs()

    def test_list_datasets(self):
        with pytest.raises(AirflowException):
            self.hook.list_datasets()

    def test_delete_dataset(self):
        with pytest.raises(AirflowException):
            self.hook.delete_dataset()
