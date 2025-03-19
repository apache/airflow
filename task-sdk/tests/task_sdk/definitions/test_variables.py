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

from airflow.sdk import Variable
from airflow.sdk.execution_time.comms import VariableResult
from airflow.sdk.execution_time.supervisor import initialize_secrets_backend_on_workers

from tests_common.test_utils.config import conf_vars


class TestVariables:
    def test_var_get(self, mock_supervisor_comms):
        var_result = VariableResult(key="my_key", value="my_value")
        mock_supervisor_comms.get_message.return_value = var_result

        var = Variable.get(key="my_key")
        assert var is not None
        assert isinstance(var, str)
        assert var == "my_value"

    def test_var_get_from_secrets_found(self, mock_supervisor_comms, tmp_path):
        path = tmp_path / "var.env"
        path.write_text("VAR_A=some_value")

        with conf_vars(
            {
                (
                    "workers",
                    "secrets_backend",
                ): "airflow.secrets.local_filesystem.LocalFilesystemBackend",
                ("workers", "secrets_backend_kwargs"): f'{{"variables_file_path": "{path}"}}',
            }
        ):
            initialize_secrets_backend_on_workers()
            retrieved_var = Variable.get_variable_from_secrets(key="VAR_A")
            assert retrieved_var is not None
            assert retrieved_var == "some_value"
