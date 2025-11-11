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

from unittest.mock import patch

from tests_common.test_utils.fernet import generate_fernet_key_string


class TestFernetUtils:
    """Test utils for Fernet encryption."""

    @patch("tests_common.test_utils.fernet.Fernet")
    def test_generate_fernet_key_string(self, mock_fernet):
        """Test generating a Fernet key."""
        mock_fernet.generate_key.return_value = b"test_key"
        key = generate_fernet_key_string()
        assert key == "test_key"
        mock_fernet.generate_key.assert_called_once()

    @patch("tests_common.test_utils.fernet.Fernet")
    def test_update_environment_variable_from_compose_yaml(self, mock_fernet, tmp_path):
        """Test updating environment variable in docker-compose YAML file."""
        mock_fernet.generate_key.return_value = b"new_test_key"
        compose_yaml_content = """
        x-airflow-common:
            environment:
                AIRFLOW__CORE__FERNET_KEY: old_key
        """
        compose_file = tmp_path / "docker-compose.yaml"
        compose_file.write_text(compose_yaml_content)

        from tests_common.test_utils.fernet import update_environment_variable_from_compose_yaml

        update_environment_variable_from_compose_yaml(compose_file)

        updated_content = compose_file.read_text()
        assert "new_test_key" in updated_content
        mock_fernet.generate_key.assert_called_once()
