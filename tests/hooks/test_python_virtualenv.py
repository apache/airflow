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

import json
from unittest import mock

import pytest

from airflow.exceptions import AirflowNotFoundException
from airflow.hooks.python_virtualenv import PythonVirtualenvHook
from airflow.models import Connection
from airflow.utils import db
from tests.test_utils.db import clear_db_connections

pytestmark = pytest.mark.db_test


class TestPythonVirtualenvHook:
    @classmethod
    def setup_class(cls) -> None:
        for conn_id, extra in [
            ("use_requirements", {"requirements": "apache-airflow==2.1.0\nfuncsigs==1.0.2"}),
            ("use_system_site_packages", {"system_site_packages": True}),
            ("use_pip_options", {"pip_install_options": "--no-cache-dir --proxy=http://proxy:3128"}),
            ("use_index_urls", {"index_urls": "https://pypi.org/simple,https://pypi.example.com/simple"}),
        ]:
            db.merge_conn(Connection(conn_type="python_venv", conn_id=conn_id, extra=json.dumps(extra)))

    @classmethod
    def teardown_class(cls) -> None:
        clear_db_connections()

    def test_python_virtualenv_set_connection_with_wrong_connection(self):
        with pytest.raises(AirflowNotFoundException):
            PythonVirtualenvHook(venv_conn_id="some_conn")

    @mock.patch("airflow.hooks.python_virtualenv.PythonVirtualenvHook.get_connection")
    def test_python_virtualenv_set_connection_with_connection(self, mock_conn):
        PythonVirtualenvHook(venv_conn_id="some_conn")
        mock_conn.assert_called_once_with("some_conn")

    def test_parse_requirements_in_python_virtualenv_hook(self):
        hook = PythonVirtualenvHook(venv_conn_id="use_requirements")
        assert "apache-airflow==2.1.0" in hook.requirements
        assert "funcsigs==1.0.2" in hook.requirements

    def test_parse_system_site_packages_in_python_virtualenv_hook(self):
        hook = PythonVirtualenvHook(venv_conn_id="use_system_site_packages")
        assert hook.system_site_packages

    def test_parse_pip_install_options_in_python_virtualenv_hook(self):
        hook = PythonVirtualenvHook(venv_conn_id="use_pip_options")
        assert "--no-cache-dir" in hook.pip_install_options
        assert "--proxy=http://proxy:3128" in hook.pip_install_options

    def test_parse_index_urls_in_python_virtualenv_hook(self):
        hook = PythonVirtualenvHook(venv_conn_id="use_index_urls")
        assert "https://pypi.org/simple" in hook.index_urls
        assert "https://pypi.example.com/simple" in hook.index_urls
