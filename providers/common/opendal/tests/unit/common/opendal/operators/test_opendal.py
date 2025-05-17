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

import shutil
from pathlib import Path
from tempfile import mkdtemp
from unittest.mock import Mock, patch

from airflow.providers.common.opendal.operators.opendal import OpenDALTaskOperator


def del_paths(paths):
    for path in paths:
        if "/tmp" in path:
            shutil.rmtree(path, ignore_errors=True)


class TestOpenDALTaskOperator:
    def test_init(self):
        """
        Test the initialization of OpenDALTaskOperator.
        """
        opendal_config = {
            "source_config": {
                "conn_id": "aws_default",
                "operator_args": {"key": "value"},
            },
            "destination_config": {
                "conn_id": "aws_default",
                "operator_args": {"key": "value"},
            },
        }
        action = "write"
        opendal_conn_id = "opendal_default"
        data = b"test_data"

        operator = OpenDALTaskOperator(
            task_id="test_task",
            opendal_config=opendal_config,
            action=action,
            opendal_conn_id=opendal_conn_id,
            data=data,
        )

        assert operator.opendal_config == opendal_config
        assert operator.action == action
        assert operator.opendal_conn_id == opendal_conn_id
        assert operator.data == data

    @patch("airflow.providers.common.opendal.hooks.opendal.OpenDALHook.fetch_conn")
    def test_execute_to_write_with_source_config(self, mock_fetch_conn):
        """
        Test the execute method of OpenDALTaskOperator for write action.
        """
        temp_dir = mkdtemp()

        path_to_write = f"{temp_dir}/hello.txt"
        mock_fetch_conn.return_value = Mock(
            conn_type="opendal",
            extra_dejson={
                "source_config": {
                    "operator_args": {"scheme": "fs"},
                },
            },
        )

        opendal_config = {
            "source_config": {
                "conn_id": "opendal_default",
                "operator_args": {"root": "/"},
                "path": path_to_write,
            },
        }
        action = "write"
        opendal_conn_id = "opendal_default"
        data = b"test_data"

        operator = OpenDALTaskOperator(
            task_id="test_task",
            opendal_config=opendal_config,
            action=action,
            opendal_conn_id=opendal_conn_id,
            data=data,
        )

        result = operator.execute(context={})
        assert result is None
        assert Path(path_to_write).read_bytes() == data
        del_paths([temp_dir])

    @patch("airflow.providers.common.opendal.hooks.opendal.OpenDALHook.fetch_conn")
    def test_execute_to_read_with_source_config(self, mock_fetch_conn):
        """
        Test the execute method of OpenDALTaskOperator for read action.
        """
        temp_dir = mkdtemp()

        path_to_write = f"{temp_dir}/hello.txt"

        # Write test data to the file
        Path(path_to_write).write_bytes(b"test_data")

        mock_fetch_conn.return_value = Mock(
            conn_type="opendal",
            extra_dejson={
                "source_config": {
                    "operator_args": {"scheme": "fs"},
                },
            },
        )

        opendal_config = {
            "source_config": {
                "conn_id": "opendal_default",
                "operator_args": {"root": "/"},
                "path": path_to_write,
            },
        }

        operator = OpenDALTaskOperator(
            task_id="test_task",
            opendal_config=opendal_config,
            action="read",
            opendal_conn_id="opendal_default",
        )

        result = operator.execute(context={})
        assert result is not None
        assert result == "test_data"
        del_paths([temp_dir])

    @patch("airflow.providers.common.opendal.hooks.opendal.OpenDALHook.fetch_conn")
    def test_execute_to_copy_with_source_destination_config(self, mock_fetch_conn):
        """
        Test the execute method of OpenDALTaskOperator for read action.
        """

        source_temp_dir = mkdtemp()
        destination_temp_dir = mkdtemp()

        source_path = f"{source_temp_dir}/hello.txt"
        dest_path = f"{destination_temp_dir}/hello.txt"

        # Write test data to the file
        Path(source_path).write_bytes(b"test_data")

        mock_fetch_conn.return_value = Mock(
            conn_type="opendal",
            extra_dejson={
                "source_config": {
                    "operator_args": {"scheme": "fs"},
                },
                "destination_config": {
                    "operator_args": {"scheme": "fs"},
                },
            },
        )

        opendal_config = {
            "source_config": {
                "operator_args": {"root": "/"},
                "path": source_path,
            },
            "destination_config": {
                "operator_args": {"root": "/"},
                "path": dest_path,
            },
        }

        operator = OpenDALTaskOperator(
            task_id="test_task",
            opendal_config=opendal_config,
            action="copy",
            opendal_conn_id="opendal_default",
        )

        result = operator.execute(context={})
        assert result is None
        assert Path(dest_path).read_bytes() == Path(source_path).read_bytes()

        del_paths([source_temp_dir, destination_temp_dir])
