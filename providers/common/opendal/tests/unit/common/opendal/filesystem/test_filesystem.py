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

from pathlib import Path

from opendal import Operator

from airflow.providers.common.opendal.filesystem.opendal_fs import (
    OpenDALBaseFileSystem,
    OpenDALCopy,
    OpenDALRead,
    OpenDALWrite,
)

OPENDAL_INPUT_CONFIG = {
    "source_config": {"path": "/tmp/file.txt"},
    "destination_config": {"path": "/tmp/dest/file.txt"},
}
operator = Operator("fs", root="/")


class TestOpenDALFilesystem:
    def test_source_path(self):
        opendal_op = OpenDALBaseFileSystem(opendal_config=OPENDAL_INPUT_CONFIG, source_operator=operator)
        assert opendal_op.source_path == "/tmp/file.txt"

    def test_destination_path(self):
        opendal_op = OpenDALBaseFileSystem(
            opendal_config=OPENDAL_INPUT_CONFIG, source_operator=operator, destination_operator=operator
        )
        assert opendal_op.destination_path == "/tmp/dest/file.txt"


class TestOpenDALRead:
    def test_execute_opendal_task(self):
        Path("/tmp/file.txt").write_bytes(b"test_data")
        opendal_op = OpenDALRead(opendal_config=OPENDAL_INPUT_CONFIG, source_operator=operator)
        assert opendal_op.execute_opendal_task() == "test_data"


class TestOpenDALWrite:
    def test_execute_opendal_task(self):
        data = b"test_data"
        opendal_op = OpenDALWrite(opendal_config=OPENDAL_INPUT_CONFIG, source_operator=operator, data=data)
        assert opendal_op.execute_opendal_task() is None
        assert Path("/tmp/file.txt").read_bytes() == data


class TestOpenDALCopy:
    def test_execute_opendal_task(self):
        Path("/tmp/file.txt").write_bytes(b"test_data")
        opendal_op = OpenDALCopy(
            opendal_config=OPENDAL_INPUT_CONFIG, source_operator=operator, destination_operator=operator
        )
        assert opendal_op.execute_opendal_task() is None
        assert Path("/tmp/dest/file.txt").read_bytes() == b"test_data"
