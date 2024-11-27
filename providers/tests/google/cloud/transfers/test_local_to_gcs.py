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
import os
from glob import glob
from unittest import mock

import pytest

from airflow.models.dag import DAG
from airflow.providers.common.compat.openlineage.facet import (
    Identifier,
    SymlinksDatasetFacet,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

pytestmark = pytest.mark.db_test


class TestFileToGcsOperator:
    _config = {
        "bucket": "dummy",
        "mime_type": "application/octet-stream",
        "gzip": False,
        "chunk_size": 262144,
    }

    def setup_method(self):
        args = {"owner": "airflow", "start_date": datetime.datetime(2017, 1, 1)}
        self.dag = DAG("test_dag_id", schedule=None, default_args=args)
        self.testfile1 = "/tmp/fake1.csv"
        with open(self.testfile1, "wb") as f:
            f.write(b"x" * 393216)
        self.testfile2 = "/tmp/fake2.csv"
        with open(self.testfile2, "wb") as f:
            f.write(b"x" * 393216)
        self.testfiles = [self.testfile1, self.testfile2]

    def teardown_method(self):
        os.remove(self.testfile1)
        os.remove(self.testfile2)

    def test_init(self):
        operator = LocalFilesystemToGCSOperator(
            task_id="file_to_gcs_operator",
            dag=self.dag,
            src=self.testfile1,
            dst="test/test1.csv",
            **self._config,
        )
        assert operator.src == self.testfile1
        assert operator.dst == "test/test1.csv"
        assert operator.bucket == self._config["bucket"]
        assert operator.mime_type == self._config["mime_type"]
        assert operator.gzip == self._config["gzip"]
        assert operator.chunk_size == self._config["chunk_size"]

    @mock.patch("airflow.providers.google.cloud.transfers.local_to_gcs.GCSHook", autospec=True)
    def test_execute(self, mock_hook):
        mock_instance = mock_hook.return_value
        operator = LocalFilesystemToGCSOperator(
            task_id="file_to_gcs_operator",
            dag=self.dag,
            src=self.testfile1,
            dst="test/test1.csv",
            **self._config,
        )
        operator.execute(None)
        mock_instance.upload.assert_called_once_with(
            bucket_name=self._config["bucket"],
            filename=self.testfile1,
            gzip=self._config["gzip"],
            mime_type=self._config["mime_type"],
            object_name="test/test1.csv",
            chunk_size=self._config["chunk_size"],
        )

    @pytest.mark.db_test
    def test_execute_with_empty_src(self):
        operator = LocalFilesystemToGCSOperator(
            task_id="file_to_gcs_operator",
            dag=self.dag,
            src="no_file.txt",
            dst="test/no_file.txt",
            **self._config,
        )
        with pytest.raises(FileNotFoundError):
            operator.execute(None)

    @mock.patch("airflow.providers.google.cloud.transfers.local_to_gcs.GCSHook", autospec=True)
    def test_execute_multiple(self, mock_hook):
        mock_instance = mock_hook.return_value
        operator = LocalFilesystemToGCSOperator(
            task_id="file_to_gcs_operator", dag=self.dag, src=self.testfiles, dst="test/", **self._config
        )
        operator.execute(None)
        files_objects = zip(
            self.testfiles, ["test/" + os.path.basename(testfile) for testfile in self.testfiles]
        )
        calls = [
            mock.call(
                bucket_name=self._config["bucket"],
                filename=filepath,
                gzip=self._config["gzip"],
                mime_type=self._config["mime_type"],
                object_name=object_name,
                chunk_size=self._config["chunk_size"],
            )
            for filepath, object_name in files_objects
        ]
        mock_instance.upload.assert_has_calls(calls)

    @mock.patch("airflow.providers.google.cloud.transfers.local_to_gcs.GCSHook", autospec=True)
    def test_execute_wildcard(self, mock_hook):
        mock_instance = mock_hook.return_value
        operator = LocalFilesystemToGCSOperator(
            task_id="file_to_gcs_operator", dag=self.dag, src="/tmp/fake*.csv", dst="test/", **self._config
        )
        operator.execute(None)
        object_names = ["test/" + os.path.basename(fp) for fp in glob("/tmp/fake*.csv")]
        files_objects = zip(glob("/tmp/fake*.csv"), object_names)
        calls = [
            mock.call(
                bucket_name=self._config["bucket"],
                filename=filepath,
                gzip=self._config["gzip"],
                mime_type=self._config["mime_type"],
                object_name=object_name,
                chunk_size=self._config["chunk_size"],
            )
            for filepath, object_name in files_objects
        ]
        mock_instance.upload.assert_has_calls(calls)

    @pytest.mark.parametrize(
        ("src", "dst"),
        [
            ("/tmp/fake*.csv", "test/test1.csv"),
            ("/tmp/fake*.csv", "test"),
            ("/tmp/fake*.csv", "test/dir"),
        ],
    )
    @mock.patch("airflow.providers.google.cloud.transfers.local_to_gcs.GCSHook", autospec=True)
    def test_execute_negative(self, mock_hook, src, dst):
        mock_instance = mock_hook.return_value
        operator = LocalFilesystemToGCSOperator(
            task_id="file_to_gcs_operator",
            dag=self.dag,
            src=src,
            dst=dst,
            **self._config,
        )
        with pytest.raises(ValueError):
            operator.execute(None)
        mock_instance.assert_not_called()

    @pytest.mark.parametrize(
        ("src", "dst", "expected_input", "expected_output", "symlink"),
        [
            ("/tmp/fake*.csv", "test/", "/tmp", "test", True),
            ("/tmp/../tmp/fake*.csv", "test/", "/tmp", "test", True),
            ("/tmp/fake1.csv", "test/test1.csv", "/tmp/fake1.csv", "test/test1.csv", False),
            ("/tmp/fake1.csv", "test/pre", "/tmp/fake1.csv", "test/pre", False),
        ],
    )
    def test_get_openlineage_facets_on_start_with_string_src(
        self, src, dst, expected_input, expected_output, symlink
    ):
        operator = LocalFilesystemToGCSOperator(
            task_id="gcs_to_file_sensor",
            dag=self.dag,
            src=src,
            dst=dst,
            **self._config,
        )
        result = operator.get_openlineage_facets_on_start()
        assert not result.job_facets
        assert not result.run_facets
        assert len(result.outputs) == 1
        assert len(result.inputs) == 1
        assert result.outputs[0].name == expected_output
        assert result.inputs[0].name == expected_input
        if symlink:
            assert result.inputs[0].facets["symlink"] == SymlinksDatasetFacet(
                identifiers=[Identifier(namespace="file", name=src, type="file")]
            )

    @pytest.mark.parametrize(
        ("src", "dst", "expected_inputs", "expected_output"),
        [
            (["/tmp/fake1.csv", "/tmp/fake2.csv"], "test/", ["/tmp/fake1.csv", "/tmp/fake2.csv"], "test"),
            (["/tmp/fake1.csv", "/tmp/fake2.csv"], "", ["/tmp/fake1.csv", "/tmp/fake2.csv"], "/"),
        ],
    )
    def test_get_openlineage_facets_on_start_with_list_src(self, src, dst, expected_inputs, expected_output):
        operator = LocalFilesystemToGCSOperator(
            task_id="gcs_to_file_sensor",
            dag=self.dag,
            src=src,
            dst=dst,
            **self._config,
        )
        result = operator.get_openlineage_facets_on_start()
        assert not result.job_facets
        assert not result.run_facets
        assert len(result.outputs) == 1
        assert len(result.inputs) == len(expected_inputs)
        assert result.outputs[0].name == expected_output
        assert result.outputs[0].namespace == "gs://dummy"
        assert all(inp.name in expected_inputs for inp in result.inputs)
        assert all(inp.namespace == "file" for inp in result.inputs)
