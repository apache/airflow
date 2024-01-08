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

from openlineage.client.run import Dataset

from airflow.providers.common.io.operators.file_transfer import FileTransferOperator


def test_file_transfer_copy():
    with mock.patch(
        "airflow.providers.common.io.operators.file_transfer.ObjectStoragePath"
    ) as mock_object_storage_path:
        source_path = mock.MagicMock()
        target_path = mock.MagicMock()
        mock_object_storage_path.side_effect = [source_path, target_path]
        source_path.exists.return_value = True
        target_path.exists.return_value = False
        operator = FileTransferOperator(
            task_id="test_common_io_file_transfer_task",
            src="test_source",
            dst="test_target",
        )
        operator.execute(context={})
        mock_object_storage_path.assert_has_calls(
            [
                mock.call("test_source", conn_id=None),
                mock.call("test_target", conn_id=None),
            ],
        )
        source_path.copy.assert_called_once_with(target_path)
        target_path.copy.assert_not_called()


def test_get_openlineage_facets_on_start():
    src_bucket = "src-bucket"
    src_key = "src-key"
    dst_bucket = "dst-bucket"
    dst_key = "dst-key"

    expected_input = Dataset(namespace=f"s3://{src_bucket}", name=src_key)
    expected_output = Dataset(namespace=f"s3://{dst_bucket}", name=dst_key)

    op = FileTransferOperator(
        task_id="test",
        src=f"s3://{src_bucket}/{src_key}",
        dst=f"s3://{dst_bucket}/{dst_key}",
    )

    lineage = op.get_openlineage_facets_on_start()
    assert len(lineage.inputs) == 1
    assert len(lineage.outputs) == 1
    assert lineage.inputs[0] == expected_input
    assert lineage.outputs[0] == expected_output
