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

from copy import deepcopy
from typing import Any
from unittest import mock

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.sensors.dataproc_metastore import MetastoreHivePartitionSensor

DATAPROC_METASTORE_SENSOR_PATH = "airflow.providers.google.cloud.sensors.dataproc_metastore.{}"
TEST_TASK_ID = "test-task"
PARTITION_1 = "column=value"
PARTITION_2 = "column1=value1/column2=value2"
RESULT_FILE_NAME_1 = "result-001"
RESULT_FILE_NAME_2 = "result-002"
MANIFEST_SUCCESS = {
    "status": {
        "code": 0,
        "message": "Query results are successfully uploaded to cloud storage",
        "details": [],
    },
    "filenames": [],
}
MANIFEST_FAIL = {"status": {"code": 1, "message": "Bad things happened", "details": []}, "filenames": []}
RESULT_FILE_CONTENT: dict[str, Any] = {"rows": [], "metadata": {}}
ROW_1 = []
ROW_2 = []
TEST_SERVICE_ID = "test-service"
TEST_REGION = "test-region"
TEST_TABLE = "test_table"
GCP_PROJECT = "test-project"
GCP_CONN_ID = "test-conn"


class TestMetastoreHivePartitionSensor:
    @pytest.mark.parametrize(
        "requested_partitions, result_files_with_rows, expected_result",
        [
            (None, [(RESULT_FILE_NAME_1, [])], False),
            ([None], [(RESULT_FILE_NAME_1, [])], False),
            (None, [(RESULT_FILE_NAME_1, [ROW_1])], True),
            ([None], [(RESULT_FILE_NAME_1, [ROW_1])], True),
            (None, [(RESULT_FILE_NAME_1, [ROW_1, ROW_2])], True),
            ([None], [(RESULT_FILE_NAME_1, [ROW_1, ROW_2])], True),
            ([PARTITION_1], [(RESULT_FILE_NAME_1, [])], False),
            ([PARTITION_1], [(RESULT_FILE_NAME_1, [ROW_1])], True),
            ([PARTITION_1, PARTITION_2], [(RESULT_FILE_NAME_1, [])], False),
            ([PARTITION_1, PARTITION_2], [(RESULT_FILE_NAME_1, [ROW_1])], False),
            ([PARTITION_1, PARTITION_2], [(RESULT_FILE_NAME_1, [ROW_1, ROW_2])], True),
            ([PARTITION_1, PARTITION_1], [(RESULT_FILE_NAME_1, [])], False),
            ([PARTITION_1, PARTITION_1], [(RESULT_FILE_NAME_1, [ROW_1])], True),
            ([PARTITION_1, PARTITION_2], [(RESULT_FILE_NAME_1, []), (RESULT_FILE_NAME_2, [])], False),
            ([PARTITION_1, PARTITION_2], [(RESULT_FILE_NAME_1, [ROW_1]), (RESULT_FILE_NAME_2, [])], False),
            ([PARTITION_1, PARTITION_2], [(RESULT_FILE_NAME_1, []), (RESULT_FILE_NAME_2, [ROW_2])], False),
            (
                [PARTITION_1, PARTITION_2],
                [(RESULT_FILE_NAME_1, [ROW_1]), (RESULT_FILE_NAME_2, [ROW_2])],
                True,
            ),
        ],
    )
    @mock.patch(DATAPROC_METASTORE_SENSOR_PATH.format("DataprocMetastoreHook"))
    @mock.patch(DATAPROC_METASTORE_SENSOR_PATH.format("parse_json_from_gcs"))
    def test_poke_positive_manifest(
        self,
        mock_parse_json_from_gcs,
        mock_hook,
        requested_partitions,
        result_files_with_rows,
        expected_result,
    ):
        manifest = deepcopy(MANIFEST_SUCCESS)
        parse_json_from_gcs_side_effect = []
        for file_name, rows in result_files_with_rows:
            manifest["filenames"].append(file_name)
            file = deepcopy(RESULT_FILE_CONTENT)
            file["rows"] = rows
            parse_json_from_gcs_side_effect.append(file)

        mock_parse_json_from_gcs.side_effect = [manifest, *parse_json_from_gcs_side_effect]

        sensor = MetastoreHivePartitionSensor(
            task_id=TEST_TASK_ID,
            service_id=TEST_SERVICE_ID,
            region=TEST_REGION,
            table=TEST_TABLE,
            partitions=requested_partitions,
            gcp_conn_id=GCP_CONN_ID,
        )
        assert sensor.poke(context={}) == expected_result

    @pytest.mark.parametrize("empty_manifest", [dict(), list(), tuple(), None, ""])
    @mock.patch(DATAPROC_METASTORE_SENSOR_PATH.format("DataprocMetastoreHook"))
    @mock.patch(DATAPROC_METASTORE_SENSOR_PATH.format("parse_json_from_gcs"))
    def test_poke_empty_manifest(self, mock_parse_json_from_gcs, mock_hook, empty_manifest):
        mock_parse_json_from_gcs.return_value = empty_manifest

        sensor = MetastoreHivePartitionSensor(
            task_id=TEST_TASK_ID,
            service_id=TEST_SERVICE_ID,
            region=TEST_REGION,
            table=TEST_TABLE,
            partitions=[PARTITION_1],
            gcp_conn_id=GCP_CONN_ID,
        )

        with pytest.raises(AirflowException):
            sensor.poke(context={})

    @mock.patch(DATAPROC_METASTORE_SENSOR_PATH.format("DataprocMetastoreHook"))
    @mock.patch(DATAPROC_METASTORE_SENSOR_PATH.format("parse_json_from_gcs"))
    def test_poke_wrong_status(self, mock_parse_json_from_gcs, mock_hook):
        error_message = "Test error message"
        mock_parse_json_from_gcs.return_value = {"code": 1, "message": error_message}

        sensor = MetastoreHivePartitionSensor(
            task_id=TEST_TASK_ID,
            service_id=TEST_SERVICE_ID,
            region=TEST_REGION,
            table=TEST_TABLE,
            partitions=[PARTITION_1],
            gcp_conn_id=GCP_CONN_ID,
        )

        with pytest.raises(AirflowException, match=f"Request failed: {error_message}"):
            sensor.poke(context={})
