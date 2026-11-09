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

from types import SimpleNamespace

import pytest

from airflow.providers.common.io.macros.xcom import xcom_path
from airflow.providers.common.io.xcom import backend as xcom_backend

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS, XCOM_RETURN_KEY

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk.execution_time.comms import XComResult

pytestmark = pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Looking up the path requires Airflow 3")

TI = SimpleNamespace(dag_id="test-dag-id", run_id="test-run")


@pytest.fixture(autouse=True)
def object_storage_backend(tmp_path):
    xcom_backend._get_base_path.cache_clear()
    with conf_vars(
        {
            ("core", "xcom_backend"): "airflow.providers.common.io.xcom.backend.XComObjectStorageBackend",
            ("common.io", "xcom_objectstorage_path"): f"file://{tmp_path.as_posix()}",
            ("common.io", "xcom_objectstorage_threshold"): "0",
        }
    ):
        yield f"file://{tmp_path.as_posix()}"
    xcom_backend._get_base_path.cache_clear()


def test_the_path_of_a_value_in_object_storage(object_storage_backend, mock_supervisor_comms):
    stored = f"{object_storage_backend}/test-dag-id/test-run/extract/value.json"
    mock_supervisor_comms.send.return_value = XComResult(key=XCOM_RETURN_KEY, value=stored)

    assert xcom_path(TI, "extract") == stored
    request = mock_supervisor_comms.send.call_args.args[0]
    assert (request.dag_id, request.run_id, request.task_id) == ("test-dag-id", "test-run", "extract")


def test_the_path_without_its_protocol(object_storage_backend, mock_supervisor_comms):
    stored = f"{object_storage_backend}/test-dag-id/test-run/extract/value.json"
    mock_supervisor_comms.send.return_value = XComResult(key=XCOM_RETURN_KEY, value=stored)

    assert xcom_path(TI, "extract", protocol=False) == stored.removeprefix("file://")


def test_another_dag_run_and_map_index(object_storage_backend, mock_supervisor_comms):
    stored = f"{object_storage_backend}/other/run/extract/value.json"
    mock_supervisor_comms.send.return_value = XComResult(key="rows", value=stored)

    xcom_path(TI, "extract", "rows", dag_id="other", run_id="run", map_index=3)

    request = mock_supervisor_comms.send.call_args.args[0]
    assert (request.key, request.dag_id, request.run_id, request.map_index) == ("rows", "other", "run", 3)


def test_no_such_xcom_fails_the_rendering(mock_supervisor_comms):
    mock_supervisor_comms.send.return_value = XComResult(key=XCOM_RETURN_KEY, value=None)

    with pytest.raises(LookupError, match="No XCom 'return_value' from task 'extract'"):
        xcom_path(TI, "extract")


def test_a_value_in_the_database_fails_the_rendering(mock_supervisor_comms):
    mock_supervisor_comms.send.return_value = XComResult(key=XCOM_RETURN_KEY, value={"small": True})

    with pytest.raises(ValueError, match="stored in the database"):
        xcom_path(TI, "extract")


def test_another_backend_fails_the_rendering(mock_supervisor_comms):
    with conf_vars({("core", "xcom_backend"): "airflow.sdk.bases.xcom.BaseXCom"}):
        with pytest.raises(TypeError, match="does not store values in object storage"):
            xcom_path(TI, "extract")
