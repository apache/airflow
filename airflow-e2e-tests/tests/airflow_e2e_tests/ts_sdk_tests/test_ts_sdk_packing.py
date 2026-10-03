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
"""
E2E test for the TypeScript bundle that ``airflow-ts-pack`` writes.

Run with::

    E2E_TEST_MODE=ts_sdk uv run --project airflow-e2e-tests pytest \\
        tests/airflow_e2e_tests/ts_sdk_tests/test_ts_sdk_packing.py -xvs

The packed bundle carries a metadata line with its SDK and the source of each Dag declared in TypeScript,
and no list of the task handlers it registers: the Dag processor asks the bundle for them instead.
"""

from __future__ import annotations

import json

from airflow_e2e_tests.constants import TS_SDK_BUNDLE_FILE


def test_packed_bundle_metadata_records_no_task_handlers(compose_project_path):
    """The second line of the deployed bundle is its metadata, with its SDK and without task handlers."""
    bundle = compose_project_path / "ts-bundles" / TS_SDK_BUNDLE_FILE
    metadata_line = bundle.read_bytes().split(b"\n")[1]
    prefix = b"//# airflowMetadata="
    assert metadata_line.startswith(prefix), metadata_line[:100]

    metadata = json.loads(metadata_line[len(prefix) :])

    assert "task_handlers" not in metadata, metadata
    assert metadata["sdk"]["language"] == "typescript", metadata
    assert metadata["sdk"]["supervisor_schema_version"], metadata
