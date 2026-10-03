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
E2E tests for the Go bundles that ``airflow-go-pack`` writes.

Run with::

    E2E_TEST_MODE=go_sdk uv run --project airflow-e2e-tests pytest \\
        tests/airflow_e2e_tests/go_sdk_tests/test_go_sdk_packing.py -xvs

``conftest._setup_go_sdk_integration`` packs the example bundle and the two test bundles, and these tests
read what the packer wrote. A packed bundle carries a manifest with its digests and its SDK, and no list
of the Dags and tasks it registers: the Dag processor asks the bundle for its task handlers instead.

The packer runs no bundle, and the pack in the setup enforces it: the Go SDK's ``Serve`` fails when a bundle
is started without ``--comm`` and ``--logs``, so a packer that started one would fail the pack.
"""

from __future__ import annotations

import yaml

from airflow_e2e_tests.constants import (
    GO_SDK_BIN_PATH,
    GO_SDK_BUNDLE_NAME,
    GO_SDK_ROOT_PATH,
    GO_TEST_BUNDLE_ARTIFACTS,
    GO_TEST_BUNDLE_BUILD_PATH,
    GO_TEST_BUNDLE_ROOT_PATH,
    LANG_SDK_NATIVE_TOOLCHAIN,
)
from airflow_e2e_tests.e2e_test_utils.go_toolchain import run_go


def test_packed_bundles_record_no_dag_inventory():
    """The manifest of each packed bundle has its digests and its SDK, and no ``dags`` mapping."""
    bundles = [
        (GO_SDK_ROOT_PATH, GO_SDK_BIN_PATH / GO_SDK_BUNDLE_NAME),
        *((GO_TEST_BUNDLE_ROOT_PATH, GO_TEST_BUNDLE_BUILD_PATH / name) for name in GO_TEST_BUNDLE_ARTIFACTS),
    ]
    for module, bundle in bundles:
        completed = run_go(
            ["tool", "airflow-go-pack", "inspect", bundle], module=module, native=LANG_SDK_NATIVE_TOOLCHAIN
        )
        manifest = yaml.safe_load(completed.stdout)

        assert "dags" not in manifest, f"{bundle.name} lists the Dags it registers: {manifest}"
        assert manifest["digests"]["integrity"], manifest
        assert manifest["digests"]["cache"], manifest
        assert manifest["sdk"]["language"] == "go", manifest
        assert manifest["sdk"]["supervisor_schema_version"], manifest
