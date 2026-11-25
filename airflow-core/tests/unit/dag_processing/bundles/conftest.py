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

import pytest

from tests_common.test_utils.config import conf_vars


@pytest.fixture
def isolate_bundle_storage(tmp_path):
    """Keep a test's bundle cache and version tracking files under its ``tmp_path``."""
    with (
        conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}),
        # The tracking folder is resolved once at import time, not from the config above.
        mock.patch(
            "airflow.dag_processing.bundles.base.STALE_BUNDLE_TRACKING_FOLDER", tmp_path / "bundles/_tracking"
        ),
    ):
        yield
