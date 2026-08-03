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

import importlib
import sys

import pytest


class TestSecretsMaskerDeprecationShim:
    """Regression tests for https://github.com/apache/airflow/issues/70875."""

    @pytest.fixture(autouse=True)
    def _fresh_import(self):
        # The module fires its deprecation warning at import time, so make sure
        # each test gets a fresh import instead of reusing sys.modules' cached copy.
        sys.modules.pop("airflow.sdk.execution_time.secrets_masker", None)
        yield
        sys.modules.pop("airflow.sdk.execution_time.secrets_masker", None)

    def test_warning_names_the_correct_module_for_mask_secret(self):
        """The deprecation warning must name airflow.sdk.log for mask_secret."""
        with pytest.warns() as record:
            importlib.import_module("airflow.sdk.execution_time.secrets_masker")

        messages = [str(w.message) for w in record.list]
        combined = " ".join(messages)

        assert "airflow.sdk.log" in combined
        assert "mask_secret" in combined
        assert "_shared.secrets_masker" in combined
