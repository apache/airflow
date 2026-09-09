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

from airflow.api_fastapi.execution_api.versions import (
    MIN_VERSION_ONLY_FAILED_CLEAR,
    bundle,
)


def test_min_version_only_failed_clear_is_registered():
    """@AC-FR004-01 The advertised failed-only clear minimum maps to a registered API version."""
    registered = {version.value for version in bundle.versions}
    assert MIN_VERSION_ONLY_FAILED_CLEAR in registered


def test_min_version_only_failed_clear_is_supported_by_head():
    """@AC-FR004-02 The current head version is at least the failed-only clear minimum."""
    # bundle.versions is ordered newest-first; the first entry is head/latest.
    latest = bundle.versions[0].value
    assert latest >= MIN_VERSION_ONLY_FAILED_CLEAR
