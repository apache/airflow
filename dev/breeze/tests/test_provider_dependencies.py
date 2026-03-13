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

import pytest

from airflow_breeze.utils.provider_dependencies import (
    get_related_providers,
)


def test_get_downstream_only():
    related_providers = get_related_providers(
        "trino", upstream_dependencies=False, downstream_dependencies=True
    )
    assert {"openlineage", "google", "common.sql", "common.compat"} == related_providers


def test_get_upstream_only():
    related_providers = get_related_providers(
        "trino", upstream_dependencies=True, downstream_dependencies=False
    )
    assert {"mysql", "google"} == related_providers


def test_both():
    related_providers = get_related_providers(
        "trino", upstream_dependencies=True, downstream_dependencies=True
    )
    assert {"openlineage", "google", "mysql", "common.sql", "common.compat"} == related_providers


def test_none():
    with pytest.raises(ValueError, match=r".*must be.*"):
        get_related_providers("trino", upstream_dependencies=False, downstream_dependencies=False)
