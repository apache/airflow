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

OPERATORS_BASE = "airflow.providers.apache.polaris.operators.base"


@pytest.fixture
def management_hook():
    """The (autospecced) ``PolarisManagementHook`` instance that Management API operators create."""
    with mock.patch(f"{OPERATORS_BASE}.PolarisManagementHook", autospec=True) as hook_class:
        yield hook_class.return_value


@pytest.fixture
def catalog_hook():
    """The (autospecced) ``PolarisCatalogHook`` instance that catalog API operators create."""
    with mock.patch(f"{OPERATORS_BASE}.PolarisCatalogHook", autospec=True) as hook_class:
        hook = hook_class.return_value
        hook.resolve_catalog.side_effect = lambda catalog: catalog or "analytics"
        yield hook
