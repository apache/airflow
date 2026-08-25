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
import types
from unittest import mock

import pytest

RESOURCE_DETAILS_MODULE = "airflow.api_fastapi.auth.managers.models.resource_details"
ACCESS_VIEW_SHIM_MODULE = "airflow.providers.common.compat.security.access_view"


@pytest.mark.parametrize("member_name", ["IMPORT_ERRORS_ALL", "AUDIT_LOGS_ALL"])
def test_resolves_to_the_core_access_view_member_or_none(member_name):
    """The shim mirrors the running core: the ``AccessView`` member on a core that
    defines it (>= 3.4.0), otherwise ``None``. Kept version-agnostic so it holds
    across the whole provider compatibility matrix, including cores that predate the
    member or lack ``api_fastapi`` entirely.
    """
    shim = importlib.import_module(ACCESS_VIEW_SHIM_MODULE)

    try:
        from airflow.api_fastapi.auth.managers.models.resource_details import AccessView

        expected = getattr(AccessView, member_name, None)
    except ImportError:
        expected = None

    assert getattr(shim, f"{member_name}_ACCESS_VIEW") is expected


def test_is_none_on_older_core_without_the_member():
    """On a core that predates the new ``AccessView`` members the shim resolves to ``None``."""

    class _AccessViewWithoutNewMembers:
        """Stand-in for an older core AccessView that lacks the new members."""

    fake_resource_details = types.ModuleType(RESOURCE_DETAILS_MODULE)
    fake_resource_details.AccessView = _AccessViewWithoutNewMembers

    with mock.patch.dict(sys.modules, {RESOURCE_DETAILS_MODULE: fake_resource_details}):
        reloaded = importlib.reload(importlib.import_module(ACCESS_VIEW_SHIM_MODULE))
        assert reloaded.IMPORT_ERRORS_ALL_ACCESS_VIEW is None
        assert reloaded.AUDIT_LOGS_ALL_ACCESS_VIEW is None

    # Restore the module against the real core so later imports see the real value.
    importlib.reload(importlib.import_module(ACCESS_VIEW_SHIM_MODULE))
