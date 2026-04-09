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

from airflow.providers.common.ai.plugins.hitl_review import (
    HITLReviewPlugin,
    _get_access_dependencies,
    _get_plugin_fastapi_apps,
    _get_plugin_react_apps,
    hitl_review_app,
)

from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_PLUS


def test_version_guard_returns_empty_for_unsupported_airflow():
    assert _get_access_dependencies(method="GET", airflow_v_3_1_plus=False) == []
    assert _get_access_dependencies(method="PUT", airflow_v_3_1_plus=False) == []
    assert _get_plugin_fastapi_apps(airflow_v_3_1_plus=False, app=hitl_review_app) == []
    assert _get_plugin_react_apps(airflow_v_3_1_plus=False) == []


@pytest.mark.skipif(not AIRFLOW_V_3_1_PLUS, reason="Requires Airflow 3.1+")
def test_access_dependencies_enabled_on_supported_airflow():
    deps = _get_access_dependencies(method="GET", airflow_v_3_1_plus=True)
    assert len(deps) == 1


def test_hitl_review_plugin_registration_matches_airflow_version():
    if AIRFLOW_V_3_1_PLUS:
        assert len(HITLReviewPlugin.fastapi_apps) == 1
        assert len(HITLReviewPlugin.react_apps) == 1
    else:
        assert HITLReviewPlugin.fastapi_apps == []
        assert HITLReviewPlugin.react_apps == []
