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

from airflow.providers.fab.www.security.permissions import ACTION_CAN_EDIT
from airflow.providers.fab.www.utils import get_fab_action_from_method_map, get_session_lifetime_config

from tests_common.test_utils.config import conf_vars


class TestMethodToFabActionMap:
    def test_patch_maps_to_can_edit(self):
        """PATCH should resolve to the same FAB action as PUT (can_edit)."""
        method_map = get_fab_action_from_method_map()
        assert method_map["PATCH"] == ACTION_CAN_EDIT
        assert method_map["PUT"] == ACTION_CAN_EDIT


class TestUpdatedConfigNames:
    @conf_vars({("fab", "session_lifetime_minutes"): "43200"})
    def test_config_val_is_default(self):
        session_lifetime_config = get_session_lifetime_config()
        assert session_lifetime_config == 43200

    @conf_vars({("fab", "session_lifetime_minutes"): "43201"})
    def test_config_val_is_not_default(self):
        session_lifetime_config = get_session_lifetime_config()
        assert session_lifetime_config == 43201

    @conf_vars({("fab", "session_lifetime_days"): ""})
    def test_uses_updated_session_timeout_config_by_default(self):
        session_lifetime_config = get_session_lifetime_config()
        default_timeout_minutes = 30 * 24 * 60
        assert session_lifetime_config == default_timeout_minutes
