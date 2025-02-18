#
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

from airflow.providers.standard.hooks.filesystem import FSHook

pytestmark = pytest.mark.db_test


class TestFSHook:
    def test_get_ui_field_behaviour(self):
        fs_hook = FSHook()
        assert fs_hook.get_ui_field_behaviour() == {
            "hidden_fields": ["host", "schema", "port", "login", "password", "extra"],
            "relabeling": {},
            "placeholders": {},
        }

    def test_get_path(self):
        fs_hook = FSHook(fs_conn_id="fs_default")

        assert fs_hook.get_path() == "/"
