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

from airflow.providers.edge3.ctl.definition import (
    EDGE_COMMANDS,
    get_edge_airflowctl_commands,
)


class TestEdgeAirflowctlDefinition:
    def test_edge_commands_match_public_routes(self):
        # Only routes under worker_api/routes/ that are public-safe end up here.
        # Today that's just ``health``; ``ui.py`` is intentionally excluded and
        # the worker-protocol endpoints (worker/jobs/logs) require worker JWTs.
        assert sorted(c.name for c in EDGE_COMMANDS) == ["health"]

    def test_top_level_groups(self):
        groups = get_edge_airflowctl_commands()
        assert sorted(g.name for g in groups) == ["edge"]
