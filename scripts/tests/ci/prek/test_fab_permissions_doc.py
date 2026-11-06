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

import textwrap
from unittest import mock

import fab_permissions_doc


def test_entity_maps_reads_views_mapped_by_assignment(tmp_path):
    source = tmp_path / "fab_auth_manager.py"
    source.write_text(
        textwrap.dedent(
            """
            _MAP_ACCESS_VIEW_TO_FAB_RESOURCE_TYPE = {AccessView.JOBS: RESOURCE_JOB}
            if DAG_BUNDLES_ACCESS_VIEW is not None:
                _MAP_ACCESS_VIEW_TO_FAB_RESOURCE_TYPE[DAG_BUNDLES_ACCESS_VIEW] = RESOURCE_DAG_BUNDLE
            """
        )
    )
    with mock.patch.object(fab_permissions_doc, "AUTH_MANAGER_PY", source):
        _, view_map = fab_permissions_doc._entity_maps()

    assert view_map == {"JOBS": "RESOURCE_JOB", "DAG_BUNDLES": "RESOURCE_DAG_BUNDLE"}
