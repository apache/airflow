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

from airflow.www.fab_security.sqla.models import Permission


def assert_db_permissions_equal_to_pairs(
    db_permissions: list[Permission], expected_permission_pairs: list[tuple[str, str]]
):
    permission_pairs = [(permission.action.name, permission.resource.name) for permission in db_permissions]
    assert sorted(permission_pairs) == sorted(expected_permission_pairs)
