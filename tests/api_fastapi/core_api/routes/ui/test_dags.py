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

from tests.api_fastapi.core_api.routes.public.test_dags import TestDagEndpoint


class TestRecentDagRuns(TestDagEndpoint):
    def test_recent_dag_runs(self, test_client, query_params, expected_total_entries, expected_ids):
        response = test_client.get("/ui/dags/recent_dag_runs", params=query_params)
        assert response.status_code == 200
        print(response.json())
