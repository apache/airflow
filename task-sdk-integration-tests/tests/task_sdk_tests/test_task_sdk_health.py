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

from task_sdk_tests import console


def test_task_sdk_health(sdk_client, task_sdk_api_version):
    """Test Task SDK health check using session setup."""
    console.print("[yellow]Making health check request...")
    response = sdk_client.get("health/ping")

    console.print(" Health Check Response ".center(72, "="))
    console.print(f"[bright_blue]Status Code:[/] {response.status_code}")
    console.print(f"[bright_blue]Response:[/] {response.json()}")
    console.print("=" * 72)

    assert response.status_code == 200
    assert response.json() == {"ok": ["airflow.api_fastapi.auth.tokens.JWTValidator"], "failing": {}}

    console.print("[green]✅ Task SDK health check passed!")
