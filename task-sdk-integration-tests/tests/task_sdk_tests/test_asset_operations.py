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
"""
Integration tests for Asset operations.

These tests validate the Execution API endpoints for Asset operations:
- get(): Get asset by name or URI
"""

from __future__ import annotations

from airflow.sdk.api.datamodels._generated import AssetResponse
from airflow.sdk.execution_time.comms import ErrorResponse
from task_sdk_tests import console


def test_asset_get_by_name(sdk_client_for_assets, asset_test_setup):
    """Test getting asset by name."""
    console.print("[yellow]Getting asset by name...")

    response = sdk_client_for_assets.assets.get(name=asset_test_setup["name"])

    console.print(" Asset Get By Name Response ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(response).__name__}")
    console.print(f"[bright_blue]Name:[/] {response.name}")
    console.print(f"[bright_blue]URI:[/] {response.uri}")
    console.print(f"[bright_blue]Group:[/] {response.group}")
    console.print("=" * 72)

    assert isinstance(response, AssetResponse)
    assert response.name == asset_test_setup["name"]
    assert response.uri == asset_test_setup["uri"]
    console.print("[green]Asset get by name test passed!")


def test_asset_get_by_name_not_found(sdk_client_for_assets):
    """Test getting non-existent asset by name."""
    console.print("[yellow]Getting non-existent asset by name...")

    response = sdk_client_for_assets.assets.get(name="non_existent_asset_name")

    console.print(" Asset Get (Not Found) Response ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(response).__name__}")
    console.print(f"[bright_blue]Error Type:[/] {response.error}")
    console.print(f"[bright_blue]Detail:[/] {response.detail}")
    console.print("=" * 72)

    assert isinstance(response, ErrorResponse)
    assert str(response.error).endswith("ASSET_NOT_FOUND")
    console.print("[green]Asset get by name (not found) test passed!")


def test_asset_get_by_uri(sdk_client_for_assets, asset_test_setup):
    """
    Test getting asset by URI.

    Expected: AssetResponse with asset details
    Endpoint: GET /execution/assets/by-uri?uri={uri}
    """
    console.print("[yellow]Getting asset by URI...")

    response = sdk_client_for_assets.assets.get(uri=asset_test_setup["uri"])

    console.print(" Asset Get By URI Response ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(response).__name__}")
    console.print(f"[bright_blue]Name:[/] {response.name}")
    console.print(f"[bright_blue]URI:[/] {response.uri}")
    console.print(f"[bright_blue]Group:[/] {response.group}")
    console.print("=" * 72)

    assert isinstance(response, AssetResponse)
    assert response.name == asset_test_setup["name"]
    assert response.uri == asset_test_setup["uri"]
    console.print("[green]Asset get by URI test passed!")


def test_asset_get_by_uri_not_found(sdk_client_for_assets):
    """
    Test getting non-existent asset by URI.

    Expected: ErrorResponse with ASSET_NOT_FOUND error
    Endpoint: GET /execution/assets/by-uri?uri={uri}
    """
    console.print("[yellow]Getting non-existent asset by URI...")

    response = sdk_client_for_assets.assets.get(uri="non_existent_asset_uri")

    console.print(" Asset Get (Not Found) Response ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(response).__name__}")
    console.print(f"[bright_blue]Error Type:[/] {response.error}")
    console.print(f"[bright_blue]Detail:[/] {response.detail}")
    console.print("=" * 72)

    assert isinstance(response, ErrorResponse)
    assert str(response.error).endswith("ASSET_NOT_FOUND")
    console.print("[green]Asset get by URI (not found) test passed!")
