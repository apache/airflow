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
Integration tests for Asset Event operations.

These tests validate the Execution API endpoints for Asset Event operations:
- get(): Get asset events by name, URI, or alias
"""

from __future__ import annotations

from airflow.sdk.api.datamodels._generated import AssetEventsResponse
from task_sdk_tests import console


def test_asset_event_get(sdk_client_for_assets, asset_test_setup):
    """Test getting asset events by name."""
    console.print("[yellow]Getting asset events by name...")

    response = sdk_client_for_assets.asset_events.get(name=asset_test_setup["name"])

    console.print(" Asset Event Get Response ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(response).__name__}")
    console.print(f"[bright_blue]Number of Events:[/] {len(response.asset_events)}")
    assert isinstance(response, AssetEventsResponse)
    assert len(response.asset_events) >= 1
    event = response.asset_events[0]
    console.print(f"[bright_blue]First Event ID:[/] {event.id}")
    console.print(f"[bright_blue]First Event Asset Name:[/] {event.asset.name}")
    console.print(f"[bright_blue]First Event Asset URI:[/] {event.asset.uri}")
    console.print(f"[bright_blue]First Event Timestamp:[/] {event.timestamp}")
    console.print("=" * 72)
    assert event.asset.name == asset_test_setup["name"]
    assert event.asset.uri == asset_test_setup["uri"]

    console.print("[green]✅ Asset event get test passed!")


def test_asset_event_get_not_found(sdk_client_for_assets):
    """Test getting asset events for non-existent asset."""
    console.print("[yellow]Getting asset events for non-existent asset...")

    response = sdk_client_for_assets.asset_events.get(name="non_existent_asset_name")

    console.print(" Asset Event Get (Not Found) Response ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(response).__name__}")
    console.print(f"[bright_blue]Number of Events:[/] {len(response.asset_events)}")
    console.print("=" * 72)

    assert isinstance(response, AssetEventsResponse)
    assert len(response.asset_events) == 0, "Expected empty list for non-existent asset"
    console.print("[green]✅ Asset event get (not found) test passed!")


def test_asset_event_get_by_uri(sdk_client_for_assets, asset_test_setup):
    """
    Test getting asset events by URI.

    Expected: AssetEventsResponse with events
    Endpoint: GET /execution/asset-events/by-asset?uri={uri}
    """
    console.print("[yellow]Getting asset events by URI...")

    response = sdk_client_for_assets.asset_events.get(uri=asset_test_setup["uri"])

    console.print(" Asset Event Get Response ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(response).__name__}")
    console.print(f"[bright_blue]Number of Events:[/] {len(response.asset_events)}")

    assert isinstance(response, AssetEventsResponse)
    assert len(response.asset_events) >= 1

    event = response.asset_events[0]

    console.print(f"[bright_blue]First Event ID:[/] {event.id}")
    console.print(f"[bright_blue]First Event Asset Name:[/] {event.asset.name}")
    console.print(f"[bright_blue]First Event Asset URI:[/] {event.asset.uri}")
    console.print(f"[bright_blue]First Event Timestamp:[/] {event.timestamp}")
    console.print("=" * 72)

    assert event.asset.name == asset_test_setup["name"]
    assert event.asset.uri == asset_test_setup["uri"]

    console.print("[green]✅ Asset event get (URI) test passed!")


def test_asset_event_get_by_alias(sdk_client_for_assets, asset_test_setup):
    """
    Test getting asset events by alias name.

    Expected: AssetEventsResponse with events
    Endpoint: GET /execution/asset-events/by-asset-alias?name={alias_name}
    """
    console.print("[yellow]Getting asset events by alias...")

    response = sdk_client_for_assets.asset_events.get(alias_name=asset_test_setup["alias_name"])

    console.print(" Asset Event Get Response ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(response).__name__}")
    console.print(f"[bright_blue]Number of Events:[/] {len(response.asset_events)}")

    assert isinstance(response, AssetEventsResponse)
    assert len(response.asset_events) >= 1

    event = response.asset_events[0]

    console.print(f"[bright_blue]First Event ID:[/] {event.id}")
    console.print(f"[bright_blue]First Event Asset Name:[/] {event.asset.name}")
    console.print(f"[bright_blue]First Event Asset URI:[/] {event.asset.uri}")
    console.print(f"[bright_blue]First Event Timestamp:[/] {event.timestamp}")
    console.print("=" * 72)

    assert event.asset.name == asset_test_setup["name"]
    assert event.asset.uri == asset_test_setup["uri"]

    console.print("[green]✅ Asset event get (alias) test passed!")
