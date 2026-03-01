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
"""Tests for pool commands."""

from __future__ import annotations

import json
from unittest import mock

import pytest

from airflowctl.api.client import Client
from airflowctl.api.datamodels.generated import (
    BulkActionOnExistence,
    BulkBodyPoolBody,
    BulkCreateActionPoolBody,
)
from airflowctl.ctl.commands import pool_command


@pytest.fixture
def mock_client():
    """Create a mock client."""
    with mock.patch("airflowctl.api.client.get_client") as mock_get_client:
        client = mock.MagicMock(spec=Client)
        mock_get_client.return_value.__enter__.return_value = client
        yield client


class TestPoolImportCommand:
    """Test cases for pool import command."""

    def test_import_missing_file(self, mock_client, tmp_path):
        """Test import with missing file."""
        non_existent = tmp_path / "non_existent.json"
        with pytest.raises(SystemExit, match=f"Missing pools file {non_existent}"):
            pool_command.import_(mock.MagicMock(file=non_existent))

    def test_import_invalid_json(self, mock_client, tmp_path):
        """Test import with invalid JSON file."""
        invalid_json = tmp_path / "invalid.json"
        invalid_json.write_text("invalid json")
        with pytest.raises(SystemExit, match="Invalid json file"):
            pool_command.import_(mock.MagicMock(file=invalid_json))

    def test_import_invalid_pool_config(self, mock_client, tmp_path):
        """Test import with invalid pool configuration."""
        invalid_pool = tmp_path / "invalid_pool.json"
        invalid_pool.write_text(json.dumps([{"invalid": "config"}]))
        with pytest.raises(SystemExit, match="Invalid pool configuration: {'invalid': 'config'}"):
            pool_command.import_(mock.MagicMock(file=invalid_pool))

    def test_import_success(self, mock_client, tmp_path, capsys):
        """Test successful pool import."""
        pools_file = tmp_path / "pools.json"
        pools_data = [
            {
                "name": "test_pool",
                "slots": 1,
                "description": "Test pool",
                "include_deferred": True,
            }
        ]
        pools_file.write_text(json.dumps(pools_data))

        # Mock the bulk response with the correct structure
        mock_response = mock.MagicMock()
        mock_response.success = ["test_pool"]
        mock_response.errors = []

        mock_bulk_builder = mock.MagicMock()
        mock_bulk_builder.create = mock_response

        mock_client.pools.bulk.return_value = mock_bulk_builder

        pool_command.import_(mock.MagicMock(file=pools_file))

        # Verify bulk operation was called with correct parameters
        mock_client.pools.bulk.assert_called_once()
        call_args = mock_client.pools.bulk.call_args[1]
        assert isinstance(call_args["pools"], BulkBodyPoolBody)
        assert len(call_args["pools"].actions) == 1
        action = call_args["pools"].actions[0]
        assert isinstance(action, BulkCreateActionPoolBody)
        assert action.action == "create"
        assert action.action_on_existence == BulkActionOnExistence.FAIL
        assert len(action.entities) == 1
        assert action.entities[0].name == "test_pool"
        assert action.entities[0].slots == 1
        assert action.entities[0].description == "Test pool"
        assert action.entities[0].include_deferred is True

        # Update the assertion to match the actual output format
        captured = capsys.readouterr()
        assert str(["test_pool"]) in captured.out


class TestPoolExportCommand:
    """Test cases for pool export command."""

    def test_export_json_to_file(self, mock_client, tmp_path, capsys):
        """Test successful pool export to file with json output."""
        export_file = tmp_path / "export.json"
        # Create a proper pool object with dictionary attributes instead of MagicMock
        pool = {
            "name": "test_pool",
            "slots": 1,
            "description": "Test pool",
            "include_deferred": True,
            "occupied_slots": 0,
            "running_slots": 0,
            "queued_slots": 0,
            "scheduled_slots": 0,
            "open_slots": 1,
            "deferred_slots": 0,
        }
        # Create a mock response with proper dictionary attributes
        mock_pools = mock.MagicMock()
        mock_pools.pools = [type("Pool", (), pool)()]
        mock_pools.total_entries = 1
        mock_client.pools.list.return_value = mock_pools

        pool_command.export(mock.MagicMock(file=export_file, output="json"))

        # Verify the exported file content
        exported_data = json.loads(export_file.read_text())
        assert len(exported_data) == 1
        assert exported_data[0]["name"] == "test_pool"
        assert exported_data[0]["slots"] == 1
        assert exported_data[0]["description"] == "Test pool"
        assert exported_data[0]["include_deferred"] is True

        # Verify output message
        captured = capsys.readouterr()
        expected_output = f"Exported {len(exported_data)} pool(s) to {export_file}"
        assert expected_output in captured.out.replace("\n", "")

    def test_export_non_json_output(self, mock_client, tmp_path, capsys):
        """Test pool export with non-json output format."""
        # Create a proper dictionary structure
        mock_pool = {
            "name": "test_pool",
            "slots": 1,
            "description": "Test pool",
            "include_deferred": True,
            "occupied_slots": 0,
            "running_slots": 0,
            "queued_slots": 0,
            "scheduled_slots": 0,
            "open_slots": 1,
            "deferred_slots": 0,
        }
        # Create a mock response with a proper pools attribute
        mock_pools = mock.MagicMock()
        mock_pools.pools = [mock.MagicMock(**mock_pool)]
        mock_pools.total_entries = 1
        mock_client.pools.list.return_value = mock_pools

        pool_command.export(mock.MagicMock(file=tmp_path / "unused.json", output="table"))

        # Verify console output contains the raw dict
        captured = capsys.readouterr()
        assert "test_pool" in captured.out
        assert "slots" in captured.out
        assert "description" in captured.out
        assert "include_deferred" in captured.out

    def test_export_failure(self, mock_client, tmp_path):
        """Test pool export with API failure."""
        export_file = tmp_path / "export.json"
        mock_client.pools.list.side_effect = Exception("API Error")

        with pytest.raises(SystemExit, match="Failed to export pools: API Error"):
            pool_command.export(mock.MagicMock(file=export_file, output="json"))
