from __future__ import annotations

import os
from unittest.mock import patch
import pytest

from airflow.models import Connection
from plugins.wandb_provider.hooks.wandb import WandbHook

import wandb


@pytest.fixture
def mock_conn():
    conn = Connection(
        conn_id="wandb_conn",
        conn_type="wandb",
        password="test_api_key",
    )
    os.environ[f"AIRFLOW_CONN_{str(conn.conn_id).upper()}"] = conn.get_uri()
    return conn


@pytest.fixture
def mock_hook(mock_conn):
    with patch.object(WandbHook, "get_connection", return_value=mock_conn):
        return WandbHook(conn_id=mock_conn.conn_id)


class TestWandbHook:
    def test__set_env_variables(self, mock_hook):
        mock_hook._set_env_variables()

        assert os.environ["WANDB_API_KEY"] == "test_api_key"

    def test__unset_env_variables(self, mock_hook):
        os.environ["WANDB_API_KEY"] = "test_api_key"

        mock_hook._unset_env_variables()

        assert "WANDB_API_KEY" not in os.environ

    def test_get_conn(self, mock_hook):
        conn = mock_hook.get_conn()

        assert conn == wandb
