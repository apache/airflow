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
"""Supervised execution of TestConnection workloads."""

from __future__ import annotations

import signal
import uuid

import structlog

from airflow.sdk.api.client import Client
from airflow.sdk.api.datamodels._generated import ConnectionTestState
from airflow.sdk.definitions.connection import Connection as SDKConnection
from airflow.sdk.execution_time.context import _preset_connections

__all__ = ["supervise_connection_test"]

log = structlog.get_logger(logger_name="connection_test_supervisor")


def supervise_connection_test(
    *,
    connection_test_id: uuid.UUID,
    connection_id: str,
    timeout: int,
    token: str,
    server: str,
) -> int:
    """Execute a connection test on the worker and report the result via the Execution API."""
    from airflow.models.connection import Connection

    client = Client(base_url=server, token=token)

    def _handle_timeout(signum, frame):
        raise TimeoutError(f"Connection test timed out after {timeout}s")

    signal.signal(signal.SIGALRM, _handle_timeout)
    signal.alarm(timeout)
    try:
        client.connection_tests.update_state(connection_test_id, ConnectionTestState.RUNNING)

        r = client.connection_tests.get_connection(connection_test_id)

        conn = Connection(
            conn_id=r.conn_id,
            conn_type=r.conn_type,
            host=r.host,
            login=r.login,
            password=r.password,
            schema=r.schema_,
            port=r.port,
            extra=r.extra,
        )
        preset = SDKConnection(
            conn_id=r.conn_id,
            conn_type=r.conn_type,
            host=r.host,
            login=r.login,
            password=r.password,
            schema=r.schema_,
            port=r.port,
            extra=r.extra,
        )
        preset_token = _preset_connections.set({preset.conn_id: preset})
        try:
            success, message = conn.test_connection()
        finally:
            _preset_connections.reset(preset_token)

        state = ConnectionTestState.SUCCESS if success else ConnectionTestState.FAILED
        client.connection_tests.update_state(connection_test_id, state, message)
    except TimeoutError:
        log.error(
            "Connection test timed out after %ds",
            timeout,
            connection_id=connection_id,
        )
        client.connection_tests.update_state(
            connection_test_id,
            ConnectionTestState.FAILED,
            f"Connection test timed out after {timeout}s",
        )
    except Exception as e:
        log.exception("Connection test failed unexpectedly", connection_id=connection_id)
        client.connection_tests.update_state(
            connection_test_id,
            ConnectionTestState.FAILED,
            f"Connection test failed unexpectedly: {type(e).__name__}",
        )
    finally:
        signal.alarm(0)

    return 0
