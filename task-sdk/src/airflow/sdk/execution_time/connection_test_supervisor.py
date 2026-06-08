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

import os
import time
import uuid

import structlog
from structlog.contextvars import bind_contextvars, clear_contextvars

from airflow.sdk._shared.observability.metrics import stats
from airflow.sdk.api.client import Client
from airflow.sdk.api.datamodels._generated import ConnectionTestState
from airflow.sdk.definitions.connection import Connection as SDKConnection
from airflow.sdk.exceptions import AirflowTaskTimeout
from airflow.sdk.execution_time.timeout import TimeoutPosix

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
    client = Client(base_url=server, token=token)

    bind_contextvars(connection_test_id=str(connection_test_id), connection_id=connection_id)
    log.info("Starting connection test", timeout=timeout)
    start = time.monotonic()

    try:
        r = client.connection_tests.get_connection(connection_test_id)

        conn = SDKConnection(
            conn_id=r.conn_id,
            conn_type=r.conn_type,
            host=r.host,
            login=r.login,
            password=r.password,
            schema=r.schema_,
            port=r.port,
            extra=r.extra,
        )
        key = f"AIRFLOW_CONN_{r.conn_id.upper()}"
        old_conn = os.getenv(key)
        old_context = os.getenv("_AIRFLOW_PROCESS_CONTEXT")

        os.environ[key] = conn.get_uri()
        # Set process context to "client" so that Connection deserialization uses SDK Connection class
        # which has from_uri() method, instead of core Connection class
        os.environ["_AIRFLOW_PROCESS_CONTEXT"] = "client"
        try:
            with (
                stats.timer("connection_test.hook_duration"),
                TimeoutPosix(
                    seconds=timeout,
                    error_message=f"Connection test timed out after {timeout}s",
                ),
            ):
                success, message = conn.test_connection()
        finally:
            if old_conn is None:
                del os.environ[key]
            else:
                os.environ[key] = old_conn

            if old_context is None:
                del os.environ["_AIRFLOW_PROCESS_CONTEXT"]
            else:
                os.environ["_AIRFLOW_PROCESS_CONTEXT"] = old_context

        state = ConnectionTestState.SUCCESS if success else ConnectionTestState.FAILED
        client.connection_tests.update_state(connection_test_id, state, message)
        stats.incr("connection_test.success" if success else "connection_test.failed")
        log.info(
            "Connection test finished",
            state=state.value,
            duration=round(time.monotonic() - start, 3),
        )
    except AirflowTaskTimeout:
        log.error(
            "Connection test timed out, marking failed",
            timeout=timeout,
            duration=round(time.monotonic() - start, 3),
        )
        stats.incr("connection_test.failed")
        client.connection_tests.update_state(
            connection_test_id,
            ConnectionTestState.FAILED,
            f"Connection test timed out after {timeout}s",
        )
    except Exception as e:
        log.exception(
            "Connection test failed unexpectedly",
            duration=round(time.monotonic() - start, 3),
        )
        stats.incr("connection_test.failed")
        client.connection_tests.update_state(
            connection_test_id,
            ConnectionTestState.FAILED,
            f"Connection test failed unexpectedly: {type(e).__name__}",
        )
    finally:
        clear_contextvars()

    return 0
