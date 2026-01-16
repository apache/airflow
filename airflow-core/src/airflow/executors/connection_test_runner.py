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

"""Connection test runner for executing connection tests on workers."""

from __future__ import annotations

import logging
import os
import random
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from typing import TYPE_CHECKING

from airflow.models.connection import Connection
from airflow.models.crypto import get_fernet
from airflow.secrets.environment_variables import CONN_ENV_PREFIX
from airflow.utils.strings import get_random_string

if TYPE_CHECKING:
    from airflow.executors.workloads import TestConnection

log = logging.getLogger(__name__)

MAX_RETRIES = 3
RETRY_DELAY_BASE = 2


def _run_test_connection(conn: Connection, conn_type: str) -> tuple[bool, str]:
    """Run the actual connection test."""
    log.info("Testing connection of type %s", conn_type)
    status, message = conn.test_connection()
    log.info("Connection test result: status=%s, message=%s", status, message)
    return status, message


def execute_connection_test(
    encrypted_connection_uri: str,
    conn_type: str,
    timeout: int = 60,
) -> tuple[bool, str]:
    """Execute a connection test with timeout enforcement."""
    try:
        fernet = get_fernet()
        connection_uri = fernet.decrypt(encrypted_connection_uri.encode("utf-8")).decode("utf-8")
    except Exception as e:
        log.exception("Failed to decrypt connection URI")
        return False, f"Failed to decrypt connection URI: {e}"

    # Some hooks look up the connection in __init__, so we need to export it
    transient_conn_id = get_random_string()
    conn_env_var = f"{CONN_ENV_PREFIX}{transient_conn_id.upper()}"

    try:
        conn = Connection(conn_id=transient_conn_id, uri=connection_uri)
        os.environ[conn_env_var] = connection_uri

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_run_test_connection, conn, conn_type)
            try:
                status, message = future.result(timeout=timeout)
                return status, message
            except FuturesTimeoutError:
                log.warning("Connection test timed out after %s seconds", timeout)
                return False, f"Connection test timed out after {timeout} seconds"
    except Exception:
        log.exception("Connection test failed with exception")
        return False, f"Connection test failed: {traceback.format_exc()}"
    finally:
        os.environ.pop(conn_env_var, None)


def execute_connection_test_workload(workload: TestConnection) -> tuple[bool, str]:
    """Execute a connection test from a TestConnection workload."""
    return execute_connection_test(
        encrypted_connection_uri=workload.encrypted_connection_uri,
        conn_type=workload.conn_type,
        timeout=workload.timeout,
    )


def report_connection_test_result(
    request_id: str,
    success: bool,
    message: str,
    server_url: str,
    token: str,
) -> bool:
    """Report connection test result back to the API server with retry logic."""
    import httpx

    state = "success" if success else "failed"
    result_url = f"{server_url.rstrip('/')}/connection-tests/{request_id}/state"
    payload = {"state": state, "result_status": success, "result_message": message}

    log.info("Reporting connection test result: request_id=%s, state=%s", request_id, state)

    for attempt in range(MAX_RETRIES):
        try:
            with httpx.Client() as client:
                response = client.patch(
                    result_url,
                    json=payload,
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=30.0,
                )
                response.raise_for_status()
            log.info("Connection test result reported: request_id=%s", request_id)
            return True
        except httpx.HTTPStatusError as e:
            log.warning(
                "HTTP error reporting result: request_id=%s, status=%s, attempt=%s/%s",
                request_id,
                e.response.status_code,
                attempt + 1,
                MAX_RETRIES,
            )
            if 400 <= e.response.status_code < 500 and e.response.status_code != 429:
                return False
        except Exception:
            log.exception("Failed to report result: request_id=%s", request_id)

        if attempt < MAX_RETRIES - 1:
            delay = RETRY_DELAY_BASE * (2**attempt) * (0.5 + random.random())
            time.sleep(delay)

    log.error("Failed to report result after retries: request_id=%s", request_id)
    return False
