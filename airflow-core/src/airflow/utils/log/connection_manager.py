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
"""Connection management for server-side operations like remote log reading."""

from __future__ import annotations

import contextlib
import logging
import os
from functools import lru_cache

from airflow.configuration import conf

log = logging.getLogger(__name__)


@lru_cache
def _get_remote_logging_connection_uri(conn_id: str) -> str | None:
    """
    Fetch and cache connection URI for remote logging.

    Similar to task-sdk supervisor pattern, but uses airflow-core connection access.
    """
    from airflow.models.connection import Connection

    try:
        conn = Connection.get_connection_from_secrets(conn_id)
        return conn.get_uri()
    except Exception:
        log.exception("Unable to retrieve remote logging connection %s", conn_id)
        return None


def _get_remote_log_conn_id() -> str | None:
    """Get the remote log connection ID from configuration."""
    return conf.get("logging", "remote_log_conn_id", fallback=None)


@contextlib.contextmanager
def with_remote_logging_connection():
    """
    Context manager to pre-fetch remote logging connection and set as environment variable.

    This follows the same pattern as task-sdk supervisor's _remote_logging_conn but uses
    airflow-core's connection access. When remote log handlers try to get connections,
    they'll find them in the environment variables instead of trying to use SUPERVISOR_COMMS.

    Usage:
        with with_remote_logging_connection():
            # Remote log handlers will find connections in env vars
            sources, logs = remote_io.read(path, ti)
    """
    conn_id = _get_remote_log_conn_id()
    if not conn_id:
        # No remote logging connection configured
        yield
        return

    # Get connection URI using server-side access
    conn_uri = _get_remote_logging_connection_uri(conn_id)
    if not conn_uri:
        log.warning("Could not fetch remote logging connection %s", conn_id)
        yield
        return

    env_key = f"AIRFLOW_CONN_{conn_id.upper()}"
    old_value = os.getenv(env_key)

    try:
        os.environ[env_key] = conn_uri
        log.debug("Set remote logging connection %s in environment", conn_id)
        yield
    finally:
        # Restore original environment state
        if old_value is None:
            if env_key in os.environ:
                del os.environ[env_key]
        else:
            os.environ[env_key] = old_value
