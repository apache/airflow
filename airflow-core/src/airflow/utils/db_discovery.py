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

import logging
import socket
import time

import tenacity
from sqlalchemy.engine.url import make_url

from airflow.configuration import conf

logger = logging.getLogger(__name__)


class DbDiscoveryStatus:
    """Enum with the return value for `check_db_discovery_if_needed()`."""

    # The hostname resolves.
    OK = "ok"
    # There has been some temporary DNS lookup blip and the connection will probably recover.
    # Causes: a dns timeout or a temporary network issue.
    TEMPORARY_ERROR = "dns_temporary_failure"
    # Unknown hostname or service, this is permanent and the connection can't be recovered.
    # Causes: a cmd or config typo, a hostname that doesn't exist.
    UNKNOWN_HOSTNAME = "unknown_hostname"
    # Unknown hostname or service, this is permanent and the connection can't be recovered.
    # Causes: Failed DNS server or config typo.
    PERMANENT_ERROR = "dns_permanent_failure"
    # Some other error.
    UNKNOWN_ERROR = "unknown_error"


# db status - how long ago it was retrieved
db_health_status: tuple[str, float] = (DbDiscoveryStatus.OK, 0.0)

# TODO: For now, this is used for testing
#  but it can also be used to add stats.
db_retry_count: int = 0


def _is_temporary_dns_error(ex: BaseException) -> bool:
    return isinstance(ex, socket.gaierror) and ex.errno == socket.EAI_AGAIN


def _check_dns_resolution_with_retries(
    host: str,
    retries: int,
    initial_retry_wait: float,
    max_retry_wait: float,
) -> tuple[str, BaseException | None]:
    global db_retry_count
    # Initialize to 0 in case it has another value from previous attempts.
    db_retry_count = 0

    def _before_sleep(retry_state: tenacity.RetryCallState) -> None:
        nonlocal host, retries
        global db_retry_count

        db_retry_count += 1
        logger.warning(
            "Temporary DNS failure for host '%s' (attempt %d/%d)",
            host,
            retry_state.attempt_number,
            retries,
        )

    # tenacity retries start counting from 1
    run_with_db_discovery_retries = tenacity.Retrying(
        retry=tenacity.retry_if_exception(_is_temporary_dns_error),
        stop=tenacity.stop_after_attempt(retries + 1),
        wait=tenacity.wait_exponential(
            multiplier=initial_retry_wait,
            max=max_retry_wait,
        ),
        before_sleep=_before_sleep,
        reraise=True,
    )

    try:
        for attempt in run_with_db_discovery_retries:
            with attempt:
                socket.getaddrinfo(host, None)
    except socket.gaierror as err:
        if err.errno == socket.EAI_AGAIN:
            return DbDiscoveryStatus.TEMPORARY_ERROR, err
        if err.errno == socket.EAI_NONAME:
            return DbDiscoveryStatus.UNKNOWN_HOSTNAME, err
        if err.errno == socket.EAI_FAIL:
            return DbDiscoveryStatus.PERMANENT_ERROR, err
        return DbDiscoveryStatus.UNKNOWN_ERROR, err

    return DbDiscoveryStatus.OK, None


def check_db_discovery_with_retries(retry_num: int, initial_retry_wait: float, max_retry_wait: float):
    global db_health_status

    # DNS check.
    url = make_url(conf.get("database", "sql_alchemy_conn"))
    host = url.host
    dns_status, dns_exc = _check_dns_resolution_with_retries(
        host, retry_num, initial_retry_wait, max_retry_wait
    )

    if dns_status != DbDiscoveryStatus.OK and dns_exc:
        logger.error("Database hostname '%s' failed DNS resolution: %s", host, dns_status)
        db_health_status = (dns_status, time.time())
        raise dns_exc

    db_health_status = (dns_status, time.time())
