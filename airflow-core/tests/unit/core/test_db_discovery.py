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

import contextlib
import logging
import os
import shutil
import socket
import time

import pytest
from sqlalchemy import text

from airflow import settings
from airflow.utils import db_discovery
from airflow.utils.db_discovery import DbDiscoveryStatus

log = logging.getLogger(__name__)


def dispose_connection_pool():
    """Dispose any cached sockets so that the next query will force a new connect."""
    settings.engine.dispose()
    # Wait for SqlAlchemy.
    time.sleep(0.5)


def make_db_test_call():
    """
    Create a session and execute a query.

    It will establish a new connection if there isn't one available.
    New connections use DNS lookup.
    """
    from airflow.utils.session import create_session

    with create_session() as session:
        session.execute(text("SELECT 1"))


def assert_query_raises_exc(expected_error_msg: str, expected_status: str, expected_retry_num: int):
    with pytest.raises(socket.gaierror, match=expected_error_msg):
        make_db_test_call()

    assert len(db_discovery.db_health_status) == 2

    assert db_discovery.db_health_status[0] == expected_status
    assert db_discovery.db_retry_count == expected_retry_num


@pytest.mark.backend("postgres")
class TestDbDiscoveryIntegration:
    @pytest.fixture
    def patch_getaddrinfo_for_eai_fail(self, monkeypatch):
        import socket

        def always_fail(*args, **kwargs):
            # The error message isn't important, as long as the error code is EAI_FAIL.
            raise socket.gaierror(socket.EAI_FAIL, "permanent failure")

        monkeypatch.setattr(socket, "getaddrinfo", always_fail)

    def test_dns_resolution_blip(self):
        os.environ["AIRFLOW__DATABASE__CHECK_DB_DISCOVERY"] = "True"

        resolv_file = "/etc/resolv.conf"
        resolv_backup = "/tmp/resolv.conf.bak"

        # Back up the original file so that it can later be restored.
        shutil.copy(resolv_file, resolv_backup)

        try:
            # Replace the IP with a bad resolver.
            with open(resolv_file, "w", encoding="utf-8") as fh:
                fh.write("nameserver 10.255.255.1\noptions timeout:1 attempts:1 ndots:0\n")

            # New connection + DNS lookup.
            dispose_connection_pool()
            assert_query_raises_exc(
                expected_error_msg="Temporary failure in name resolution",
                expected_status=DbDiscoveryStatus.TEMPORARY_ERROR,
                expected_retry_num=3,
            )

        finally:
            # Reset the values for the next tests.
            db_discovery.db_health_status = (DbDiscoveryStatus.OK, 0.0)
            db_discovery.db_retry_count = 0

            # Restore the original file.
            with contextlib.suppress(Exception):
                shutil.copy(resolv_backup, resolv_file)

    def test_permanent_dns_failure(self, patch_getaddrinfo_for_eai_fail):
        os.environ["AIRFLOW__DATABASE__CHECK_DB_DISCOVERY"] = "True"

        try:
            # New connection + DNS lookup.
            dispose_connection_pool()
            assert_query_raises_exc(
                expected_error_msg="permanent failure",
                expected_status=DbDiscoveryStatus.PERMANENT_ERROR,
                expected_retry_num=0,
            )

        finally:
            # Reset the values for the next tests.
            db_discovery.db_health_status = (DbDiscoveryStatus.OK, 0.0)
            db_discovery.db_retry_count = 0

    def test_invalid_hostname_in_config(self):
        os.environ["AIRFLOW__DATABASE__CHECK_DB_DISCOVERY"] = "True"
        os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = (
            "postgresql+psycopg2://postgres:airflow@invalid/airflow"
        )

        try:
            # New connection + DNS lookup.
            dispose_connection_pool()
            assert_query_raises_exc(
                expected_error_msg="Name or service not known",
                expected_status=DbDiscoveryStatus.UNKNOWN_HOSTNAME,
                expected_retry_num=0,
            )
        finally:
            os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = (
                "postgresql+psycopg2://postgres:airflow@postgres/airflow"
            )

            # Reset the values for the next tests.
            db_discovery.db_health_status = (DbDiscoveryStatus.OK, 0.0)
            db_discovery.db_retry_count = 0

    @pytest.mark.parametrize(
        "check_enabled",
        [
            pytest.param(True, id="check-enabled"),
            pytest.param(False, id="check-disabled"),
        ],
    )
    def test_no_errors(self, check_enabled: bool):
        os.environ["AIRFLOW__DATABASE__CHECK_DB_DISCOVERY"] = str(check_enabled)

        dispose_connection_pool()
        make_db_test_call()

        # No status checks and no retries.
        assert db_discovery.db_health_status[0] == DbDiscoveryStatus.OK
        assert db_discovery.db_retry_count == 0
