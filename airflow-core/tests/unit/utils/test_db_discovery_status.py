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
from __future__ import annotations

import socket

import pytest

from airflow.utils import db_discovery
from airflow.utils.db_discovery import DbDiscoveryStatus


class TestDbDiscoveryStatus:
    @pytest.mark.parametrize(
        "retry, expected_sleep_time",
        [
            pytest.param(0, 0.5, id="attempt-1"),
            pytest.param(1, 1, id="attempt-2"),
            pytest.param(2, 2, id="attempt-3"),
            pytest.param(3, 4, id="attempt-4"),
            pytest.param(4, 8, id="attempt-5"),
            pytest.param(5, 15, id="attempt-6"),
            pytest.param(6, 15, id="attempt-7"),
        ],
    )
    def test_get_sleep_time(self, retry: int, expected_sleep_time: float):
        sleep = db_discovery.get_sleep_time(retry, 0.5, 15)
        assert sleep == expected_sleep_time

    @pytest.mark.parametrize(
        "error_code, expected_status",
        [
            (socket.EAI_FAIL, DbDiscoveryStatus.PERMANENT_ERROR),
            (socket.EAI_AGAIN, DbDiscoveryStatus.TEMPORARY_ERROR),
            (socket.EAI_NONAME, DbDiscoveryStatus.UNKNOWN_HOSTNAME),
            (socket.EAI_SYSTEM, DbDiscoveryStatus.UNKNOWN_ERROR),
        ],
    )
    def test_check_dns_resolution_with_retries(self, monkeypatch, error_code, expected_status):
        def raise_exc(*args, **kwargs):
            # The error message isn't important because the validation is based on the error code.
            raise socket.gaierror(error_code, "patched failure")

        monkeypatch.setattr(socket, "getaddrinfo", raise_exc)

        status, err = db_discovery._check_dns_resolution_with_retries("some_host", 3, 0.5, 5)

        assert status == expected_status
        assert isinstance(err, socket.gaierror)
        assert err.errno == error_code

        # If the failure is temporary, then there must be retries.
        if error_code == socket.EAI_AGAIN:
            assert db_discovery.db_retry_count > 1
        else:
            assert db_discovery.db_retry_count == 0
