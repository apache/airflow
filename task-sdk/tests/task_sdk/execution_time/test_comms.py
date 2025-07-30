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

import uuid
from socket import socketpair

import msgspec
import pytest

from airflow.sdk.execution_time.comms import BundleInfo, StartupDetails, _ResponseFrame
from airflow.sdk.execution_time.task_runner import CommsDecoder
from airflow.utils import timezone


class TestCommsDecoder:
    """Test the communication between the subprocess and the "supervisor"."""

    @pytest.mark.usefixtures("disable_capturing")
    def test_recv_StartupDetails(self):
        r, w = socketpair()

        msg = {
            "type": "StartupDetails",
            "ti": {
                "id": uuid.UUID("4d828a62-a417-4936-a7a6-2b3fabacecab"),
                "task_id": "a",
                "try_number": 1,
                "run_id": "b",
                "dag_id": "c",
            },
            "ti_context": {
                "dag_run": {
                    "dag_id": "c",
                    "run_id": "b",
                    "logical_date": "2024-12-01T01:00:00Z",
                    "data_interval_start": "2024-12-01T00:00:00Z",
                    "data_interval_end": "2024-12-01T01:00:00Z",
                    "start_date": "2024-12-01T01:00:00Z",
                    "run_after": "2024-12-01T01:00:00Z",
                    "end_date": None,
                    "run_type": "manual",
                    "conf": None,
                    "consumed_asset_events": [],
                },
                "max_tries": 0,
                "should_retry": False,
                "variables": None,
                "connections": None,
            },
            "file": "/dev/null",
            "start_date": "2024-12-01T01:00:00Z",
            "dag_rel_path": "/dev/null",
            "bundle_info": {"name": "any-name", "version": "any-version"},
        }
        bytes = msgspec.msgpack.encode(_ResponseFrame(0, msg, None))
        w.sendall(len(bytes).to_bytes(4, byteorder="big") + bytes)

        decoder = CommsDecoder(socket=r, log=None)

        msg = decoder._get_response()
        assert isinstance(msg, StartupDetails)
        assert msg.ti.id == uuid.UUID("4d828a62-a417-4936-a7a6-2b3fabacecab")
        assert msg.ti.task_id == "a"
        assert msg.ti.dag_id == "c"
        assert msg.dag_rel_path == "/dev/null"
        assert msg.bundle_info == BundleInfo(name="any-name", version="any-version")
        assert msg.start_date == timezone.datetime(2024, 12, 1, 1)
