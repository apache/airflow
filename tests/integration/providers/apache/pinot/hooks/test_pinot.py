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

from unittest import mock

import pytest

from airflow.providers.apache.pinot.hooks.pinot import PinotDbApiHook


@pytest.mark.integration("pinot")
class TestPinotDbApiHookIntegration:
    # This test occasionally fail in the CI. Re-run this test if it failed after timeout but only once.
    @pytest.mark.flaky(reruns=1, reruns_delay=30)
    @mock.patch.dict("os.environ", AIRFLOW_CONN_PINOT_BROKER_DEFAULT="pinot://pinot:8000/")
    def test_should_return_records(self):
        hook = PinotDbApiHook()
        sql = "select playerName from baseballStats  ORDER BY playerName limit 5"
        records = hook.get_records(sql)
        assert [["A. Harry"], ["A. Harry"], ["Aaron"], ["Aaron Albert"], ["Aaron Albert"]] == records
