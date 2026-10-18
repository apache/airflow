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

import warnings
from unittest.mock import MagicMock, call

import pytest

from airflow.providers.common.compat import _resumable_job_mixin, sdk

from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS


class _Base:
    def __init__(self, **kwargs):
        self.base_kwargs = kwargs


class _JobOperator(_resumable_job_mixin.ResumableJobMixin, _Base):
    external_id_key = "job_id"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.calls = MagicMock(spec=["submit_job", "poll_until_complete", "get_job_result"])

    def submit_job(self, context):
        self.calls.submit_job(context)
        return "job-1"

    def poll_until_complete(self, external_id, context):
        self.calls.poll_until_complete(external_id, context)

    def get_job_result(self, external_id, context):
        self.calls.get_job_result(external_id, context)
        return "result"


def test_resumable_job_mixin_is_exported():
    assert "ResumableJobMixin" in sdk.__all__


@pytest.mark.skipif(not AIRFLOW_V_3_3_PLUS, reason="ResumableJobMixin arrived in Airflow 3.3")
def test_sdk_class_is_used_from_airflow_3_3():
    from airflow.sdk import ResumableJobMixin

    assert sdk.ResumableJobMixin is ResumableJobMixin


@pytest.mark.skipif(AIRFLOW_V_3_3_PLUS, reason="The SDK has ResumableJobMixin from Airflow 3.3")
def test_stand_in_is_used_below_airflow_3_3():
    assert sdk.ResumableJobMixin is _resumable_job_mixin.ResumableJobMixin


class TestStandIn:
    def test_durable_left_at_default_does_not_warn(self):
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            operator = _JobOperator(task_id="t")

        assert operator.durable is False
        assert operator.base_kwargs == {"task_id": "t"}

    @pytest.mark.parametrize("durable", [True, False])
    def test_explicit_durable_warns_and_is_disabled(self, durable):
        with pytest.warns(UserWarning, match="`durable` has no effect on Airflow versions below 3.3"):
            operator = _JobOperator(task_id="t", durable=durable)

        assert operator.durable is False
        assert operator.base_kwargs == {"task_id": "t"}

    def test_execute_resumable_submits_polls_and_returns_result(self):
        operator = _JobOperator(task_id="t")
        context = {}

        assert operator.execute_resumable(context) == "result"
        assert operator.calls.mock_calls == [
            call.submit_job(context),
            call.poll_until_complete("job-1", context),
            call.get_job_result("job-1", context),
        ]
