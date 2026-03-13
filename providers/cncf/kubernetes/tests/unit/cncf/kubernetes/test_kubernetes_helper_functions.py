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

import re
from unittest import mock

import pytest
from kubernetes.client.rest import ApiException as SyncApiException
from kubernetes_asyncio.client.exceptions import ApiException as AsyncApiException
from urllib3.exceptions import HTTPError

from airflow.providers.cncf.kubernetes.kubernetes_helper_functions import (
    KubernetesApiException,
    WaitRetryAfterOrExponential,
    _should_retry_api,
    create_unique_id,
)

pod_name_regex = r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$"


class DummyRetryState:
    def __init__(self, exception=None):
        self.outcome = mock.Mock() if exception is not None else None
        if self.outcome:
            self.outcome.exception = mock.Mock(return_value=exception)


def test_should_retry_api():
    exc = HTTPError()
    assert _should_retry_api(exc)

    exc = KubernetesApiException()
    assert _should_retry_api(exc)

    exc = SyncApiException(status=500)
    assert _should_retry_api(exc)

    exc = AsyncApiException(status=500)
    assert _should_retry_api(exc)

    exc = SyncApiException(status=404)
    assert not _should_retry_api(exc)

    exc = AsyncApiException(status=404)
    assert not _should_retry_api(exc)


class TestWaitRetryAfterOrExponential:
    @pytest.mark.no_wait_patch_disabled
    @pytest.mark.parametrize(("exception"), [SyncApiException, AsyncApiException])
    def test_call_with_retry_after_header(self, exception):
        exc = exception(status=429)
        exc.headers = {"Retry-After": "15"}
        retry_state = DummyRetryState(exception=exc)
        wait = WaitRetryAfterOrExponential()(retry_state)
        assert wait == 15

    @pytest.mark.no_wait_patch_disabled
    @pytest.mark.parametrize(
        ("attempt_number", "expected_wait", "exception"),
        [(1, 1, SyncApiException), (4, 8, AsyncApiException)],
    )
    def test_call_without_retry_after_header(self, attempt_number, expected_wait, exception):
        exc = exception(status=409)
        retry_state = DummyRetryState(exception=exc)
        retry_state.attempt_number = attempt_number
        wait = WaitRetryAfterOrExponential()(retry_state)
        assert wait == expected_wait


class TestCreateUniqueId:
    @pytest.mark.parametrize(
        ("val", "expected"),
        [
            ("task-id", "task-id"),  # no problem
            ("task_id", "task-id"),  # underscores
            ("---task.id---", "task-id"),  # dots
            (".task.id", "task-id"),  # leading dot invalid
            ("**task.id", "task-id"),  # leading dot invalid
            ("-90Abc*&", "90abc"),  # invalid ends
            ("90AçLbˆˆç˙ßß˜˜˙c*a", "90aclb-c-ssss-c-a"),  # weird unicode
        ],
    )
    def test_create_pod_id_task_only(self, val, expected):
        actual = create_unique_id(task_id=val, unique=False)
        assert actual == expected
        assert re.match(pod_name_regex, actual)

    @pytest.mark.parametrize(
        ("val", "expected"),
        [
            ("dag-id", "dag-id"),  # no problem
            ("dag_id", "dag-id"),  # underscores
            ("---dag.id---", "dag-id"),  # dots
            (".dag.id", "dag-id"),  # leading dot invalid
            ("**dag.id", "dag-id"),  # leading dot invalid
            ("-90Abc*&", "90abc"),  # invalid ends
            ("90AçLbˆˆç˙ßß˜˜˙c*a", "90aclb-c-ssss-c-a"),  # weird unicode
        ],
    )
    def test_create_pod_id_dag_only(self, val, expected):
        actual = create_unique_id(dag_id=val, unique=False)
        assert actual == expected
        assert re.match(pod_name_regex, actual)

    @pytest.mark.parametrize(
        ("dag_id", "task_id", "expected"),
        [
            ("dag-id", "task-id", "dag-id-task-id"),  # no problem
            ("dag_id", "task_id", "dag-id-task-id"),  # underscores
            ("dag.id", "task.id", "dag-id-task-id"),  # dots
            (".dag.id", ".---task.id", "dag-id-task-id"),  # leading dot invalid
            ("**dag.id", "**task.id", "dag-id-task-id"),  # leading dot invalid
            ("-90Abc*&", "-90Abc*&", "90abc-90abc"),  # invalid ends
            ("90AçLbˆˆç˙ßß˜˜˙c*a", "90AçLbˆˆç˙ßß˜˜˙c*a", "90aclb-c-ssss-c-a-90aclb-c-ssss-c-a"),  # ugly
        ],
    )
    def test_create_pod_id_dag_and_task(self, dag_id, task_id, expected):
        actual = create_unique_id(dag_id=dag_id, task_id=task_id, unique=False)
        assert actual == expected
        assert re.match(pod_name_regex, actual)

    def test_create_pod_id_dag_too_long_with_suffix(self):
        actual = create_unique_id("0" * 254)
        assert len(actual) == 63
        assert re.match(r"0{54}-[a-z0-9]{8}", actual)
        assert re.match(pod_name_regex, actual)

    def test_create_pod_id_dag_too_long_non_unique(self):
        actual = create_unique_id("0" * 254, unique=False)
        assert len(actual) == 63
        assert re.match(r"0{63}", actual)
        assert re.match(pod_name_regex, actual)

    @pytest.mark.parametrize("unique", [True, False])
    @pytest.mark.parametrize("length", [25, 100, 200, 300])
    def test_create_pod_id(self, length, unique):
        """Test behavior of max_length and unique."""
        dag_id = "dag-dag-dag-dag-dag-dag-dag-dag-dag-dag-dag-dag-dag-dag-dag-dag-"
        task_id = "task-task-task-task-task-task-task-task-task-task-task-task-task-task-task-task-task-"
        actual = create_unique_id(
            dag_id=dag_id,
            task_id=task_id,
            max_length=length,
            unique=unique,
        )
        base = f"{dag_id}{task_id}".strip("-")
        if unique:
            assert actual[:-9] == base[: length - 9].strip("-")
            assert re.match(r"-[a-z0-9]{8}", actual[-9:])
        else:
            assert actual == base[:length]
