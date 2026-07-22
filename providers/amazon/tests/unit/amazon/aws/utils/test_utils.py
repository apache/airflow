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

import datetime
import logging
from unittest import mock

import pytest
import tenacity
from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.utils import (
    _StringCompareEnum,
    build_resource_in_use_retry_args,
    datetime_to_epoch,
    datetime_to_epoch_ms,
    datetime_to_epoch_us,
    get_airflow_version,
    get_botocore_version,
    is_resource_in_use_error,
)

DT = datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)
EPOCH = 946_684_800


class EnumTest(_StringCompareEnum):
    FOO = "FOO"


def test_datetime_to_epoch():
    assert datetime_to_epoch(DT) == EPOCH


def test_datetime_to_epoch_ms():
    assert datetime_to_epoch_ms(DT) == EPOCH * 1000


def test_datetime_to_epoch_us():
    assert datetime_to_epoch_us(DT) == EPOCH * 1_000_000


def test_get_airflow_version():
    assert len(get_airflow_version()) == 3


def test_str_enum():
    assert EnumTest.FOO == "FOO"
    assert EnumTest.FOO.value == "FOO"


def test_botocore_version():
    pytest.importorskip("botocore", reason="`botocore` not installed")

    botocore_version = get_botocore_version()
    assert len(botocore_version) == 3
    assert isinstance(botocore_version[0], int), "botocore major version expected to be an integer"
    assert isinstance(botocore_version[1], int), "botocore minor version expected to be an integer"
    assert isinstance(botocore_version[2], int), "botocore patch version expected to be an integer"


def _run_with_retry(call, **overrides):
    retry_args = {**build_resource_in_use_retry_args(logging.getLogger()), **overrides}
    for attempt in tenacity.Retrying(**retry_args):
        with attempt:
            return call()


@pytest.mark.parametrize(
    ("exception", "expected"),
    [
        pytest.param(
            ClientError({"Error": {"Code": "ResourceInUseException"}}, "DeleteCluster"),
            True,
            id="resource_in_use",
        ),
        pytest.param(
            ClientError({"Error": {"Code": "ResourceNotFoundException"}}, "DeleteCluster"),
            False,
            id="other_client_error",
        ),
        pytest.param(ValueError("boom"), False, id="non_client_error"),
    ],
)
def test_is_resource_in_use_error(exception, expected):
    assert is_resource_in_use_error(exception) is expected


@mock.patch("time.sleep", return_value=None)
def test_build_resource_in_use_retry_args_retries_then_succeeds(mock_sleep):
    in_use = ClientError({"Error": {"Code": "ResourceInUseException"}}, "DeleteCluster")
    call = mock.Mock(side_effect=[in_use, in_use, "ok"])

    assert _run_with_retry(call) == "ok"
    assert call.call_count == 3


@mock.patch("time.sleep", return_value=None)
def test_build_resource_in_use_retry_args_reraises_when_stop_reached(mock_sleep):
    in_use = ClientError({"Error": {"Code": "ResourceInUseException"}}, "DeleteCluster")
    call = mock.Mock(side_effect=in_use)

    # Override the stop so exhaustion is reached without waiting out the real timeout.
    with pytest.raises(ClientError) as exc_info:
        _run_with_retry(call, stop=tenacity.stop_after_attempt(3))

    assert exc_info.value is in_use
    assert call.call_count == 3


@mock.patch("time.sleep", return_value=None)
def test_build_resource_in_use_retry_args_does_not_retry_other_errors(mock_sleep):
    other = ClientError({"Error": {"Code": "ResourceNotFoundException"}}, "DeleteCluster")
    call = mock.Mock(side_effect=other)

    with pytest.raises(ClientError):
        _run_with_retry(call)

    assert call.call_count == 1
