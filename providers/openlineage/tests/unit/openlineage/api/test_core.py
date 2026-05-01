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

from airflow.providers.openlineage.api import emit, is_openlineage_active

_CORE = "airflow.providers.openlineage.api.core"


def test_is_openlineage_active_true_when_enabled_and_listener_present():
    with (
        mock.patch(f"{_CORE}.conf.is_disabled", return_value=False),
        mock.patch(f"{_CORE}.get_openlineage_listener", return_value=mock.MagicMock()),
    ):
        assert is_openlineage_active() is True


def test_is_openlineage_active_false_when_disabled():
    with mock.patch(f"{_CORE}.conf.is_disabled", return_value=True):
        assert is_openlineage_active() is False


def test_is_openlineage_active_false_when_listener_missing():
    with (
        mock.patch(f"{_CORE}.conf.is_disabled", return_value=False),
        mock.patch(f"{_CORE}.get_openlineage_listener", return_value=None),
    ):
        assert is_openlineage_active() is False


def test_emit_forwards_to_adapter():
    event = mock.MagicMock()
    with (
        mock.patch(f"{_CORE}.conf.is_disabled", return_value=False),
        mock.patch(f"{_CORE}.get_openlineage_listener") as listener_factory,
    ):
        listener = mock.MagicMock()
        listener_factory.return_value = listener
        emit(event)
        listener.adapter.emit.assert_called_once_with(event)


def test_emit_noop_when_disabled():
    event = mock.MagicMock()
    with (
        mock.patch(f"{_CORE}.conf.is_disabled", return_value=True),
        mock.patch(f"{_CORE}.get_openlineage_listener") as listener_factory,
    ):
        emit(event)
        listener_factory.assert_not_called()
