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

from unittest.mock import Mock, patch

import pytest

from airflow.providers.keycloak.auth_manager.cache import KeycloakDagPermissionCache


@pytest.fixture
def user():
    mock_user = Mock()
    mock_user.get_id.return_value = "user_id"
    return mock_user


def _resolver_with_calls(calls, allow_ids: set[str]):
    def _resolver(method, user, dag_id, attributes):
        calls.append(dag_id)
        return dag_id in allow_ids

    return _resolver


@patch("airflow.providers.keycloak.auth_manager.cache.monotonic")
def test_filter_authorized_dag_ids_uses_cache(mock_monotonic, user):
    mock_monotonic.side_effect = [1.0, 1.1, 2.0]
    resolver_calls: list[str] = []
    cache = KeycloakDagPermissionCache(
        permission_resolver=_resolver_with_calls(resolver_calls, {"dag_a"}),
        permissions_cache_ttl_seconds=30,
    )

    dag_ids = {"dag_a", "dag_b"}

    first = cache.filter_authorized_dag_ids(dag_ids=dag_ids, user=user, method="GET", team_name=None)
    second = cache.filter_authorized_dag_ids(dag_ids=dag_ids, user=user, method="GET", team_name=None)

    assert first == {"dag_a"}
    assert second == {"dag_a"}
    assert resolver_calls == ["dag_a", "dag_b"]


@patch("airflow.providers.keycloak.auth_manager.cache.monotonic")
def test_filter_authorized_dag_ids_cache_expires(mock_monotonic, user):
    mock_monotonic.side_effect = [1.0, 1.1, 10.0, 10.1]
    resolver_calls: list[str] = []
    cache = KeycloakDagPermissionCache(
        permission_resolver=_resolver_with_calls(resolver_calls, {"dag_a"}),
        permissions_cache_ttl_seconds=5,
    )

    dag_ids = {"dag_a"}

    first = cache.filter_authorized_dag_ids(dag_ids=dag_ids, user=user, method="GET", team_name=None)
    second = cache.filter_authorized_dag_ids(dag_ids=dag_ids, user=user, method="GET", team_name=None)

    assert first == {"dag_a"}
    assert second == {"dag_a"}
    assert resolver_calls == ["dag_a", "dag_a"]


def test_schedule_dag_permission_warmup_submits_task(user):
    cache = KeycloakDagPermissionCache(
        permission_resolver=lambda method, user, dag_id, attributes: True,
        permissions_cache_ttl_seconds=30,
    )
    submit_mock = Mock()
    cache._dag_warmup_executor = Mock(submit=submit_mock)

    cache.schedule_dag_permission_warmup(user, method="GET")
    cache.schedule_dag_permission_warmup(user, method="GET")

    submit_mock.assert_called_once()
    scheduled_callable, scheduled_user, scheduled_method = submit_mock.call_args.args
    assert scheduled_callable == cache._warmup_user_dag_permissions
    assert scheduled_user == user
    assert scheduled_method == "GET"


def test_schedule_dag_permission_warmup_skipped_when_disabled(user):
    cache = KeycloakDagPermissionCache(
        permission_resolver=lambda method, user, dag_id, attributes: True,
        permissions_cache_ttl_seconds=0,
    )
    submit_mock = Mock()
    cache._dag_warmup_executor = Mock(submit=submit_mock)

    cache.schedule_dag_permission_warmup(user, method="GET")

    submit_mock.assert_not_called()
