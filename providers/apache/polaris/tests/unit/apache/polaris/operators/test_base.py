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

import inspect

import pytest

from airflow.providers.apache.polaris.exceptions import (
    PolarisApiError,
    PolarisAuthenticationError,
    PolarisConflictError,
    PolarisConnectionError,
    PolarisForbiddenError,
    PolarisNotFoundError,
)
from airflow.providers.apache.polaris.hooks.polaris import PolarisCatalogHook, PolarisManagementHook
from airflow.providers.apache.polaris.operators import catalog, namespace, policy, principal, rbac, table
from airflow.providers.apache.polaris.operators.base import PolarisBaseOperator
from airflow.providers.apache.polaris.operators.catalog import PolarisCreateCatalogOperator
from airflow.providers.apache.polaris.operators.policy import PolarisDropPolicyOperator
from airflow.providers.common.compat.sdk import AirflowFailException

OPERATORS = sorted(
    (
        cls
        for module in (catalog, namespace, policy, principal, rbac, table)
        for name, cls in inspect.getmembers(module, inspect.isclass)
        if name.endswith("Operator") and cls.__module__ == module.__name__ and "Base" not in name
    ),
    key=lambda cls: cls.__name__,
)

CATALOG = {
    "type": "INTERNAL",
    "name": "analytics",
    "properties": {"default-base-location": "s3://lake/analytics"},
    "storageConfigInfo": {"storageType": "S3", "roleArn": "arn:aws:iam::1:role/r"},
}


class TestBase:
    def test_hook_uses_conn_id_and_class(self):
        op = PolarisCreateCatalogOperator(task_id="t", catalog=CATALOG, polaris_conn_id="my_polaris")
        assert isinstance(op.hook, PolarisManagementHook)
        assert op.hook.polaris_conn_id == "my_polaris"
        assert op.hook is op.hook  # cached

    def test_catalog_operators_use_the_catalog_hook(self):
        op = PolarisDropPolicyOperator(task_id="t", namespace="n", policy_name="p", polaris_conn_id="c")
        assert isinstance(op.hook, PolarisCatalogHook)

    def test_default_conn_id(self):
        assert PolarisDropPolicyOperator(task_id="t", namespace="n", policy_name="p").polaris_conn_id == (
            "polaris_default"
        )

    @pytest.mark.parametrize("operator", OPERATORS, ids=lambda cls: cls.__name__)
    def test_every_operator_templates_the_connection_id(self, operator):
        assert "polaris_conn_id" in operator.template_fields

    def test_connection_id_is_rendered_before_the_hook_is_created(self):
        op = PolarisDropPolicyOperator(
            task_id="t", namespace="n", policy_name="p", polaris_conn_id="{{ params.conn_id }}"
        )
        op.render_template_fields({"params": {"conn_id": "polaris_prod"}})
        assert op.hook.polaris_conn_id == "polaris_prod"


class TestFailWithoutRetry:
    @pytest.mark.parametrize(
        "error",
        [
            PolarisForbiddenError(403, "Principal 'p' is not authorized for op CREATE_CATALOG"),
            PolarisNotFoundError(404, "Catalog does not exist"),
            PolarisApiError(400, "Invalid catalog"),
            PolarisAuthenticationError("Token request failed with HTTP 401. Check Client ID."),
            PolarisConnectionError("Host is empty"),
        ],
    )
    def test_permanent_errors_fail_the_task_without_retry(self, management_hook, error):
        management_hook.create_catalog.side_effect = error
        with pytest.raises(AirflowFailException, match="Not retrying") as info:
            PolarisCreateCatalogOperator(task_id="t", catalog=CATALOG).execute({})
        assert str(error) in str(info.value)
        assert ". Not retrying" in str(info.value)  # one sentence break, with or without a final period
        assert ".. Not retrying" not in str(info.value)
        assert info.value.__cause__ is error

    @pytest.mark.parametrize(
        "error",
        [
            PolarisApiError(503, "Service unavailable"),
            PolarisConflictError(409, "Concurrent update"),
            PolarisAuthenticationError("Token endpoint unreachable", retryable=True),
        ],
    )
    def test_transient_errors_are_raised_for_retries(self, management_hook, error):
        management_hook.create_catalog.side_effect = error
        with pytest.raises(type(error)) as info:
            PolarisCreateCatalogOperator(task_id="t", catalog=CATALOG, if_exists="fail").execute({})
        assert info.value is error

    def test_other_errors_are_untouched(self, management_hook):
        error = RuntimeError("boom")
        management_hook.create_catalog.side_effect = error
        with pytest.raises(RuntimeError) as info:
            PolarisCreateCatalogOperator(task_id="t", catalog=CATALOG).execute({})
        assert info.value is error

    def test_applies_to_every_subclass(self):
        class MyOperator(PolarisBaseOperator):
            def execute(self, context):
                raise PolarisForbiddenError(403, "denied")

        with pytest.raises(AirflowFailException, match="denied"):
            MyOperator(task_id="t").execute({})
