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

import json
from datetime import datetime

import pytest
from apache_polaris.sdk.catalog.models.applicable_policy import ApplicablePolicy
from apache_polaris.sdk.catalog.models.policy import Policy
from apache_polaris.sdk.catalog.models.table_identifier import TableIdentifier

from airflow.providers.apache.polaris.operators.policy import (
    PolarisAttachPolicyOperator,
    PolarisCreatePolicyOperator,
    PolarisDetachPolicyOperator,
    PolarisDropPolicyOperator,
    PolarisGetApplicablePoliciesOperator,
)
from airflow.providers.common.compat.sdk import DAG, AirflowFailException

from tests_common.test_utils.compat import DagSerialization

TARGET = {"type": "namespace", "path": ["sales"]}


def _policy(name, policy_type, content, inherited=False):
    return ApplicablePolicy.from_dict(
        {
            "policy-type": policy_type,
            "inheritable": True,
            "name": name,
            "version": 0,
            "content": json.dumps(content),
            "inherited": inherited,
            "namespace": ["sales"],
        }
    )


EXPIRY = _policy(
    "expiry", "system.snapshot-expiry", {"version": "2025-02-03", "enable": True}, inherited=True
)
COMPACT_OFF = _policy("compact", "system.data-compaction", {"version": "2025-02-03", "enable": False})
ORPHANS = _policy(
    "orphans", "system.orphan-file-removal", {"enable": True, "locations": ["s3://lake/sales/t1"]}
)


class TestPolicyOperators:
    def test_create_defaults_to_update_and_parses_content(self, catalog_hook):
        catalog_hook.create_policy.return_value = Policy.from_dict(
            {
                "policy-type": "system.snapshot-expiry",
                "inheritable": True,
                "name": "expiry",
                "version": 2,
                "content": '{"enable": true}',
            }
        )
        result = PolarisCreatePolicyOperator(
            task_id="t",
            namespace="sales",
            policy_name="expiry",
            policy_type="system.snapshot-expiry",
            content={"enable": True},
        ).execute({})
        catalog_hook.create_policy.assert_called_once_with(
            "sales",
            "expiry",
            "system.snapshot-expiry",
            content={"enable": True},
            description=None,
            catalog=None,
            if_exists="update",
        )
        assert result == {
            "name": "expiry",
            "type": "system.snapshot-expiry",
            "description": None,
            "inheritable": True,
            "version": 2,
            "content": {"enable": True},
        }

    def test_drop_attach_detach(self, catalog_hook):
        target = {"type": "table-like", "path": ["sales", "orders"]}
        PolarisDropPolicyOperator(task_id="a", namespace="sales", policy_name="p", detach_all=True).execute(
            {}
        )
        PolarisAttachPolicyOperator(task_id="b", namespace="sales", policy_name="p", target=target).execute(
            {}
        )
        PolarisDetachPolicyOperator(task_id="c", namespace="sales", policy_name="p", target=target).execute(
            {}
        )
        catalog_hook.drop_policy.assert_called_once_with(
            "sales", "p", catalog=None, detach_all=True, if_missing="skip"
        )
        catalog_hook.attach_policy.assert_called_once_with(
            "sales", "p", target, parameters=None, catalog=None
        )
        catalog_hook.detach_policy.assert_called_once_with(
            "sales", "p", target, catalog=None, if_missing="skip"
        )


class TestGetApplicablePolicies:
    def test_single_target_filters_disabled(self, catalog_hook):
        catalog_hook.get_applicable_policies.return_value = [EXPIRY, COMPACT_OFF]
        result = PolarisGetApplicablePoliciesOperator(task_id="t", namespace="sales", table="orders").execute(
            {}
        )
        catalog_hook.get_applicable_policies.assert_called_once_with(
            catalog="analytics", namespace="sales", table="orders", policy_type=None
        )
        assert [p["name"] for p in result] == ["expiry"]
        assert result[0]["inherited"] is True
        assert result[0]["defined_in"] == ["sales"]

    def test_enabled_only_false_keeps_disabled(self, catalog_hook):
        catalog_hook.get_applicable_policies.return_value = [EXPIRY, COMPACT_OFF]
        result = PolarisGetApplicablePoliciesOperator(task_id="t", enabled_only=False).execute({})
        assert [p["name"] for p in result] == ["expiry", "compact"]

    def test_enabled_only_keeps_policies_without_an_enable_key(self, catalog_hook):
        catalog_hook.get_applicable_policies.return_value = [
            _policy("custom", "acme.retention", {"days": 30}),
            COMPACT_OFF,
        ]
        result = PolarisGetApplicablePoliciesOperator(task_id="t").execute({})
        assert [p["name"] for p in result] == ["custom"]

    def test_one_type_is_filtered_server_side(self, catalog_hook):
        catalog_hook.get_applicable_policies.return_value = [EXPIRY]
        PolarisGetApplicablePoliciesOperator(task_id="t", policy_types=["system.snapshot-expiry"]).execute({})
        assert (
            catalog_hook.get_applicable_policies.call_args.kwargs["policy_type"] == "system.snapshot-expiry"
        )

    def test_several_types_are_filtered_locally(self, catalog_hook):
        catalog_hook.get_applicable_policies.return_value = [EXPIRY, ORPHANS]
        result = PolarisGetApplicablePoliciesOperator(
            task_id="t", policy_types=["system.snapshot-expiry", "system.data-compaction"]
        ).execute({})
        assert catalog_hook.get_applicable_policies.call_args.kwargs["policy_type"] is None
        assert [p["name"] for p in result] == ["expiry"]

    def test_per_table(self, catalog_hook):
        catalog_hook.list_tables.return_value = [
            TableIdentifier(namespace=["sales"], name="t1"),
            TableIdentifier(namespace=["sales"], name="t2"),
        ]
        catalog_hook.get_applicable_policies.side_effect = lambda **kw: (
            [EXPIRY, ORPHANS] if kw["table"] == "t1" else [EXPIRY, COMPACT_OFF]
        )
        catalog_hook.get_table_location.return_value = "s3://lake/sales/t1"
        result = PolarisGetApplicablePoliciesOperator(
            task_id="t", catalog="lake", namespace="sales", per_table=True
        ).execute({})
        assert [(i["table"], i["policy"]["name"]) for i in result] == [
            ("t1", "expiry"),
            ("t1", "orphans"),
            ("t2", "expiry"),
        ]
        assert all(i["catalog"] == "lake" and i["namespace"] == ["sales"] for i in result)
        assert "table_location" not in result[0]
        assert result[1]["table_location"] == "s3://lake/sales/t1"
        catalog_hook.get_table_location.assert_called_once_with(["sales"], "t1", catalog="lake")
        json.dumps(result)

    @pytest.mark.parametrize(
        ("kwargs", "match"),
        [
            ({"per_table": True}, "namespace is required"),
            ({"table": "t"}, "namespace is required"),
            ({"namespace": "s", "table": "t", "per_table": True}, "either table or per_table"),
        ],
    )
    def test_invalid_arguments_fail_when_the_dag_is_parsed(self, kwargs, match):
        with pytest.raises(ValueError, match=match):
            PolarisGetApplicablePoliciesOperator(task_id="t", **kwargs)

    def test_invalid_namespace_fails_without_retry(self, catalog_hook):
        op = PolarisGetApplicablePoliciesOperator(task_id="t", namespace="sales..eu", per_table=True)
        with pytest.raises(AirflowFailException, match="Invalid namespace"):
            op.execute({})

    def test_one_policy_type_as_a_string(self, catalog_hook):
        catalog_hook.get_applicable_policies.return_value = [EXPIRY, ORPHANS]
        result = PolarisGetApplicablePoliciesOperator(
            task_id="t", policy_types="system.snapshot-expiry"
        ).execute({})
        assert (
            catalog_hook.get_applicable_policies.call_args.kwargs["policy_type"] == "system.snapshot-expiry"
        )
        assert [p["name"] for p in result] == ["expiry"]

    def test_policy_types_rendered_as_text_fail(self, catalog_hook):
        op = PolarisGetApplicablePoliciesOperator(task_id="t", policy_types="['system.snapshot-expiry']")
        with pytest.raises(AirflowFailException, match="render_template_as_native_obj"):
            op.execute({})


class TestEndToEnd:
    """One operator through the real hook and SDK against the fake server."""

    def test_get_applicable_policies_per_table(self, fake_polaris, polaris_conn):
        cat_api = "/api/catalog"
        fake_polaris.route(
            "GET",
            f"{cat_api}/v1/lake/namespaces/sales/tables",
            (200, {"identifiers": [{"namespace": ["sales"], "name": "t1"}]}),
        )
        fake_polaris.route(
            "GET",
            f"{cat_api}/polaris/v1/lake/applicable-policies",
            (200, {"applicable-policies": [EXPIRY.to_dict()]}),
        )
        op = PolarisGetApplicablePoliciesOperator(
            task_id="t",
            namespace="sales",
            per_table=True,
            polaris_conn_id=polaris_conn(default_catalog="lake"),
        )
        result = op.execute({})
        assert result == [
            {
                "catalog": "lake",
                "namespace": ["sales"],
                "table": "t1",
                "policy": {
                    "name": "expiry",
                    "type": "system.snapshot-expiry",
                    "description": None,
                    "inheritable": True,
                    "version": 0,
                    "content": {"version": "2025-02-03", "enable": True},
                    "inherited": True,
                    "defined_in": ["sales"],
                },
            }
        ]

    def test_attaching_over_another_policy_of_the_type_fails_without_retry(self, fake_polaris, polaris_conn):
        fake_polaris.route(
            "PUT",
            "/api/catalog/polaris/v1/lake/namespaces/sales/policies/expiry/mappings",
            (
                409,
                {
                    "error": {
                        "message": "The policy mapping of same type (system.snapshot-expiry) already exists",
                        "type": "PolicyMappingAlreadyExistsException",
                        "code": 409,
                    }
                },
            ),
        )
        op = PolarisAttachPolicyOperator(
            task_id="t",
            namespace="sales",
            policy_name="expiry",
            target={"type": "namespace", "path": ["sales"]},
            polaris_conn_id=polaris_conn(default_catalog="lake"),
        )
        with pytest.raises(AirflowFailException, match="same type"):
            op.execute({})


def test_operators_serialize():
    with DAG("polaris_policy_operators", schedule=None, start_date=datetime(2026, 1, 1)) as dag:
        PolarisCreatePolicyOperator(
            task_id="create",
            namespace="sales",
            policy_name="expiry",
            policy_type="system.snapshot-expiry",
            content={"version": "2025-02-03", "enable": True, "config": {"tag": "{{ ds }}"}},
        )
        PolarisDropPolicyOperator(task_id="drop", namespace="sales", policy_name="expiry")
        PolarisAttachPolicyOperator(task_id="attach", namespace="sales", policy_name="expiry", target=TARGET)
        PolarisDetachPolicyOperator(task_id="detach", namespace="sales", policy_name="expiry", target=TARGET)
        PolarisGetApplicablePoliciesOperator(
            task_id="applicable", namespace="sales", per_table=True, policy_types=["system.snapshot-expiry"]
        )
    restored = DagSerialization.from_dict(DagSerialization.to_dict(dag))
    assert sorted(restored.task_ids) == sorted(dag.task_ids)
    assert restored.get_task("create").content["config"] == {"tag": "{{ ds }}"}
