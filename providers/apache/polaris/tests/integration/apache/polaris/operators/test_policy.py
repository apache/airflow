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

import pytest

from airflow.providers.apache.polaris.exceptions import PolarisConflictError
from airflow.providers.apache.polaris.hooks.polaris import PolarisCatalogHook
from airflow.providers.apache.polaris.operators.policy import PolarisGetApplicablePoliciesOperator

pytestmark = pytest.mark.integration("polaris")


def test_policies_resolve_per_table(conn, catalog_name):
    pyiceberg = pytest.importorskip("pyiceberg")
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField

    conn_id = conn(default_catalog=catalog_name)
    hook = PolarisCatalogHook(conn_id)
    iceberg = hook.get_iceberg_catalog()  # exercises the pyiceberg credential flow with the realm header
    assert pyiceberg
    iceberg.create_namespace("sales")
    iceberg.create_table("sales.orders", schema=Schema(NestedField(1, "id", LongType(), required=False)))

    content = {"version": "2025-02-03", "enable": True, "config": {"min_snapshot_to_keep": 1}}
    hook.create_policy("sales", "expiry", "system.snapshot-expiry", content=content)
    hook.attach_policy("sales", "expiry", {"type": "namespace", "path": ["sales"]})
    hook.attach_policy("sales", "expiry", {"type": "namespace", "path": ["sales"]})  # idempotent

    hook.update_policy("sales", "expiry", description="Keep one snapshot")
    newer = {**content, "config": {"min_snapshot_to_keep": 5}}
    updated = hook.create_policy(
        "sales", "expiry", "system.snapshot-expiry", content=newer, if_exists="update"
    )
    assert json.loads(updated.content) == newer
    assert updated.description == "Keep one snapshot"  # not cleared by a content-only update

    # A target holds one inheritable policy per type: attaching a second one is refused.
    hook.create_policy("sales", "expiry2", "system.snapshot-expiry", content=content)
    with pytest.raises(PolarisConflictError) as info:
        hook.attach_policy("sales", "expiry2", {"type": "namespace", "path": ["sales"]})
    assert info.value.retryable is False
    assert hook.drop_policy("sales", "expiry2")

    items = PolarisGetApplicablePoliciesOperator(
        task_id="t", namespace="sales", per_table=True, polaris_conn_id=conn_id
    ).execute({})
    assert [(i["table"], i["policy"]["name"], i["policy"]["inherited"]) for i in items] == [
        ("orders", "expiry", True)
    ]
    assert items[0]["policy"]["content"] == newer

    assert hook.detach_policy("sales", "expiry", {"type": "namespace", "path": ["sales"]})
    assert hook.drop_policy("sales", "expiry")
