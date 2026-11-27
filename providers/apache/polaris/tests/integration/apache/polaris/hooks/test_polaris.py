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
import uuid

import pytest

from airflow.providers.apache.polaris.exceptions import (
    PolarisApiError,
    PolarisAuthenticationError,
    PolarisConflictError,
)
from airflow.providers.apache.polaris.hooks.polaris import (
    PolarisCatalogHook,
    PolarisHook,
    PolarisManagementHook,
)

from integration.apache.polaris import CLIENT_SECRET, REALM

pytestmark = pytest.mark.integration("polaris")


def test_test_connection(conn):
    assert PolarisHook(conn()).test_connection()[0]


def test_unknown_realm_is_reported(conn):
    with pytest.raises(PolarisAuthenticationError, match="rejected the realm"):
        PolarisHook(conn(realm="NO_SUCH_REALM")).get_token()


def test_ungranted_role_fails_on_use_not_on_token(conn):
    hook = PolarisManagementHook(conn(scope="PRINCIPAL_ROLE:no_such_role"))
    hook.get_token()  # Polaris issues the token...
    with pytest.raises(PolarisAuthenticationError, match="HTTP 401"):
        hook.list_catalogs()  # ...and rejects it when used.


def test_static_token(conn):
    token = PolarisHook(conn()).get_token()
    hook = PolarisManagementHook(conn("polaris_it_token", login=None, password=token, auth_type="token"))
    assert isinstance(hook.list_catalogs(), list)


def test_catalog_lifecycle_is_idempotent(conn, catalog_name, catalog_spec):
    hook = PolarisManagementHook(conn())
    again = hook.create_catalog(catalog_spec(catalog_name), if_exists="skip")
    assert again.name == catalog_name
    with pytest.raises(PolarisConflictError):
        hook.create_catalog(catalog_spec(catalog_name))
    updated = hook.update_catalog(catalog_name, properties={"owner": "data-eng"})
    assert updated.properties.to_dict()["owner"] == "data-eng"


def test_rbac(conn, catalog_name):
    hook = PolarisManagementHook(conn())
    suffix = uuid.uuid4().hex[:6]
    principal, principal_role, catalog_role = f"p_{suffix}", f"pr_{suffix}", f"cr_{suffix}"
    try:
        created = hook.create_principal(principal)
        assert hook.create_principal(principal, if_exists="skip").name == principal
        assert created.name == principal
        assert "credentials" not in created.to_dict()
        hook.create_principal_role(principal_role)
        hook.create_principal_role(principal_role, if_exists="skip")
        hook.create_catalog_role(catalog_name, catalog_role)
        assert hook.create_catalog_role(catalog_name, catalog_role, if_exists="skip").name == catalog_role
        hook.assign_principal_role(principal, principal_role)
        hook.assign_principal_role(principal, principal_role)  # idempotent
        hook.assign_catalog_role(principal_role, catalog_name, catalog_role)
        hook.assign_catalog_role(principal_role, catalog_name, catalog_role)  # idempotent
        grant = {"type": "catalog", "privilege": "TABLE_READ_DATA"}
        hook.add_grant(catalog_name, catalog_role, grant)
        assert grant in [g.to_dict() for g in hook.list_grants(catalog_name, catalog_role)]
        assert hook.revoke_grant(catalog_name, catalog_role, grant)
        # Revoking what is not granted: Polaris answers 400 GRANT_NOT_FOUND, treated as missing.
        assert hook.revoke_grant(catalog_name, catalog_role, grant, if_missing="skip") is False
        assert hook.revoke_catalog_role(principal_role, catalog_name, catalog_role) is True
        assert (
            hook.revoke_catalog_role(principal_role, catalog_name, catalog_role, if_missing="skip") is False
        )
        assert hook.revoke_principal_role(principal, principal_role) is True
        assert hook.revoke_principal_role(principal, principal_role, if_missing="skip") is False
        assert principal_role in [r.name for r in hook.list_principal_roles()]
        assert catalog_role in [r.name for r in hook.list_catalog_roles(catalog_name)]
        assert principal in [p.name for p in hook.list_principals()]
        assert hook.delete_catalog_role(catalog_name, catalog_role) is True
        assert hook.delete_principal_role(principal_role) is True
        assert hook.delete_principal(principal) is True
    finally:
        hook.revoke_catalog_role(principal_role, catalog_name, catalog_role, if_missing="skip")
        hook.delete_catalog_role(catalog_name, catalog_role, if_missing="skip")
        hook.delete_principal_role(principal_role, if_missing="skip")
        hook.delete_principal(principal, if_missing="skip")


def test_engine_properties_use_a_token(conn, catalog_name):
    props = PolarisHook(conn()).get_iceberg_rest_properties(catalog=catalog_name)
    assert props["header.Polaris-Realm"] == REALM
    assert CLIENT_SECRET not in json.dumps(props)


def test_namespace_lifecycle(conn, catalog_name):
    hook = PolarisCatalogHook(conn(default_catalog=catalog_name))
    assert hook.create_namespace("ops", properties={"owner": "a", "tier": "1"})["owner"] == "a"
    assert hook.create_namespace("ops", properties={"owner": "ignored"}, if_exists="skip")["owner"] == "a"
    hook.create_namespace("ops.child")
    assert hook.namespace_exists(["ops", "child"])
    # One request with both would hit Polaris's 409 "Concurrent modification"; the hook splits it.
    result = hook.update_namespace_properties("ops", updates={"owner": "b"}, removals=["tier", "absent"])
    assert result == {"updated": ["owner"], "removed": ["tier"], "missing": ["absent"]}
    properties = hook.get_namespace_properties("ops")
    assert properties["owner"] == "b"
    assert "tier" not in properties
    with pytest.raises(PolarisConflictError, match="not empty") as info:
        hook.drop_namespace("ops")
    assert info.value.retryable is False
    assert hook.drop_namespace("ops.child") is True
    assert hook.drop_namespace("ops") is True
    assert hook.drop_namespace("ops", if_missing="skip") is False
    assert not hook.namespace_exists("ops")


def test_table_lifecycle(conn, catalog_name):
    hook = PolarisCatalogHook(conn(default_catalog=catalog_name))
    hook.create_namespace("sales")
    schema = {
        "type": "struct",
        "fields": [
            {"id": 1, "name": "id", "type": "long", "required": True},
            {"id": 2, "name": "day", "type": "date", "required": False},
        ],
    }
    created = hook.create_table(
        "sales",
        "orders",
        schema,
        partition_spec={"fields": [{"source-id": 2, "field-id": 1000, "name": "day", "transform": "day"}]},
        sort_order={
            "fields": [
                {"source-id": 1, "transform": "identity", "direction": "asc", "null-order": "nulls-first"}
            ]
        },
        properties={"owner": "x"},
    )
    uuid = created["metadata"]["table-uuid"]
    assert created["metadata"]["default-spec-id"] == 0
    assert created["metadata"]["default-sort-order-id"] == 1
    assert hook.create_table("sales", "orders", schema, if_exists="skip")["metadata"]["table-uuid"] == uuid

    result = hook.update_table_properties("sales", "orders", updates={"tier": "gold"}, removals=["owner"])
    assert (result["updated"], result["removed"]) == (["tier"], ["owner"])
    assert hook.update_table_properties("sales", "orders", updates={"tier": "gold"})["updated"] == []

    # Within the namespace: Polaris 1.9.0+ refuses moving a table into a namespace whose location
    # does not contain the table's files.
    assert hook.rename_table("sales", "orders", "orders_v2") is True
    assert hook.rename_table("sales", "orders", "orders_v2", if_missing="skip") is False
    metadata_location = hook.load_table("sales", "orders_v2")["metadata-location"]

    assert hook.drop_table("sales", "orders_v2") is True  # the metadata files stay
    registered = hook.register_table("sales", "orders", metadata_location)
    assert registered["metadata"]["table-uuid"] == uuid
    assert hook.register_table("sales", "orders", metadata_location, if_exists="skip")

    assert hook.drop_table("sales", "orders", purge=True) is True
    assert hook.drop_table("sales", "orders", if_missing="skip") is False


def test_iceberg_v3_table_with_field_defaults(conn, catalog_name):
    # The SDK's models can neither send nor read field defaults; the hook uses the JSON directly.
    hook = PolarisCatalogHook(conn(default_catalog=catalog_name))
    hook.create_namespace("v3")
    schema = {
        "type": "struct",
        "fields": [
            {"id": 1, "name": "id", "type": "long", "required": False, "initial-default": 5},
            {"id": 2, "name": "region", "type": "string", "required": False, "write-default": "eu"},
        ],
    }
    created = hook.create_table("v3", "events", schema, properties={"format-version": "3"})
    assert created["metadata"]["format-version"] == 3
    fields = hook.load_table("v3", "events")["metadata"]["schemas"][0]["fields"]
    assert (fields[0]["initial-default"], fields[1]["write-default"]) == (5, "eu")
    assert hook.update_table_properties("v3", "events", updates={"owner": "x"})["updated"] == ["owner"]


def test_invalid_table_definition_fails_with_an_explanation(conn, catalog_name):
    hook = PolarisCatalogHook(conn(default_catalog=catalog_name))
    hook.create_namespace("bad")
    schema = {"type": "struct", "fields": [{"id": 1, "name": "id", "type": "long"}]}  # no "required"
    with pytest.raises(PolarisApiError, match="could not parse the table definition") as info:
        hook.create_table("bad", "t", schema)
    assert info.value.retryable is False


def test_update_catalog_retries_after_a_concurrent_update(conn, catalog_name):
    hook, other = PolarisManagementHook(conn()), PolarisManagementHook(conn())
    read = hook.get_catalog

    def read_then_race(name):
        current = read(name)
        if not other.get_catalog(name).properties.to_dict().get("by"):
            other.update_catalog(name, properties={"by": "someone else"})  # makes `current` stale
        return current

    hook.get_catalog = read_then_race
    properties = hook.update_catalog(catalog_name, properties={"owner": "airflow"}).properties.to_dict()
    assert (properties["owner"], properties["by"]) == ("airflow", "someone else")


def test_create_namespace_if_exists_after_a_concurrent_drop(conn, catalog_name):
    # Another client drops the namespace between Polaris's "already exists" and the hook's read.
    hook = PolarisCatalogHook(conn(default_catalog=catalog_name))
    other = PolarisCatalogHook(conn(default_catalog=catalog_name))
    other.create_namespace("race", properties={"by": "someone else"})
    read = hook.get_namespace_properties

    def drop_then_read(*args, **kwargs):
        other.drop_namespace("race", if_missing="skip")
        return read(*args, **kwargs)

    hook.get_namespace_properties = drop_then_read
    hook.create_namespace("race", properties={"owner": "airflow"}, if_exists="skip")
    properties = other.get_namespace_properties("race")
    assert (properties.get("owner"), properties.get("by")) == ("airflow", None)


@pytest.mark.parametrize("if_exists", ["skip", "update"])
def test_create_policy_if_exists_after_a_concurrent_drop(conn, catalog_name, if_exists):
    # Another client drops the policy between Polaris's "already exists" and the hook's read.
    hook = PolarisCatalogHook(conn(default_catalog=catalog_name))
    other = PolarisCatalogHook(conn(default_catalog=catalog_name))
    hook.create_namespace("race")
    other.create_policy("race", "p", "system.snapshot-expiry", content={"enable": False})
    load = hook.load_policy

    def drop_then_load(*args, **kwargs):
        other.drop_policy("race", "p", if_missing="skip")
        return load(*args, **kwargs)

    hook.load_policy = drop_then_load
    policy = hook.create_policy(
        "race", "p", "system.snapshot-expiry", content={"enable": True}, if_exists=if_exists
    )
    assert json.loads(policy.content) == {"enable": True}
    assert json.loads(other.load_policy("race", "p").content) == {"enable": True}


def test_stale_versions_are_reported_as_stale(conn, catalog_name):
    from apache_polaris.sdk.catalog.models.update_policy_request import UpdatePolicyRequest
    from apache_polaris.sdk.management import UpdateCatalogRequest

    management = PolarisManagementHook(conn())
    current = management.get_catalog(catalog_name)
    stale = UpdateCatalogRequest(
        current_entity_version=current.entity_version + 5, properties=current.properties.to_dict()
    )
    with pytest.raises(PolarisConflictError) as info:
        management.call(management.management_api.update_catalog, catalog_name, stale)
    assert info.value.stale_version, info.value.error_type

    hook = PolarisCatalogHook(conn(default_catalog=catalog_name))
    hook.create_namespace("stale")
    policy = hook.create_policy("stale", "p", "system.snapshot-expiry", content={"enable": True})
    request = UpdatePolicyRequest(content='{"enable": false}', current_policy_version=policy.version + 5)
    with pytest.raises(PolarisConflictError) as info:
        hook.call(hook.policy_api.update_policy, *hook._scope("stale", None), "p", request)
    assert info.value.stale_version, info.value.error_type
