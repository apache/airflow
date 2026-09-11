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
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from types import SimpleNamespace
from unittest import mock
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from airflow.providers.common.compat.sdk import AirflowConfigException, conf
from airflow.providers.fab.auth_manager.models import (
    Action,
    Permission,
    Resource,
    Role,
    assoc_permission_role,
)
from airflow.providers.fab.auth_manager.security_manager.override import FabAirflowSecurityManagerOverride

from tests_common.test_utils.config import conf_vars


def make_security_manager(session):
    manager = object.__new__(FabAirflowSecurityManagerOverride)
    manager.appbuilder = SimpleNamespace(session=session)
    return manager


@pytest.fixture
def role_engine(tmp_path):
    configured_engine = create_engine(conf.get("database", "sql_alchemy_conn"))
    database = f"custom_roles_{uuid4().hex}"
    if configured_engine.dialect.name == "sqlite":
        engine = create_engine(f"sqlite:///{tmp_path / 'roles.db'}")
    else:
        with configured_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
            connection.exec_driver_sql(f"CREATE DATABASE {database}")
        engine = create_engine(configured_engine.url.set(database=database))
    try:
        Role.metadata.create_all(
            engine,
            tables=[
                Action.__table__,
                Resource.__table__,
                Permission.__table__,
                Role.__table__,
                assoc_permission_role,
            ],
        )
        yield engine
    finally:
        engine.dispose()
        if configured_engine.dialect.name != "sqlite":
            with configured_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
                connection.exec_driver_sql(f"DROP DATABASE {database}")
        configured_engine.dispose()


@pytest.fixture
def manager(role_engine):
    with Session(role_engine) as session:
        yield make_security_manager(session)


def get_role_permissions(role):
    return {(permission.action.name, permission.resource.name) for permission in role.permissions}


class TestCustomRoleValidation:
    @pytest.mark.parametrize(
        "config",
        [
            [],
            None,
            False,
            1,
            "roles",
            {"": []},
            {" ": []},
            {"x" * 65: []},
            {"Analyst": {}},
            {"Admin": "oops"},
            {"Analyst": [None]},
            {"Analyst": [{"permission": "can_read", "resource": "DAGs"}]},
            {"Analyst": [{"action": "can_read"}]},
            {"Analyst": [{"action": "can_read", "resource": "DAGs", "extra": True}]},
            {"Analyst": [{"action": None, "resource": "DAGs"}]},
            {"Analyst": [{"action": " ", "resource": "DAGs"}]},
            {"Analyst": [{"action": "x" * 101, "resource": "DAGs"}]},
            {"Analyst": [{"action": "can_read", "resource": 42}]},
            {"Analyst": [{"action": "can_read", "resource": ""}]},
            {"Analyst": [{"action": "can_read", "resource": "x" * 251}]},
        ],
    )
    def test_invalid_config_does_not_start_creation(self, config):
        manager = make_security_manager(mock.Mock(spec=Session))
        with conf_vars({("fab", "custom_roles"): json.dumps(config)}):
            with pytest.raises(AirflowConfigException, match="custom_roles"):
                manager.create_roles_from_config()
        assert manager.session.mock_calls == []

    @conf_vars({("fab", "custom_roles"): "{"})
    def test_invalid_json_does_not_start_creation(self):
        manager = make_security_manager(mock.Mock(spec=Session))
        with pytest.raises(AirflowConfigException, match="custom_roles"):
            manager.create_roles_from_config()
        assert manager.session.mock_calls == []

    @conf_vars({("fab", "custom_roles"): '{"Valid": [], "Invalid": "oops"}'})
    def test_validates_all_roles_before_creation(self):
        manager = make_security_manager(mock.Mock(spec=Session))
        with pytest.raises(AirflowConfigException, match="Invalid"):
            manager.create_roles_from_config()
        assert manager.session.mock_calls == []

    @conf_vars({("fab", "custom_roles"): "{}"})
    def test_empty_config_does_not_access_database(self):
        manager = make_security_manager(mock.Mock(spec=Session))
        manager.create_roles_from_config()
        assert manager.session.mock_calls == []

    @mock.patch("airflow.providers.fab.auth_manager.security_manager.override.log", autospec=True)
    @conf_vars({("fab", "custom_roles"): '{"Admin": [], "Viewer": [], "User": [], "Op": [], "Public": []}'})
    def test_builtin_roles_are_skipped(self, mock_log):
        manager = make_security_manager(mock.Mock(spec=Session))
        manager.create_roles_from_config()
        assert manager.session.mock_calls == []
        assert mock_log.warning.call_count == 5


@pytest.mark.db_test
class TestCustomRolePersistence:
    @pytest.mark.parametrize("role_name", ["Analyst", "Admin", "Existing"])
    @pytest.mark.parametrize("unknown", ["action", "resource"])
    def test_rejects_unknown_names_before_creating_any_role(self, manager, role_name, unknown):
        manager.create_permission("can_read", "DAGs")
        manager.add_role("Existing")
        item = {"action": "can_read", "resource": "DAGs"}
        item[unknown] = "typo"
        config = {"First": [], role_name: [item]}
        with conf_vars({("fab", "custom_roles"): json.dumps(config)}):
            with pytest.raises(AirflowConfigException, match=f"Unknown {unknown} 'typo'"):
                manager.create_roles_from_config()
        assert {role.name for role in manager.get_all_roles()} == {"Existing"}
        assert manager.get_action("typo") is None
        assert manager.get_resource("typo") is None
        assert len(manager.session.scalars(select(Permission)).all()) == 1

    @mock.patch.object(FabAirflowSecurityManagerOverride, "get_resource", autospec=True)
    @mock.patch.object(FabAirflowSecurityManagerOverride, "get_permission", autospec=True)
    @conf_vars(
        {
            (
                "fab",
                "custom_roles",
            ): '{"Analyst": [{"action": "can_read", "resource": "DAGs"}, {"action": "can_read", "resource": "dags"}]}'
        }
    )
    def test_deduplicates_names_resolving_to_same_permission(
        self, mock_get_permission, mock_get_resource, manager
    ):
        permission = Permission(action=Action(name="can_read"), resource=Resource(name="DAGs"))
        manager.session.add(permission)
        manager.session.commit()
        mock_get_permission.return_value = permission
        mock_get_resource.return_value = permission.resource
        manager.create_roles_from_config()
        assert get_role_permissions(manager.find_role("Analyst")) == {("can_read", "DAGs")}
        assert len(manager.session.execute(select(assoc_permission_role)).all()) == 1

    @pytest.mark.parametrize("preexisting_permission", [False, True])
    @conf_vars(
        {
            (
                "fab",
                "custom_roles",
            ): '{"Analyst": [{"action": "can_read", "resource": "DAGs"}, {"action": "can_read", "resource": "DAGs"}], "Empty": []}'
        }
    )
    def test_creates_roles_and_reuses_permissions(self, manager, preexisting_permission):
        manager.session.add_all([Action(name="can_read"), Resource(name="DAGs")])
        manager.session.commit()
        if preexisting_permission:
            manager.create_permission("can_read", "DAGs")
        else:
            assert manager.get_permission("can_read", "DAGs") is None
        manager.create_roles_from_config()
        manager.create_roles_from_config()
        assert get_role_permissions(manager.find_role("Analyst")) == {("can_read", "DAGs")}
        assert manager.find_role("Empty").permissions == []
        assert len(manager.session.scalars(select(Permission)).all()) == 1
        assert manager.session.scalars(select(Action.name)).all() == ["can_read"]
        assert manager.session.scalars(select(Resource.name)).all() == ["DAGs"]
        assert len(manager.get_all_roles()) == 2

    @conf_vars({("fab", "custom_roles"): '{"Analyst": [{"action": "can_read", "resource": "DAGs"}]}'})
    def test_preserves_manual_permission_changes(self, manager):
        manager.create_permission("can_read", "DAGs")
        manager.create_roles_from_config()
        role = manager.find_role("Analyst")
        replacement = manager.create_permission("can_edit", "Connections")
        role.permissions = [replacement]
        manager.session.commit()
        manager.create_roles_from_config()
        assert get_role_permissions(manager.find_role("Analyst")) == {("can_edit", "Connections")}

    @conf_vars({("fab", "custom_roles"): '{"Analyst": "oops"}'})
    def test_validates_existing_role(self, manager):
        manager.add_role("Analyst")
        with pytest.raises(AirflowConfigException, match="Analyst"):
            manager.create_roles_from_config()
        assert manager.find_role("Analyst").permissions == []

    @pytest.mark.parametrize("failure_stage", ["permission", "commit"])
    @conf_vars(
        {
            (
                "fab",
                "custom_roles",
            ): '{"Completed": [], "Analyst": [{"action": "custom_action", "resource": "Custom resource"}]}'
        }
    )
    def test_rolls_back_failed_role_and_new_permissions(self, manager, role_engine, failure_stage):
        manager.session.add_all([Action(name="custom_action"), Resource(name="Custom resource")])
        manager.session.commit()

        def fail_permission(session, flush_context, instances):
            if any(isinstance(item, Permission) for item in session.new):
                raise RuntimeError("Permission storage failed")

        def fail_commit(session):
            if manager.find_role("Analyst") is not None:
                raise RuntimeError("Permission storage failed")

        event_name, listener = (
            ("before_flush", fail_permission)
            if failure_stage == "permission"
            else ("before_commit", fail_commit)
        )
        event.listen(manager.session, event_name, listener)
        try:
            with pytest.raises(RuntimeError, match="Permission storage failed"):
                manager.create_roles_from_config()
        finally:
            event.remove(manager.session, event_name, listener)
        with Session(role_engine) as observer:
            assert observer.scalars(select(Role.name)).all() == ["Completed"]
            assert observer.scalars(select(Action.name)).all() == ["custom_action"]
            assert observer.scalars(select(Resource.name)).all() == ["Custom resource"]
            assert observer.scalars(select(Permission)).all() == []
            assert observer.execute(select(assoc_permission_role)).all() == []
        manager.create_roles_from_config()
        assert get_role_permissions(manager.find_role("Analyst")) == {("custom_action", "Custom resource")}

    @conf_vars({("fab", "custom_roles"): '{"Analyst": [{"action": "can_read", "resource": "DAGs"}]}'})
    def test_rollback_preserves_preexisting_permission(self, manager, role_engine):
        manager.create_permission("can_read", "DAGs")

        def fail_commit(session):
            raise RuntimeError("Connection storage failed")

        event.listen(manager.session, "before_commit", fail_commit)
        try:
            with pytest.raises(RuntimeError, match="Connection storage failed"):
                manager.create_roles_from_config()
        finally:
            event.remove(manager.session, "before_commit", fail_commit)
        with Session(role_engine) as observer:
            assert observer.scalars(select(Role)).all() == []
            assert len(observer.scalars(select(Permission)).all()) == 1

    @pytest.mark.parametrize("iteration", range(5))
    def test_concurrent_role_creation_preserves_winner_permissions(self, role_engine, iteration):
        with Session(role_engine) as session:
            session.add_all([Action(name="can_read"), Action(name="can_edit"), Resource(name="DAGs")])
            session.commit()
        barrier = Barrier(2, timeout=10)
        original_find_role = FabAirflowSecurityManagerOverride.find_role

        def create_role(action):
            with Session(role_engine) as session:
                manager = make_security_manager(session)
                first_lookup = True

                def find_role(name):
                    nonlocal first_lookup
                    role = original_find_role(manager, name)
                    if first_lookup:
                        first_lookup = False
                        assert role is None
                        barrier.wait()
                    return role

                with mock.patch.object(manager, "find_role", autospec=True, side_effect=find_role):
                    manager._create_role_from_config("Analyst", [(action, "DAGs")])

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(create_role, action) for action in ("can_read", "can_edit")]
            for future in futures:
                future.result(timeout=20)
        with Session(role_engine) as observer:
            roles = observer.scalars(select(Role)).unique().all()
            assert len(roles) == 1
            assert get_role_permissions(roles[0]) in ({("can_read", "DAGs")}, {("can_edit", "DAGs")})

    def test_concurrent_roles_reuse_shared_permission(self, role_engine):
        if role_engine.dialect.name == "sqlite":
            pytest.skip("SQLite serializes writers before they can race on shared permissions")
        with Session(role_engine) as session:
            session.add_all([Action(name="can_read"), Resource(name="DAGs")])
            session.commit()
        barrier = Barrier(2, timeout=10)
        method = "get_permission"
        original_lookup = getattr(FabAirflowSecurityManagerOverride, method)

        def create_role(name):
            with Session(role_engine) as session:
                manager = make_security_manager(session)
                first_lookup = True

                def lookup(*args):
                    nonlocal first_lookup
                    result = original_lookup(manager, *args)
                    if first_lookup:
                        first_lookup = False
                        assert result is None
                        barrier.wait()
                    return result

                with mock.patch.object(manager, method, autospec=True, side_effect=lookup):
                    manager._create_role_from_config(name, [("can_read", "DAGs")])

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(create_role, name) for name in ("Analyst", "Tester")]
            for future in futures:
                future.result(timeout=20)
        with Session(role_engine) as observer:
            roles = observer.scalars(select(Role)).unique().all()
            assert {role.name for role in roles} == {"Analyst", "Tester"}
            assert all(get_role_permissions(role) == {("can_read", "DAGs")} for role in roles)
            assert len(observer.scalars(select(Permission)).all()) == 1


class TestCustomRoleRetries:
    def test_retries_shared_permission_conflict(self):
        session = mock.Mock(spec=Session)
        manager = make_security_manager(session)
        error = IntegrityError("insert", {}, Exception("duplicate"))
        session.flush.side_effect = [None, error, None]
        action = Action(id=1, name="can_read")
        resource = Resource(id=2, name="DAGs")
        permission = Permission(id=3, action=action, resource=resource)
        with (
            mock.patch.object(manager, "find_role", autospec=True, return_value=None),
            mock.patch.object(
                manager,
                "get_action",
                autospec=True,
                return_value=action,
            ),
            mock.patch.object(
                manager,
                "get_resource",
                autospec=True,
                return_value=resource,
            ),
            mock.patch.object(
                manager,
                "get_permission",
                autospec=True,
                side_effect=[None, permission, permission],
            ),
        ):
            manager._create_role_from_config("Analyst", [("can_read", "DAGs")])
        session.rollback.assert_called_once()
        session.commit.assert_called_once()
        roles = [call.args[0] for call in session.add.call_args_list if isinstance(call.args[0], Role)]
        assert len(roles) == 2
        assert roles[0] is not roles[1]
        assert roles[1].permissions == [permission]

    @pytest.mark.parametrize("shared_object_appeared", [False, True])
    def test_propagates_unexplained_or_exhausted_conflicts(self, shared_object_appeared):
        session = mock.Mock(spec=Session)
        manager = make_security_manager(session)
        error = IntegrityError("insert", {}, Exception("failure"))
        session.flush.side_effect = [None, error] * 3
        with (
            mock.patch.object(manager, "find_role", autospec=True, return_value=None),
            mock.patch.object(
                manager,
                "get_permission",
                autospec=True,
                side_effect=[None, Permission() if shared_object_appeared else None] * 3,
            ),
            mock.patch.object(
                manager,
                "get_action",
                autospec=True,
                return_value=Action(name="can_read"),
            ),
            mock.patch.object(manager, "get_resource", autospec=True, return_value=Resource(name="DAGs")),
        ):
            with pytest.raises(IntegrityError) as raised:
                manager._create_role_from_config("Analyst", [("can_read", "DAGs")])
        assert raised.value is error
        assert session.rollback.call_count == (3 if shared_object_appeared else 1)
        session.commit.assert_not_called()

    def test_does_not_retry_unrelated_role_insert_error(self):
        session = mock.Mock(spec=Session)
        session.flush.side_effect = IntegrityError("insert", {}, Exception("failure"))
        manager = make_security_manager(session)
        with mock.patch.object(manager, "find_role", autospec=True, return_value=None):
            with pytest.raises(IntegrityError):
                manager._create_role_from_config("Analyst", [])
        session.rollback.assert_called_once()
        session.commit.assert_not_called()
