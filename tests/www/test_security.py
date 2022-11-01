#
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

import contextlib
import datetime
import logging
from unittest import mock

import pytest
from flask_appbuilder import SQLA, Model, expose, has_access
from flask_appbuilder.views import BaseView, ModelView
from freezegun import freeze_time
from sqlalchemy import Column, Date, Float, Integer, String

from airflow.exceptions import AirflowException
from airflow.models import DagModel
from airflow.models.base import Base
from airflow.models.dag import DAG
from airflow.security import permissions
from airflow.www import app as application
from airflow.www.fab_security.manager import AnonymousUser
from airflow.www.fab_security.sqla.models import User, assoc_permission_role
from airflow.www.utils import CustomSQLAInterface
from tests.test_utils.api_connexion_utils import (
    create_user,
    create_user_scope,
    delete_role,
    delete_user,
    set_user_single_role,
)
from tests.test_utils.asserts import assert_queries_count
from tests.test_utils.db import clear_db_dags, clear_db_runs
from tests.test_utils.mock_security_manager import MockSecurityManager

READ_WRITE = {permissions.ACTION_CAN_READ, permissions.ACTION_CAN_EDIT}
READ_ONLY = {permissions.ACTION_CAN_READ}

logging.basicConfig(format="%(asctime)s:%(levelname)s:%(name)s:%(message)s")
logging.getLogger().setLevel(logging.DEBUG)
log = logging.getLogger(__name__)


class SomeModel(Model):
    id = Column(Integer, primary_key=True)
    field_string = Column(String(50), unique=True, nullable=False)
    field_integer = Column(Integer())
    field_float = Column(Float())
    field_date = Column(Date())

    def __repr__(self):
        return str(self.field_string)


class SomeModelView(ModelView):
    datamodel = CustomSQLAInterface(SomeModel)
    base_permissions = [
        "can_list",
        "can_show",
        "can_add",
        permissions.ACTION_CAN_EDIT,
        permissions.ACTION_CAN_DELETE,
    ]
    list_columns = ["field_string", "field_integer", "field_float", "field_date"]


class SomeBaseView(BaseView):
    route_base = ""

    @expose("/some_action")
    @has_access
    def some_action(self):
        return "action!"


def _clear_db_dag_and_runs():
    clear_db_runs()
    clear_db_dags()


def _delete_dag_permissions(dag_id, security_manager):
    dag_resource_name = permissions.resource_name_for_dag(dag_id)
    for dag_action_name in security_manager.DAG_ACTIONS:
        security_manager.delete_permission(dag_action_name, dag_resource_name)


def _create_dag_model(dag_id, session, security_manager):
    dag_model = DagModel(dag_id=dag_id)
    session.add(dag_model)
    session.commit()
    security_manager.sync_perm_for_dag(dag_id, access_control=None)
    return dag_model


def _delete_dag_model(dag_model, session, security_manager):
    session.delete(dag_model)
    session.commit()
    _delete_dag_permissions(dag_model.dag_id, security_manager)


@contextlib.contextmanager
def _create_dag_model_context(dag_id, session, security_manager):
    dag = _create_dag_model(dag_id, session, security_manager)
    yield dag
    _delete_dag_model(dag, session, security_manager)


@pytest.fixture(scope="module", autouse=True)
def clear_db_after_suite():
    yield None
    _clear_db_dag_and_runs()


@pytest.fixture(scope="function", autouse=True)
def clear_db_before_test():
    _clear_db_dag_and_runs()


@pytest.fixture(scope="module")
def app():
    _app = application.create_app(testing=True)
    _app.config["WTF_CSRF_ENABLED"] = False
    return _app


@pytest.fixture(scope="module")
def app_builder(app):
    app_builder = app.appbuilder
    app_builder.add_view(SomeBaseView, "SomeBaseView", category="BaseViews")
    app_builder.add_view(SomeModelView, "SomeModelView", category="ModelViews")
    return app.appbuilder


@pytest.fixture(scope="module")
def security_manager(app_builder):
    return app_builder.sm


@pytest.fixture(scope="module")
def session(app_builder):
    return app_builder.get_session


@pytest.fixture(scope="module")
def db(app):
    return SQLA(app)


@pytest.fixture(scope="function")
def role(request, app, security_manager):
    params = request.param
    _role = None
    params["mock_roles"] = [{"role": params["name"], "perms": params["permissions"]}]
    if params.get("create", True):
        security_manager.bulk_sync_roles(params["mock_roles"])
        _role = security_manager.find_role(params["name"])
    yield _role, params
    delete_role(app, params["name"])


@pytest.fixture(scope="function")
def mock_dag_models(request, session, security_manager):
    dags_ids = request.param
    dags = [_create_dag_model(dag_id, session, security_manager) for dag_id in dags_ids]

    yield dags_ids

    for dag in dags:
        _delete_dag_model(dag, session, security_manager)


@pytest.fixture(scope="function")
def sample_dags(security_manager):
    dags = [
        DAG("has_access_control", access_control={"Public": {permissions.ACTION_CAN_READ}}),
        DAG("no_access_control"),
    ]

    yield dags

    for dag in dags:
        _delete_dag_permissions(dag.dag_id, security_manager)


@pytest.fixture(scope="module")
def has_dag_perm(security_manager):
    def _has_dag_perm(perm, dag_id, user):
        root_dag_id = security_manager._get_root_dag_id(dag_id)
        return security_manager.has_access(perm, permissions.resource_name_for_dag(root_dag_id), user)

    return _has_dag_perm


@pytest.fixture(scope="module")
def assert_user_has_dag_perms(has_dag_perm):
    def _assert_user_has_dag_perms(perms, dag_id, user=None):
        for perm in perms:
            assert has_dag_perm(perm, dag_id, user), f"User should have '{perm}' on DAG '{dag_id}'"

    return _assert_user_has_dag_perms


@pytest.fixture(scope="module")
def assert_user_does_not_have_dag_perms(has_dag_perm):
    def _assert_user_does_not_have_dag_perms(dag_id, perms, user=None):
        for perm in perms:
            assert not has_dag_perm(perm, dag_id, user), f"User should not have '{perm}' on DAG '{dag_id}'"

    return _assert_user_does_not_have_dag_perms


@pytest.mark.parametrize(
    "role",
    [{"name": "MyRole7", "permissions": [("can_some_other_action", "AnotherBaseView")], "create": False}],
    indirect=True,
)
def test_init_role_baseview(app, security_manager, role):
    _, params = role

    with pytest.warns(
        DeprecationWarning,
        match="`init_role` has been deprecated\\. Please use `bulk_sync_roles` instead\\.",
    ):
        security_manager.init_role(params["name"], params["permissions"])

    _role = security_manager.find_role(params["name"])
    assert _role is not None
    assert len(_role.permissions) == len(params["permissions"])


@pytest.mark.parametrize(
    "role",
    [{"name": "MyRole3", "permissions": [("can_some_action", "SomeBaseView")]}],
    indirect=True,
)
def test_bulk_sync_roles_baseview(app, security_manager, role):
    _role, params = role
    assert _role is not None
    assert len(_role.permissions) == len(params["permissions"])


@pytest.mark.parametrize(
    "role",
    [
        {
            "name": "MyRole2",
            "permissions": [
                ("can_list", "SomeModelView"),
                ("can_show", "SomeModelView"),
                ("can_add", "SomeModelView"),
                (permissions.ACTION_CAN_EDIT, "SomeModelView"),
                (permissions.ACTION_CAN_DELETE, "SomeModelView"),
            ],
        }
    ],
    indirect=True,
)
def test_bulk_sync_roles_modelview(app, security_manager, role):
    _role, params = role
    assert role is not None
    assert len(_role.permissions) == len(params["permissions"])

    # Check short circuit works
    with assert_queries_count(2):  # One for Permission, one for roles
        security_manager.bulk_sync_roles(params["mock_roles"])


@pytest.mark.parametrize(
    "role",
    [{"name": "Test_Role", "permissions": []}],
    indirect=True,
)
def test_update_and_verify_permission_role(app, security_manager, role):
    _role, params = role
    perm = security_manager.get_permission(permissions.ACTION_CAN_EDIT, permissions.RESOURCE_ROLE)
    security_manager.add_permission_to_role(_role, perm)
    role_perms_len = len(_role.permissions)

    security_manager.bulk_sync_roles(params["mock_roles"])
    new_role_perms_len = len(_role.permissions)

    assert role_perms_len == new_role_perms_len
    assert new_role_perms_len == 1


def test_verify_public_role_has_no_permissions(security_manager):
    public = security_manager.find_role("Public")
    assert public.permissions == []


def test_verify_default_anon_user_has_no_accessible_dag_ids(app, session, security_manager):
    with app.app_context():
        user = AnonymousUser()
        app.config["AUTH_ROLE_PUBLIC"] = "Public"
        assert security_manager.get_user_roles(user) == {security_manager.get_public_role()}

        with _create_dag_model_context("test_dag_id", session, security_manager):
            security_manager.sync_roles()

            assert security_manager.get_accessible_dag_ids(user) == set()


def test_verify_default_anon_user_has_no_access_to_specific_dag(app, session, security_manager, has_dag_perm):
    with app.app_context():
        user = AnonymousUser()
        app.config["AUTH_ROLE_PUBLIC"] = "Public"
        assert security_manager.get_user_roles(user) == {security_manager.get_public_role()}

        dag_id = "test_dag_id"
        with _create_dag_model_context(dag_id, session, security_manager):
            security_manager.sync_roles()

            assert security_manager.can_read_dag(dag_id, user) is False
            assert security_manager.can_edit_dag(dag_id, user) is False
            assert has_dag_perm(permissions.ACTION_CAN_READ, dag_id, user) is False
            assert has_dag_perm(permissions.ACTION_CAN_EDIT, dag_id, user) is False


@pytest.mark.parametrize(
    "mock_dag_models",
    [["test_dag_id_1", "test_dag_id_2", "test_dag_id_3"]],
    indirect=True,
)
def test_verify_anon_user_with_admin_role_has_all_dag_access(app, security_manager, mock_dag_models):
    test_dag_ids = mock_dag_models
    with app.app_context():
        app.config["AUTH_ROLE_PUBLIC"] = "Admin"
        user = AnonymousUser()

        assert security_manager.get_user_roles(user) == {security_manager.get_public_role()}

        security_manager.sync_roles()

        assert security_manager.get_accessible_dag_ids(user) == set(test_dag_ids)


def test_verify_anon_user_with_admin_role_has_access_to_each_dag(
    app, session, security_manager, has_dag_perm
):
    with app.app_context():
        user = AnonymousUser()
        app.config["AUTH_ROLE_PUBLIC"] = "Admin"

        # Call `.get_user_roles` bc `user` is a mock and the `user.roles` prop needs to be set.
        user.roles = security_manager.get_user_roles(user)
        assert user.roles == {security_manager.get_public_role()}

        test_dag_ids = ["test_dag_id_1", "test_dag_id_2", "test_dag_id_3", "test_dag_id_4.with_dot"]

        for dag_id in test_dag_ids:
            with _create_dag_model_context(dag_id, session, security_manager):
                security_manager.sync_roles()

                assert security_manager.can_read_dag(dag_id, user) is True
                assert security_manager.can_edit_dag(dag_id, user) is True
                assert has_dag_perm(permissions.ACTION_CAN_READ, dag_id, user) is True
                assert has_dag_perm(permissions.ACTION_CAN_EDIT, dag_id, user) is True


def test_get_user_roles(app_builder, security_manager):
    user = mock.MagicMock()
    user.is_anonymous = False
    roles = app_builder.sm.find_role("Admin")
    user.roles = roles
    assert security_manager.get_user_roles(user) == roles


def test_get_user_roles_for_anonymous_user(app, security_manager):
    viewer_role_perms = {
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_AUDIT_LOG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_DEPENDENCIES),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_CODE),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DATASET),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_IMPORT_ERROR),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_WARNING),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_JOB),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_PLUGIN),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_SLA_MISS),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_LOG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_XCOM),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_MY_PASSWORD),
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_MY_PASSWORD),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_MY_PROFILE),
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_MY_PROFILE),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_BROWSE_MENU),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_DAG_DEPENDENCIES),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_DATASET),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_JOB),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_AUDIT_LOG),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_PLUGIN),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_SLA_MISS),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_TASK_INSTANCE),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_DOCS_MENU),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_DOCS),
    }
    app.config["AUTH_ROLE_PUBLIC"] = "Viewer"

    with app.app_context():
        user = AnonymousUser()

        perms_views = set()
        for role in security_manager.get_user_roles(user):
            perms_views.update({(perm.action.name, perm.resource.name) for perm in role.permissions})
        assert perms_views == viewer_role_perms


def test_get_current_user_permissions(app):
    action = "can_some_action"
    resource = "SomeBaseView"

    with app.app_context():
        with create_user_scope(
            app,
            username="get_current_user_permissions",
            role_name="MyRole5",
            permissions=[
                (action, resource),
            ],
        ) as user:
            assert user.perms == {(action, resource)}

        with create_user_scope(
            app,
            username="no_perms",
        ) as user:
            assert len(user.perms) == 0


def test_get_accessible_dag_ids(app, security_manager, session):
    role_name = "MyRole1"
    permission_action = [permissions.ACTION_CAN_READ]
    dag_id = "dag_id"
    username = "ElUser"

    with create_user_scope(
        app,
        username=username,
        role_name=role_name,
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        ],
    ) as user:

        dag_model = DagModel(dag_id=dag_id, fileloc="/tmp/dag_.py", schedule_interval="2 2 * * *")
        session.add(dag_model)
        session.commit()

        security_manager.sync_perm_for_dag(  # type: ignore
            dag_id, access_control={role_name: permission_action}
        )

        assert security_manager.get_accessible_dag_ids(user) == {"dag_id"}


def test_dont_get_inaccessible_dag_ids_for_dag_resource_permission(app, security_manager, session):
    # In this test case,
    # get_readable_dag_ids() don't return DAGs to which the user has CAN_EDIT action
    username = "Monsieur User"
    role_name = "MyRole1"
    permission_action = [permissions.ACTION_CAN_EDIT]
    dag_id = "dag_id"

    with create_user_scope(
        app,
        username=username,
        role_name=role_name,
        permissions=[
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
        ],
    ) as user:

        dag_model = DagModel(dag_id=dag_id, fileloc="/tmp/dag_.py", schedule_interval="2 2 * * *")
        session.add(dag_model)
        session.commit()

        security_manager.sync_perm_for_dag(  # type: ignore
            dag_id, access_control={role_name: permission_action}
        )

        assert security_manager.get_readable_dag_ids(user) == set()


def test_has_access(security_manager):
    user = mock.MagicMock()
    action_name = "action"
    resource_name = "resource"
    user.perms = [(action_name, resource_name)]
    assert security_manager.has_access(action_name, resource_name, user)


def test_sync_perm_for_dag_creates_permissions_on_resources(security_manager):
    test_dag_id = "TEST_DAG"
    prefixed_test_dag_id = f"DAG:{test_dag_id}"
    security_manager.sync_perm_for_dag(test_dag_id, access_control=None)
    assert security_manager.get_permission(permissions.ACTION_CAN_READ, prefixed_test_dag_id) is not None
    assert security_manager.get_permission(permissions.ACTION_CAN_EDIT, prefixed_test_dag_id) is not None


def test_has_all_dag_access(app, security_manager):
    for role_name in ["Admin", "Viewer", "Op", "User"]:
        with app.app_context():
            with create_user_scope(
                app,
                username="user",
                role_name=role_name,
            ) as user:
                assert security_manager.has_all_dags_access(user)

    with app.app_context():
        with create_user_scope(
            app,
            username="user",
            role_name="read_all",
            permissions=[(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG)],
        ) as user:
            assert security_manager.has_all_dags_access(user)

    with app.app_context():
        with create_user_scope(
            app,
            username="user",
            role_name="edit_all",
            permissions=[(permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG)],
        ) as user:
            assert security_manager.has_all_dags_access(user)

    with app.app_context():
        with create_user_scope(
            app,
            username="user",
            role_name="nada",
            permissions=[],
        ) as user:
            assert not security_manager.has_all_dags_access(user)


def test_access_control_with_non_existent_role(security_manager):
    with pytest.raises(AirflowException) as ctx:
        security_manager._sync_dag_view_permissions(
            dag_id="access-control-test",
            access_control={
                "this-role-does-not-exist": [permissions.ACTION_CAN_EDIT, permissions.ACTION_CAN_READ]
            },
        )
    assert "role does not exist" in str(ctx.value)


def test_all_dag_access_doesnt_give_non_dag_access(app, security_manager):
    username = "dag_access_user"
    role_name = "dag_access_role"
    with app.app_context():
        with create_user_scope(
            app,
            username=username,
            role_name=role_name,
            permissions=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            ],
        ) as user:
            assert security_manager.has_access(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG, user)
            assert not security_manager.has_access(
                permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE, user
            )


def test_access_control_with_invalid_permission(app, security_manager):
    invalid_actions = [
        "can_varimport",  # a real action, but not a member of DAG_ACTIONS
        "can_eat_pudding",  # clearly not a real action
    ]
    username = "LaUser"
    rolename = "team-a"
    with create_user_scope(
        app,
        username=username,
        role_name=rolename,
    ):
        for action in invalid_actions:
            with pytest.raises(AirflowException) as ctx:
                security_manager._sync_dag_view_permissions(
                    "access_control_test",
                    access_control={rolename: {action}},
                )
            assert "invalid permissions" in str(ctx.value)


def test_access_control_is_set_on_init(
    app,
    security_manager,
    assert_user_has_dag_perms,
    assert_user_does_not_have_dag_perms,
):
    username = "access_control_is_set_on_init"
    role_name = "team-a"
    negated_role = "NOT-team-a"
    with app.app_context():
        with create_user_scope(
            app,
            username=username,
            role_name=role_name,
            permissions=[],
        ) as user:
            security_manager._sync_dag_view_permissions(
                "access_control_test",
                access_control={role_name: [permissions.ACTION_CAN_EDIT, permissions.ACTION_CAN_READ]},
            )
            assert_user_has_dag_perms(
                perms=[permissions.ACTION_CAN_EDIT, permissions.ACTION_CAN_READ],
                dag_id="access_control_test",
                user=user,
            )

            security_manager.bulk_sync_roles([{"role": negated_role, "perms": []}])
            set_user_single_role(app, user, role_name=negated_role)
            assert_user_does_not_have_dag_perms(
                perms=[permissions.ACTION_CAN_EDIT, permissions.ACTION_CAN_READ],
                dag_id="access_control_test",
                user=user,
            )


def test_access_control_stale_perms_are_revoked(
    app,
    security_manager,
    assert_user_has_dag_perms,
    assert_user_does_not_have_dag_perms,
):
    username = "access_control_stale_perms_are_revoked"
    role_name = "team-a"
    with app.app_context():
        with create_user_scope(
            app,
            username=username,
            role_name=role_name,
            permissions=[],
        ) as user:
            set_user_single_role(app, user, role_name="team-a")
            security_manager._sync_dag_view_permissions(
                "access_control_test", access_control={"team-a": READ_WRITE}
            )
            assert_user_has_dag_perms(perms=READ_WRITE, dag_id="access_control_test", user=user)

            security_manager._sync_dag_view_permissions(
                "access_control_test", access_control={"team-a": READ_ONLY}
            )
            # Clear the cache, to make it pick up new rol perms
            user._perms = None
            assert_user_has_dag_perms(
                perms=[permissions.ACTION_CAN_READ], dag_id="access_control_test", user=user
            )
            assert_user_does_not_have_dag_perms(
                perms=[permissions.ACTION_CAN_EDIT], dag_id="access_control_test", user=user
            )


def test_no_additional_dag_permission_views_created(db, security_manager):
    ab_perm_role = assoc_permission_role

    security_manager.sync_roles()
    num_pv_before = db.session().query(ab_perm_role).count()
    security_manager.sync_roles()
    num_pv_after = db.session().query(ab_perm_role).count()
    assert num_pv_before == num_pv_after


def test_override_role_vm(app_builder):
    test_security_manager = MockSecurityManager(appbuilder=app_builder)
    assert len(test_security_manager.VIEWER_VMS) == 1
    assert test_security_manager.VIEWER_VMS == {"Airflow"}


def test_correct_roles_have_perms_to_read_config(security_manager):
    roles_to_check = security_manager.get_all_roles()
    assert len(roles_to_check) >= 5
    for role in roles_to_check:
        if role.name in ["Admin", "Op"]:
            assert security_manager.permission_exists_in_one_or_more_roles(
                permissions.RESOURCE_CONFIG, permissions.ACTION_CAN_READ, [role.id]
            )
        else:
            assert not security_manager.permission_exists_in_one_or_more_roles(
                permissions.RESOURCE_CONFIG, permissions.ACTION_CAN_READ, [role.id]
            ), (
                f"{role.name} should not have {permissions.ACTION_CAN_READ} "
                f"on {permissions.RESOURCE_CONFIG}"
            )


def test_create_dag_specific_permissions(session, security_manager, monkeypatch, sample_dags):

    access_control = {"Public": {permissions.ACTION_CAN_READ}}

    collect_dags_from_db_mock = mock.Mock()
    dagbag_mock = mock.Mock()
    dagbag_mock.dags = {dag.dag_id: dag for dag in sample_dags}
    dagbag_mock.collect_dags_from_db = collect_dags_from_db_mock
    dagbag_class_mock = mock.Mock()
    dagbag_class_mock.return_value = dagbag_mock
    import airflow.www.security

    monkeypatch.setitem(airflow.www.security.__dict__, "DagBag", dagbag_class_mock)
    security_manager._sync_dag_view_permissions = mock.Mock()

    for dag in sample_dags:
        dag_resource_name = permissions.resource_name_for_dag(dag.dag_id)
        all_perms = security_manager.get_all_permissions()
        assert ("can_read", dag_resource_name) not in all_perms
        assert ("can_edit", dag_resource_name) not in all_perms

    security_manager.create_dag_specific_permissions()

    dagbag_class_mock.assert_called_once_with(read_dags_from_db=True)
    collect_dags_from_db_mock.assert_called_once_with()

    for dag in sample_dags:
        dag_resource_name = permissions.resource_name_for_dag(dag.dag_id)
        all_perms = security_manager.get_all_permissions()
        assert ("can_read", dag_resource_name) in all_perms
        assert ("can_edit", dag_resource_name) in all_perms

    security_manager._sync_dag_view_permissions.assert_called_once_with(
        permissions.resource_name_for_dag("has_access_control"),
        access_control,
    )

    del dagbag_mock.dags["has_access_control"]
    with assert_queries_count(2):  # two query to get all perms; dagbag is mocked
        # The extra query happens at permission check
        security_manager.create_dag_specific_permissions()


def test_get_all_permissions(security_manager):
    with assert_queries_count(1):
        perms = security_manager.get_all_permissions()

    assert isinstance(perms, set)
    for perm in perms:
        assert len(perm) == 2
    assert ("can_read", "Connections") in perms


def test_get_all_non_dag_permissions(security_manager):
    with assert_queries_count(1):
        pvs = security_manager._get_all_non_dag_permissions()

    assert isinstance(pvs, dict)
    for (perm_name, viewmodel_name), perm in pvs.items():
        assert isinstance(perm_name, str)
        assert isinstance(viewmodel_name, str)
        assert isinstance(perm, security_manager.permission_model)

    assert ("can_read", "Connections") in pvs


def test_get_all_roles_with_permissions(security_manager):
    with assert_queries_count(1):
        roles = security_manager._get_all_roles_with_permissions()

    assert isinstance(roles, dict)
    for role_name, role in roles.items():
        assert isinstance(role_name, str)
        assert isinstance(role, security_manager.role_model)

    assert "Admin" in roles


def test_prefixed_dag_id_is_deprecated(security_manager):
    with pytest.warns(
        DeprecationWarning,
        match=(
            "`prefixed_dag_id` has been deprecated. "
            "Please use `airflow.security.permissions.resource_name_for_dag` instead."
        ),
    ):
        security_manager.prefixed_dag_id("hello")


def test_parent_dag_access_applies_to_subdag(app, security_manager, assert_user_has_dag_perms, session):
    username = "dag_permission_user"
    role_name = "dag_permission_role"
    parent_dag_name = "parent_dag"
    subdag_name = parent_dag_name + ".subdag"
    subsubdag_name = parent_dag_name + ".subdag.subsubdag"
    with app.app_context():
        mock_roles = [
            {
                "role": role_name,
                "perms": [
                    (permissions.ACTION_CAN_READ, f"DAG:{parent_dag_name}"),
                    (permissions.ACTION_CAN_EDIT, f"DAG:{parent_dag_name}"),
                ],
            }
        ]
        with create_user_scope(
            app,
            username=username,
            role_name=role_name,
        ) as user:
            dag1 = DagModel(dag_id=parent_dag_name)
            dag2 = DagModel(dag_id=subdag_name, is_subdag=True, root_dag_id=parent_dag_name)
            dag3 = DagModel(dag_id=subsubdag_name, is_subdag=True, root_dag_id=parent_dag_name)
            session.add_all([dag1, dag2, dag3])
            session.commit()
            security_manager.bulk_sync_roles(mock_roles)
            for dag in [dag1, dag2, dag3]:
                security_manager._sync_dag_view_permissions(
                    parent_dag_name, access_control={role_name: READ_WRITE}
                )

            assert_user_has_dag_perms(perms=READ_WRITE, dag_id=parent_dag_name, user=user)
            assert_user_has_dag_perms(perms=READ_WRITE, dag_id=parent_dag_name + ".subdag", user=user)
            assert_user_has_dag_perms(
                perms=READ_WRITE, dag_id=parent_dag_name + ".subdag.subsubdag", user=user
            )
            session.query(DagModel).delete()


def test_permissions_work_for_dags_with_dot_in_dagname(
    app, security_manager, assert_user_has_dag_perms, assert_user_does_not_have_dag_perms, session
):
    username = "dag_permission_user"
    role_name = "dag_permission_role"
    dag_id = "dag_id_1"
    dag_id_2 = "dag_id_1.with_dot"
    with app.app_context():
        mock_roles = [
            {
                "role": role_name,
                "perms": [
                    (permissions.ACTION_CAN_READ, f"DAG:{dag_id}"),
                    (permissions.ACTION_CAN_EDIT, f"DAG:{dag_id}"),
                ],
            }
        ]
        with create_user_scope(
            app,
            username=username,
            role_name=role_name,
        ) as user:
            dag1 = DagModel(dag_id=dag_id)
            dag2 = DagModel(dag_id=dag_id_2)
            session.add_all([dag1, dag2])
            session.commit()
            security_manager.bulk_sync_roles(mock_roles)
            security_manager.sync_perm_for_dag(dag1.dag_id, access_control={role_name: READ_WRITE})
            security_manager.sync_perm_for_dag(dag2.dag_id, access_control={role_name: READ_WRITE})
            assert_user_has_dag_perms(perms=READ_WRITE, dag_id=dag_id, user=user)
            assert_user_does_not_have_dag_perms(perms=READ_WRITE, dag_id=dag_id_2, user=user)
            session.query(DagModel).delete()


def test_fab_models_use_airflow_base_meta():
    # TODO: move this test to appropriate place when we have more tests for FAB models
    user = User()
    assert user.metadata is Base.metadata


@pytest.fixture()
def mock_security_manager(app_builder):
    mocked_security_manager = MockSecurityManager(appbuilder=app_builder)
    mocked_security_manager.update_user = mock.MagicMock()
    return mocked_security_manager


@pytest.fixture()
def new_user():
    user = mock.MagicMock()
    user.login_count = None
    user.fail_login_count = None
    user.last_login = None
    return user


@pytest.fixture()
def old_user():
    user = mock.MagicMock()
    user.login_count = 42
    user.fail_login_count = 9
    user.last_login = datetime.datetime(1984, 12, 1, 0, 0, 0)
    return user


@freeze_time(datetime.datetime(1985, 11, 5, 1, 24, 0))  # Get the Delorean, doc!
def test_update_user_auth_stat_first_successful_auth(mock_security_manager, new_user):
    mock_security_manager.update_user_auth_stat(new_user, success=True)

    assert new_user.login_count == 1
    assert new_user.fail_login_count == 0
    assert new_user.last_login == datetime.datetime(1985, 11, 5, 1, 24, 0)
    assert mock_security_manager.update_user.called_once


@freeze_time(datetime.datetime(1985, 11, 5, 1, 24, 0))
def test_update_user_auth_stat_subsequent_successful_auth(mock_security_manager, old_user):
    mock_security_manager.update_user_auth_stat(old_user, success=True)

    assert old_user.login_count == 43
    assert old_user.fail_login_count == 0
    assert old_user.last_login == datetime.datetime(1985, 11, 5, 1, 24, 0)
    assert mock_security_manager.update_user.called_once


@freeze_time(datetime.datetime(1985, 11, 5, 1, 24, 0))
def test_update_user_auth_stat_first_unsuccessful_auth(mock_security_manager, new_user):
    mock_security_manager.update_user_auth_stat(new_user, success=False)

    assert new_user.login_count == 0
    assert new_user.fail_login_count == 1
    assert new_user.last_login is None
    assert mock_security_manager.update_user.called_once


@freeze_time(datetime.datetime(1985, 11, 5, 1, 24, 0))
def test_update_user_auth_stat_subsequent_unsuccessful_auth(mock_security_manager, old_user):
    mock_security_manager.update_user_auth_stat(old_user, success=False)

    assert old_user.login_count == 42
    assert old_user.fail_login_count == 10
    assert old_user.last_login == datetime.datetime(1984, 12, 1, 0, 0, 0)
    assert mock_security_manager.update_user.called_once


def test_users_can_be_found(app, security_manager, session, caplog):
    """Test that usernames are case insensitive"""
    create_user(app, "Test")
    create_user(app, "test")
    create_user(app, "TEST")
    create_user(app, "TeSt")
    assert security_manager.find_user("Test")
    users = security_manager.get_all_users()
    assert len(users) == 1
    delete_user(app, "Test")
    assert "Error adding new user to database" in caplog.text
