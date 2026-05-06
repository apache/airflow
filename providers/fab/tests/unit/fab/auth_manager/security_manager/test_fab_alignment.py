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
"""Tests to ensure Airflow's vendored FAB security manager code stays aligned with upstream FAB.

Since FabAirflowSecurityManagerOverride vendors in parts of FAB's security manager,
these tests detect when the installed FAB version drifts from what override.py was
aligned with — catching mismatches that CI would otherwise miss silently.
"""

from __future__ import annotations

import ast
import datetime
import importlib.metadata
import importlib.util
import inspect
from pathlib import Path
from unittest import mock
from unittest.mock import Mock

from flask_appbuilder.security.manager import BaseSecurityManager
from sqlalchemy.orm import Session

from airflow.providers.fab.auth_manager.models import Role
from airflow.providers.fab.auth_manager.security_manager.override import FabAirflowSecurityManagerOverride

# The FAB version that override.py was last aligned with.
EXPECTED_FAB_VERSION = "5.2.0"

# FAB public methods that override.py intentionally does NOT implement.
# Every entry must have a comment explaining why it's excluded.
# When FAB adds new public methods, this test will fail until the method
# is either implemented in override.py or added here with a justification.
AUDITED_EXCLUSIONS: dict[str, str] = {
    # --- FAB 5.2.0: API Key authentication (not used by Airflow) ---
    "api_key_model": "API key feature not used by Airflow",
    "create_api_key": "API key feature not used by Airflow",
    "extract_api_key_from_request": "API key feature not used by Airflow",
    "find_api_keys_for_user": "API key feature not used by Airflow",
    "get_api_key_by_uuid": "API key feature not used by Airflow",
    "revoke_api_key": "API key feature not used by Airflow",
    "validate_api_key": "API key feature not used by Airflow",
    # --- FAB 5.1.0: SAML authentication (not used by Airflow) ---
    "auth_user_saml": "SAML auth not used by Airflow",
    "authsamlview": "SAML auth not used by Airflow",
    "get_saml_login_redirect_url": "SAML auth not used by Airflow",
    "get_saml_logout_redirect_url": "SAML auth not used by Airflow",
    "get_saml_provider": "SAML auth not used by Airflow",
    "get_saml_settings": "SAML auth not used by Airflow",
    "get_saml_userinfo": "SAML auth not used by Airflow",
    "saml_config": "SAML auth not used by Airflow",
    "saml_providers": "SAML auth not used by Airflow",
    "usersamlmodelview": "SAML auth not used by Airflow",
    # --- FAB internals: view/menu management (handled by Airflow's own view system) ---
    "add_limit_view": "Airflow manages its own view limits",
    "add_permission": "Airflow uses create_permission instead",
    "add_permission_view_menu": "Airflow uses its own permission system",
    "add_view_menu": "Airflow manages views differently",
    "del_permission": "Airflow uses delete_permission instead",
    "del_permission_view_menu": "Airflow manages its own permission system",
    "del_view_menu": "Airflow manages views differently",
    "exist_permission_on_roles": "Not used by Airflow",
    "exist_permission_on_view": "Not used by Airflow",
    "exist_permission_on_views": "Not used by Airflow",
    "find_permission": "Airflow uses get_permission instead",
    "find_permission_view_menu": "Airflow uses its own permission lookup",
    "find_permissions_view_menu": "Airflow uses its own permission lookup",
    "find_register_user": "Not used by Airflow",
    "find_roles_permission_view_menus": "Not used by Airflow",
    "find_view_menu": "Airflow uses get_resource instead",
    "get_all_view_menu": "Not used by Airflow",
    "get_db_role_permissions": "Not used by Airflow",
    "get_register_user_datamodel": "Not used by Airflow",
    "get_role_permissions": "Airflow uses get_resource_permissions instead",
    "get_url_for_registeruser": "Not used by Airflow",
    "get_user_datamodel": "Not used by Airflow",
    "get_user_menu_access": "Not used by Airflow",
    "get_user_permissions": "Not used by Airflow",
    "get_user_roles_permissions": "Not used by Airflow",
    # --- FAB internals: view class attributes (Airflow defines its own set) ---
    "groupmodelview": "Airflow does not expose group model views",
    "permissionmodelview": "Airflow uses its own ActionModelView",
    "permissionview_model": "Not used directly by Airflow",
    "permissionviewmodelview": "Airflow uses its own PermissionPairModelView",
    "registerusermodelview": "Not used by Airflow",
    "rolemodelview": "Airflow uses CustomRoleModelView",
    "viewmenu_model": "Airflow uses resource_model instead",
    "viewmenumodelview": "Airflow uses ResourceModelView instead",
    # --- FAB internals: API views (Airflow has its own API layer) ---
    "group_api": "Airflow has its own API",
    "permission_api": "Airflow has its own API",
    "permission_view_menu_api": "Airflow has its own API",
    "role_api": "Airflow has its own API",
    "security_api": "Airflow registers SecurityApi separately",
    "user_api": "Airflow has its own API",
    "view_menu_api": "Airflow has its own API",
    # --- FAB internals: lifecycle hooks / framework plumbing ---
    "add_permission_role": "Airflow uses add_permission_to_role instead",
    "auth_ldap_bind_first": "Not used by Airflow's LDAP implementation",
    "auth_username_ci": "Airflow handles case sensitivity differently",
    "before_request": "Airflow's AirflowSecurityManagerV2 defines its own",
    "create_state_transitions": "FAB internal state machine, not used by Airflow",
    "current_user": "Airflow manages current_user through its auth manager",
    "del_permission_role": "Airflow uses remove_permission_from_role instead",
    "export_roles": "Not used by Airflow",
    "find_group": "Airflow does not use FAB group lookup",
    "get_first_user": "Not used by Airflow",
    "get_public_permissions": "Not used by Airflow",
    "has_access": "Defined on AirflowSecurityManagerV2 with different signature",
    "import_roles": "Not used by Airflow",
    "is_item_public": "Not used by Airflow",
    "noop_user_update": "Not used by Airflow",
    "oauth_tokengetter": "Airflow uses oauth_token_getter",
    "oauth_user_info_getter": "Not used by Airflow",
    "post_process": "FAB internal hook, not used by Airflow",
    "pre_process": "FAB internal hook, not used by Airflow",
    "registeruser_model": "Managed as class attribute in override.py",
    "security_cleanup": "Not used by Airflow",
    "security_converge": "Not used by Airflow",
    # --- FAB 5.2.0: delete methods with different signatures ---
    "add_group": "Airflow does not use FAB group management",
    "delete_group": "Airflow does not use FAB group deletion",
    "delete_role": "Airflow has its own delete_role with different signature (by name, not id)",
    "delete_user": "Airflow does not use FAB user deletion",
}


# Methods where Airflow intentionally uses a different signature than FAB.
# These are not bugs — Airflow's override.py deliberately changed the API.
KNOWN_SIGNATURE_DEVIATIONS: set[str] = {
    # Airflow's add_permissions_menu/view take no args (self-contained)
    "add_permissions_menu",
    "add_permissions_view",
    # Airflow passes no args; the app is accessed via self.appbuilder.app
    "create_jwt_manager",
    "create_limiter",
    "create_login_manager",
    # Airflow's delete_role takes role_name (str), FAB takes role_or_id
    "delete_role",
    # Airflow's has_access is on AirflowSecurityManagerV2 with a different contract
    "has_access",
    # Airflow's update_role takes (role_id, name), FAB takes (pk, name)
    "update_role",
}


def _get_fab_sqla_manager_path() -> Path:
    """Find the installed FAB sqla manager source file."""
    spec = importlib.util.find_spec("flask_appbuilder.security.sqla.manager")
    if spec is None or spec.origin is None:
        raise RuntimeError("Cannot find flask_appbuilder.security.sqla.manager")
    return Path(spec.origin)


def _get_sqla_manager_own_methods() -> set[str]:
    """Get public methods defined directly on FAB's sqla SecurityManager via AST.

    We use AST parsing instead of importing to avoid SQLAlchemy metadata
    collisions between FAB's models and Airflow's vendored models.
    """
    source = _get_fab_sqla_manager_path().read_text()
    tree = ast.parse(source)
    methods: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == "SecurityManager":
            for item in node.body:
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    if not item.name.startswith("_"):
                        methods.add(item.name)
                elif isinstance(item, ast.Assign):
                    for target in item.targets:
                        if isinstance(target, ast.Name) and not target.id.startswith("_"):
                            methods.add(target.id)
            break
    return methods


def _get_fab_public_methods() -> set[str]:
    """Get all public methods and properties from FAB's security managers."""
    result = set()
    for name, obj in inspect.getmembers(BaseSecurityManager):
        if name.startswith("_"):
            continue
        if callable(obj) or isinstance(obj, property):
            result.add(name)
    result |= _get_sqla_manager_own_methods()
    return result


def _get_override_public_members() -> set[str]:
    """Get all public members from FabAirflowSecurityManagerOverride."""
    return {name for name in dir(FabAirflowSecurityManagerOverride) if not name.startswith("_")}


class TestFabVersionAlignment:
    def test_fab_version_matches_expected(self):
        """Fail if installed FAB version doesn't match what override.py was aligned with.

        override.py vendors parts of FAB's security manager. When FAB is upgraded,
        override.py must be reviewed for needed changes and this assertion updated.
        """
        fab_version = importlib.metadata.version("flask-appbuilder")
        assert fab_version == EXPECTED_FAB_VERSION, (
            f"Installed FAB version is {fab_version}, but override.py was aligned with "
            f"{EXPECTED_FAB_VERSION}. Review override.py against the new FAB version, "
            f"transplant any relevant changes, then update EXPECTED_FAB_VERSION."
        )

    def test_no_unaudited_fab_methods(self):
        """Fail if FAB has public methods not present in override.py and not explicitly excluded.

        Every FAB public method must be either:
        1. Implemented in FabAirflowSecurityManagerOverride, or
        2. Listed in AUDITED_EXCLUSIONS with a justification.

        If this test fails after a FAB upgrade, review each new method and either
        implement it in override.py or add it to AUDITED_EXCLUSIONS.
        """
        fab_methods = _get_fab_public_methods()
        override_members = _get_override_public_members()
        excluded = set(AUDITED_EXCLUSIONS.keys())

        unaudited = fab_methods - override_members - excluded
        assert not unaudited, (
            f"FAB has public methods not accounted for in override.py or AUDITED_EXCLUSIONS: "
            f"{sorted(unaudited)}. Either implement them in override.py or add them to "
            f"AUDITED_EXCLUSIONS in test_fab_alignment.py with a justification."
        )

    def test_no_stale_exclusions(self):
        """Warn if AUDITED_EXCLUSIONS contains methods that FAB no longer has."""
        fab_methods = _get_fab_public_methods()
        excluded = set(AUDITED_EXCLUSIONS.keys())

        stale = excluded - fab_methods
        assert not stale, (
            f"AUDITED_EXCLUSIONS contains methods no longer in FAB: {sorted(stale)}. "
            f"Remove them from the exclusion list."
        )

    def test_shared_method_signatures_compatible(self):
        """Verify that methods present in both override.py and FAB have compatible signatures.

        For each method that override.py re-implements from FAB, check that the required
        parameters (those without defaults) in FAB's version are also present in override.py's
        version. This catches breaking signature changes in FAB.
        """
        fab_methods = _get_fab_public_methods()
        override_members = _get_override_public_members()
        shared = fab_methods & override_members - KNOWN_SIGNATURE_DEVIATIONS

        incompatible = []
        for name in sorted(shared):
            # Only check signatures from BaseSecurityManager (safe to import).
            # sqla SecurityManager methods are checked via AST (no signatures).
            if not hasattr(BaseSecurityManager, name):
                continue
            fab_attr = getattr(BaseSecurityManager, name)
            if not callable(fab_attr) or isinstance(fab_attr, property):
                continue
            fab_method = fab_attr

            override_attr = getattr(FabAirflowSecurityManagerOverride, name, None)

            if fab_method is None or override_attr is None:
                continue
            if not callable(override_attr) or isinstance(override_attr, property):
                continue

            try:
                fab_sig = inspect.signature(fab_method)
                override_sig = inspect.signature(override_attr)
            except (ValueError, TypeError):
                continue

            # Get required params (no default) excluding 'self'
            fab_required = {
                p.name
                for p in fab_sig.parameters.values()
                if p.default is inspect.Parameter.empty
                and p.name != "self"
                and p.kind not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
            }
            override_params = {p.name for p in override_sig.parameters.values() if p.name != "self"}

            missing = fab_required - override_params
            if missing:
                incompatible.append(
                    f"  {name}: FAB requires {sorted(missing)} but override.py is missing them"
                )

        assert not incompatible, (
            "Method signature incompatibilities between FAB and override.py:\n" + "\n".join(incompatible)
        )


class EmptySecurityManager(FabAirflowSecurityManagerOverride):
    # super() not called on purpose to avoid the whole chain of init calls
    def __init__(self):
        pass


class TestUpdateUserChangedOn:
    """Test the changed_on fix transplanted from FAB 5.1.0."""

    @mock.patch("airflow.providers.fab.auth_manager.security_manager.override.log")
    def test_update_user_sets_changed_on_when_roles_change(self, mock_log):
        sm = EmptySecurityManager()

        old_role = Mock(spec=Role, id=1)
        new_role = Mock(spec=Role, id=2)
        mock_group = Mock(id=10)

        existing_user = Mock()
        existing_user.roles = [old_role]
        existing_user.groups = [mock_group]

        user = Mock()
        user.id = 42
        user.roles = [new_role]
        user.groups = [mock_group]
        user.changed_on = None

        mock_session = Mock(spec=Session)
        mock_session.get.return_value = existing_user
        sm.user_model = Mock

        with mock.patch.object(EmptySecurityManager, "session", mock_session):
            result = sm.update_user(user)

        assert result is True
        assert user.changed_on is not None
        assert isinstance(user.changed_on, datetime.datetime)
        assert user.changed_on.tzinfo == datetime.timezone.utc
        mock_session.merge.assert_called_once_with(user)
        mock_session.commit.assert_called_once()

    @mock.patch("airflow.providers.fab.auth_manager.security_manager.override.log")
    def test_update_user_does_not_set_changed_on_when_roles_unchanged(self, mock_log):
        sm = EmptySecurityManager()

        role = Mock(spec=Role, id=1)
        mock_group = Mock(id=10)

        existing_user = Mock()
        existing_user.roles = [role]
        existing_user.groups = [mock_group]

        user = Mock()
        user.id = 42
        user.roles = [role]
        user.groups = [mock_group]
        user.changed_on = None

        mock_session = Mock(spec=Session)
        mock_session.get.return_value = existing_user
        sm.user_model = Mock

        with mock.patch.object(EmptySecurityManager, "session", mock_session):
            result = sm.update_user(user)

        assert result is True
        assert user.changed_on is None
        mock_session.merge.assert_called_once_with(user)
        mock_session.commit.assert_called_once()

    @mock.patch("airflow.providers.fab.auth_manager.security_manager.override.log")
    def test_update_user_sets_changed_on_when_groups_change(self, mock_log):
        sm = EmptySecurityManager()

        role = Mock(spec=Role, id=1)
        old_group = Mock(id=10)
        new_group = Mock(id=20)

        existing_user = Mock()
        existing_user.roles = [role]
        existing_user.groups = [old_group]

        user = Mock()
        user.id = 42
        user.roles = [role]
        user.groups = [new_group]
        user.changed_on = None

        mock_session = Mock(spec=Session)
        mock_session.get.return_value = existing_user
        sm.user_model = Mock

        with mock.patch.object(EmptySecurityManager, "session", mock_session):
            result = sm.update_user(user)

        assert result is True
        assert user.changed_on is not None
        assert user.changed_on.tzinfo == datetime.timezone.utc
