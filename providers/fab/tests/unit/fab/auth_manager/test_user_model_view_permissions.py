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
"""Test user model view permissions registration."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from flask_appbuilder.const import AUTH_DB, AUTH_LDAP
from sqlalchemy import select

from airflow.providers.fab.auth_manager.models import Permission
from airflow.providers.fab.www import app as application
from airflow.providers.fab.www.security import permissions

if TYPE_CHECKING:
    from airflow.providers.fab.auth_manager.security_manager.override import FabAirflowSecurityManagerOverride

pytestmark = pytest.mark.db_test


@pytest.fixture
def app():
    """Create Flask app for testing."""
    _app = application.create_app(enable_plugins=False)
    _app.config["WTF_CSRF_ENABLED"] = False
    with _app.app_context():
        yield _app


@pytest.fixture
def security_manager(app):
    """Get security manager from app."""
    return app.appbuilder.sm


def _get_user_permissions(security_manager: FabAirflowSecurityManagerOverride) -> set[tuple[str, str]]:
    """Get all permissions for RESOURCE_USER."""
    resource = security_manager.get_resource(permissions.RESOURCE_USER)
    if not resource:
        return set()

    perms = security_manager.session.scalars(
        select(Permission).where(Permission.resource_id == resource.id)
    ).all()

    return {(perm.action.name, perm.resource.name) for perm in perms if perm.action and perm.resource}


def _admin_has_user_permissions(security_manager: FabAirflowSecurityManagerOverride) -> bool:
    """Check if Admin role has all user model view permissions."""
    admin = security_manager.find_role("Admin")
    if not admin:
        return False

    required_perms = {
        (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_USER),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_USER),
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_USER),
        (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_USER),
    }

    admin_perms = {
        (perm.action.name, perm.resource.name) for perm in admin.permissions if perm.action and perm.resource
    }
    return required_perms.issubset(admin_perms)


class TestUserModelViewPermissions:
    """Test that user model view permissions are properly registered for Admin role."""

    def test_user_permissions_registered_with_db_auth(self, app, security_manager):
        """
        Test that user model view permissions are registered when using DB auth.

        This test verifies the fix for issue where Admin users were denied access
        to user management views when permissions weren't properly synced.
        """
        # Set auth type to DB (so CustomUserLDAPModelView is not the active view)
        security_manager.auth_type = AUTH_DB

        # Register views (this happens during app initialization)
        security_manager.register_views()

        # Clear any existing permissions to simulate a fresh state
        # This simulates the scenario where permissions haven't been synced
        resource = security_manager.get_resource(permissions.RESOURCE_USER)
        if resource:
            perms = security_manager.session.scalars(
                select(Permission).where(Permission.resource_id == resource.id)
            ).all()
            for perm in perms:
                # Remove from Admin role
                admin = security_manager.find_role("Admin")
                if admin and perm in admin.permissions:
                    admin.permissions.remove(perm)
            security_manager.session.commit()

        # BEFORE FIX: Without ensure_user_model_view_permissions,
        # permissions might not be registered if the view isn't active
        # Let's verify the current state
        admin_has_perms_before = _admin_has_user_permissions(security_manager)

        # Sync roles - this should call ensure_user_model_view_permissions
        security_manager.sync_roles()

        # AFTER FIX: Permissions should now be registered
        user_perms_after = _get_user_permissions(security_manager)
        admin_has_perms_after = _admin_has_user_permissions(security_manager)

        # Verify permissions exist in database
        assert len(user_perms_after) >= 4, "User model view permissions should be registered"
        assert (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_USER) in user_perms_after
        assert (permissions.ACTION_CAN_READ, permissions.RESOURCE_USER) in user_perms_after
        assert (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_USER) in user_perms_after
        assert (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_USER) in user_perms_after

        # Verify Admin has all user permissions
        assert admin_has_perms_after, "Admin role should have all user model view permissions after sync"

        # The fix ensures permissions are registered regardless of auth type
        print(f"\nBEFORE FIX (simulated): Admin has user permissions: {admin_has_perms_before}")
        print(f"AFTER FIX: Admin has user permissions: {admin_has_perms_after}")
        print(f"User permissions in DB: {user_perms_after}")

    def test_user_permissions_registered_with_ldap_auth(self, app, security_manager):
        """
        Test that user model view permissions are registered when using LDAP auth.

        This ensures the fix works for all authentication types.
        """
        # Set auth type to LDAP
        security_manager.auth_type = AUTH_LDAP

        # Register views
        security_manager.register_views()

        # Sync roles
        security_manager.sync_roles()

        # Verify permissions exist
        user_perms = _get_user_permissions(security_manager)
        admin_has_perms = _admin_has_user_permissions(security_manager)

        assert len(user_perms) >= 4, "User model view permissions should be registered"
        assert admin_has_perms, "Admin role should have all user model view permissions"

    def test_ensure_user_model_view_permissions_called_during_sync(self, app, security_manager):
        """Test that ensure_user_model_view_permissions is called during sync_roles."""
        # Mock the method to verify it's called
        original_method = security_manager.ensure_user_model_view_permissions
        call_count = {"count": 0}

        def mock_ensure():
            call_count["count"] += 1
            return original_method()

        security_manager.ensure_user_model_view_permissions = mock_ensure

        # Sync roles
        security_manager.sync_roles()

        # Verify the method was called
        assert call_count["count"] == 1, (
            "ensure_user_model_view_permissions should be called during sync_roles"
        )

        # Restore original method
        security_manager.ensure_user_model_view_permissions = original_method

    def test_admin_has_all_user_permissions_after_sync(self, app, security_manager):
        """
        Test that Admin role has all user model view permissions after sync.

        This is the core fix - ensuring Admin gets all permissions regardless
        of which authentication type is active.
        """
        # Test with DB auth
        security_manager.auth_type = AUTH_DB
        security_manager.register_views()
        security_manager.sync_roles()

        admin_db = security_manager.find_role("Admin")
        admin_perms_db = {
            (p.action.name, p.resource.name) for p in admin_db.permissions if p.action and p.resource
        }

        # Test with LDAP auth
        security_manager.auth_type = AUTH_LDAP
        security_manager.register_views()
        security_manager.sync_roles()

        admin_ldap = security_manager.find_role("Admin")
        admin_perms_ldap = {
            (p.action.name, p.resource.name) for p in admin_ldap.permissions if p.action and p.resource
        }

        # Both should have user permissions
        required_user_perms = {
            (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_USER),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_USER),
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_USER),
            (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_USER),
        }

        assert required_user_perms.issubset(admin_perms_db), "Admin should have user permissions with DB auth"
        assert required_user_perms.issubset(admin_perms_ldap), (
            "Admin should have user permissions with LDAP auth"
        )

        print(
            f"\nAdmin permissions with DB auth include user perms: {required_user_perms.issubset(admin_perms_db)}"
        )
        print(
            f"Admin permissions with LDAP auth include user perms: {required_user_perms.issubset(admin_perms_ldap)}"
        )
