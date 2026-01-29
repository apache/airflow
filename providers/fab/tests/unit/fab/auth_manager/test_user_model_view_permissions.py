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
from sqlalchemy import select

from airflow.providers.fab.auth_manager.models import Permission
from airflow.providers.fab.www import app as application
from airflow.providers.fab.www.security import permissions

from tests_common.test_utils.config import conf_vars

if TYPE_CHECKING:
    from airflow.providers.fab.auth_manager.security_manager.override import FabAirflowSecurityManagerOverride

pytestmark = pytest.mark.db_test


@pytest.fixture
def app():
    """Create Flask app for testing."""
    with conf_vars(
        {
            (
                "core",
                "auth_manager",
            ): "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager",
        }
    ):
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

    def test_user_permissions_registered_after_sync(self, app, security_manager):
        """Test that user model view permissions are registered after security_manager.sync_roles()."""
        security_manager.sync_roles()
        user_perms = _get_user_permissions(security_manager)
        admin_has_perms = _admin_has_user_permissions(security_manager)

        assert len(user_perms) >= 4
        assert (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_USER) in user_perms
        assert (permissions.ACTION_CAN_READ, permissions.RESOURCE_USER) in user_perms
        assert (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_USER) in user_perms
        assert (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_USER) in user_perms
        assert admin_has_perms

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
