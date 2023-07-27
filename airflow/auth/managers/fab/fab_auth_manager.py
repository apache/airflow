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

from flask import url_for
from flask_login import current_user

from airflow import AirflowException
from airflow.auth.managers.base_auth_manager import BaseAuthManager
from airflow.auth.managers.fab.security_manager_override import FabAirflowSecurityManagerOverride


class FabAuthManager(BaseAuthManager):
    """
    Flask-AppBuilder auth manager.

    This auth manager is responsible for providing a backward compatible user management experience to users.
    """

    def get_user_name(self) -> str:
        """
        Return the username associated to the user in session.

        For backward compatibility reasons, the username in FAB auth manager is the concatenation of the
        first name and the last name.
        """
        first_name = current_user.first_name or ""
        last_name = current_user.last_name or ""
        return f"{first_name} {last_name}".strip()

    def is_logged_in(self) -> bool:
        """Return whether the user is logged in."""
        return current_user and not current_user.is_anonymous

    def get_security_manager_override_class(self) -> type:
        """Return the security manager override."""
        return FabAirflowSecurityManagerOverride

    def get_url_login(self, **kwargs) -> str:
        """Return the login page url."""
        if not self.security_manager.auth_view:
            raise AirflowException("`auth_view` not defined in the security manager.")
        if "next_url" in kwargs and kwargs["next_url"]:
            return url_for(f"{self.security_manager.auth_view.endpoint}.login", next=kwargs["next_url"])
        else:
            return url_for(f"{self.security_manager.auth_view.endpoint}.login")

    def get_url_user_profile(self) -> str | None:
        """Return the url to a page displaying info about the current user."""
        if not self.security_manager.user_view:
            return None
        return url_for(f"{self.security_manager.user_view.endpoint}.userinfo")
