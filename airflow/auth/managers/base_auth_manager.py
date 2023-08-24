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

from abc import abstractmethod
from typing import TYPE_CHECKING, Sequence

from airflow.auth.managers.models.authorized_action import AuthorizedAction
from airflow.auth.managers.models.resource_action import ResourceAction
from airflow.auth.managers.models.resource_details import ResourceDetails
from airflow.exceptions import AirflowException
from airflow.models.dag import DagModel
from airflow.security.permissions import RESOURCE_DAG, RESOURCE_DAG_PREFIX
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from airflow.auth.managers.models.base_user import BaseUser
    from airflow.cli.cli_config import CLICommand
    from airflow.www.security import AirflowSecurityManager


class BaseAuthManager(LoggingMixin):
    """
    Class to derive in order to implement concrete auth managers.

    Auth managers are responsible for any user management related operation such as login, logout, authz, ...
    """

    def __init__(self):
        self._security_manager: AirflowSecurityManager | None = None

    @staticmethod
    def get_cli_commands() -> list[CLICommand]:
        """Vends CLI commands to be included in Airflow CLI.

        Override this method to expose commands via Airflow CLI to manage this auth manager.
        """
        return []

    @abstractmethod
    def get_user_name(self) -> str:
        """Return the username associated to the user in session."""

    @abstractmethod
    def get_user(self) -> BaseUser:
        """Return the user associated to the user in session."""

    @abstractmethod
    def get_user_id(self) -> str:
        """Return the user ID associated to the user in session."""

    @abstractmethod
    def is_logged_in(self) -> bool:
        """Return whether the user is logged in."""

    @abstractmethod
    def is_authorized(
        self,
        action: ResourceAction,
        resource_type: str,
        resource_details: ResourceDetails | None = None,
        user: BaseUser | None = None,
    ) -> bool:
        """
        Return whether the user is authorized to perform a given action.

        .. code-block:: python

            # Check whether the logged-in user has permission to read the DAG "my_dag_id"
            get_auth_manager().is_authorized(
                Action.GET,
                Resource.DAG,
                ResourceDetails(
                    id="my_dag_id",
                ),
            )

        :param action: the action to perform
        :param resource_type: the type of resource the user attempts to perform the action on
        :param resource_details: optional details about the resource itself
        :param user: the user to perform the action on. If not provided (or None), it uses the current user
        """

    def is_all_authorized(
        self,
        actions: Sequence[AuthorizedAction],
    ) -> bool:
        """
        Wrapper around `is_authorized` to check whether the user is authorized to perform several actions.

        :param actions: the list of actions to check. Each item represents the list of parameters of
            `is_authorized`
        """
        return all(
            self.is_authorized(
                action=action.action,
                resource_type=action.resource_type,
                resource_details=action.resource_details,
                user=action.user,
            )
            for action in actions
        )

    def _get_root_dag_id(self, dag_id: str) -> str:
        """
        Return the root DAG id in case of sub DAG, return the DAG id otherwise.

        :param dag_id: the DAG id
        """
        if "." in dag_id:
            dm = (
                self.security_manager.appbuilder.get_session.query(DagModel.dag_id, DagModel.root_dag_id)
                .filter(DagModel.dag_id == dag_id)
                .first()
            )
            return dm.root_dag_id or dm.dag_id
        return dag_id

    @abstractmethod
    def get_url_login(self, **kwargs) -> str:
        """Return the login page url."""

    @abstractmethod
    def get_url_logout(self) -> str:
        """Return the logout page url."""

    @abstractmethod
    def get_url_user_profile(self) -> str | None:
        """Return the url to a page displaying info about the current user."""

    def get_security_manager_override_class(self) -> type:
        """
        Return the security manager override class.

        The security manager override class is responsible for overriding the default security manager
        class airflow.www.security.AirflowSecurityManager with a custom implementation. This class is
        essentially inherited from airflow.www.security.AirflowSecurityManager.

        By default, return an empty class.
        """
        return object

    @property
    def security_manager(self) -> AirflowSecurityManager:
        """Get the security manager."""
        if not self._security_manager:
            raise AirflowException("Security manager not defined.")
        return self._security_manager

    @security_manager.setter
    def security_manager(self, security_manager: AirflowSecurityManager):
        """
        Set the security manager.

        :param security_manager: the security manager
        """
        self._security_manager = security_manager

    @staticmethod
    def is_dag_resource(resource_name: str) -> bool:
        """Determines if a resource relates to a DAG."""
        return resource_name == RESOURCE_DAG or resource_name.startswith(RESOURCE_DAG_PREFIX)
