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
from typing import TYPE_CHECKING, Container, Literal

from sqlalchemy import select

from airflow.exceptions import AirflowException
from airflow.models import DagModel
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from flask import Flask
    from sqlalchemy.orm import Session

    from airflow.auth.managers.models.base_user import BaseUser
    from airflow.auth.managers.models.resource_details import (
        ConfigurationDetails,
        ConnectionDetails,
        DagAccessEntity,
        DagDetails,
        DatasetDetails,
        PoolDetails,
        VariableDetails,
    )
    from airflow.cli.cli_config import CLICommand
    from airflow.www.security_manager import AirflowSecurityManagerV2

ResourceMethod = Literal["GET", "POST", "PUT", "DELETE"]


class BaseAuthManager(LoggingMixin):
    """
    Class to derive in order to implement concrete auth managers.

    Auth managers are responsible for any user management related operation such as login, logout, authz, ...
    """

    def __init__(self, app: Flask) -> None:
        self._security_manager: AirflowSecurityManagerV2 | None = None
        self.app = app

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
    def get_user_display_name(self) -> str:
        """Return the user's display name associated to the user in session."""

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
    def is_authorized_configuration(
        self,
        *,
        method: ResourceMethod,
        details: ConfigurationDetails | None = None,
        user: BaseUser | None = None,
    ) -> bool:
        """
        Return whether the user is authorized to perform a given action on configuration.

        :param method: the method to perform
        :param details: optional details about the configuration
        :param user: the user to perform the action on. If not provided (or None), it uses the current user
        """

    @abstractmethod
    def is_authorized_cluster_activity(
        self,
        *,
        method: ResourceMethod,
        user: BaseUser | None = None,
    ) -> bool:
        """
        Return whether the user is authorized to perform a given action on the cluster activity.

        :param method: the method to perform
        :param user: the user to perform the action on. If not provided (or None), it uses the current user
        """

    @abstractmethod
    def is_authorized_connection(
        self,
        *,
        method: ResourceMethod,
        details: ConnectionDetails | None = None,
        user: BaseUser | None = None,
    ) -> bool:
        """
        Return whether the user is authorized to perform a given action on a connection.

        :param method: the method to perform
        :param details: optional details about the connection
        :param user: the user to perform the action on. If not provided (or None), it uses the current user
        """

    @abstractmethod
    def is_authorized_dag(
        self,
        *,
        method: ResourceMethod,
        access_entity: DagAccessEntity | None = None,
        details: DagDetails | None = None,
        user: BaseUser | None = None,
    ) -> bool:
        """
        Return whether the user is authorized to perform a given action on a DAG.

        :param method: the method to perform
        :param access_entity: the kind of DAG information the authorization request is about.
            If not provided, the authorization request is about the DAG itself
        :param details: optional details about the DAG
        :param user: the user to perform the action on. If not provided (or None), it uses the current user
        """

    @abstractmethod
    def is_authorized_dataset(
        self,
        *,
        method: ResourceMethod,
        details: DatasetDetails | None = None,
        user: BaseUser | None = None,
    ) -> bool:
        """
        Return whether the user is authorized to perform a given action on a dataset.

        :param method: the method to perform
        :param details: optional details about the dataset
        :param user: the user to perform the action on. If not provided (or None), it uses the current user
        """

    @abstractmethod
    def is_authorized_pool(
        self,
        *,
        method: ResourceMethod,
        details: PoolDetails | None = None,
        user: BaseUser | None = None,
    ) -> bool:
        """
        Return whether the user is authorized to perform a given action on a pool.

        :param method: the method to perform
        :param details: optional details about the pool
        :param user: the user to perform the action on. If not provided (or None), it uses the current user
        """

    @abstractmethod
    def is_authorized_variable(
        self,
        *,
        method: ResourceMethod,
        details: VariableDetails | None = None,
        user: BaseUser | None = None,
    ) -> bool:
        """
        Return whether the user is authorized to perform a given action on a variable.

        :param method: the method to perform
        :param details: optional details about the variable
        :param user: the user to perform the action on. If not provided (or None), it uses the current user
        """

    @abstractmethod
    def is_authorized_website(
        self,
        *,
        user: BaseUser | None = None,
    ) -> bool:
        """
        Return whether the user is authorized to access the read-only state of the installation.

        This includes the homepage, the list of installed plugins, the list of providers and list of triggers.

        :param user: the user to perform the action on. If not provided (or None), it uses the current user
        """

    @provide_session
    def get_permitted_dag_ids(
        self,
        *,
        methods: Container[ResourceMethod] | None = None,
        user=None,
        session: Session = NEW_SESSION,
    ) -> set[str]:
        """
        Get readable or writable DAGs for user.

        By default, reads all the DAGs and check individually if the user has permissions to access the DAG.
        Can lead to some poor performance. It is recommended to override this method in the auth manager
        implementation to provide a more efficient implementation.
        """
        if not methods:
            methods = ["PUT", "GET"]

        dag_ids = {dag.dag_id for dag in session.execute(select(DagModel.dag_id))}

        if ("GET" in methods and self.is_authorized_dag(method="GET", user=user)) or (
            "PUT" in methods and self.is_authorized_dag(method="PUT", user=user)
        ):
            # If user is authorized to read/edit all DAGs, return all DAGs
            return dag_ids

        def _is_permitted_dag_id(method: ResourceMethod, methods: Container[ResourceMethod], dag_id: str):
            return method in methods and self.is_authorized_dag(
                method=method, details=DagDetails(id=dag_id), user=user
            )

        return {
            dag_id
            for dag_id in dag_ids
            if _is_permitted_dag_id("GET", methods, dag_id) or _is_permitted_dag_id("PUT", methods, dag_id)
        }

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
        class airflow.www.security_manager.AirflowSecurityManagerV2 with a custom implementation.
        This class is essentially inherited from airflow.www.security_manager.AirflowSecurityManagerV2.

        By default, return the generic AirflowSecurityManagerV2.
        """
        from airflow.www.security_manager import AirflowSecurityManagerV2

        return AirflowSecurityManagerV2

    @property
    def security_manager(self) -> AirflowSecurityManagerV2:
        """Get the security manager."""
        if not self._security_manager:
            raise AirflowException("Security manager not defined.")
        return self._security_manager

    @security_manager.setter
    def security_manager(self, security_manager: AirflowSecurityManagerV2):
        """
        Set the security manager.

        :param security_manager: the security manager
        """
        self._security_manager = security_manager
