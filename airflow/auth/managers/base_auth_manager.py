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
from functools import cached_property
from typing import TYPE_CHECKING, Container, Literal, Sequence

from sqlalchemy import select

from airflow.auth.managers.models.resource_details import (
    DagDetails,
)
from airflow.exceptions import AirflowException
from airflow.models import DagModel
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from connexion import FlaskApi
    from sqlalchemy.orm import Session

    from airflow.auth.managers.models.base_user import BaseUser
    from airflow.auth.managers.models.batch_apis import (
        IsAuthorizedConnectionRequest,
        IsAuthorizedDagRequest,
        IsAuthorizedPoolRequest,
        IsAuthorizedVariableRequest,
    )
    from airflow.auth.managers.models.resource_details import (
        AccessView,
        ConfigurationDetails,
        ConnectionDetails,
        DagAccessEntity,
        DatasetDetails,
        PoolDetails,
        VariableDetails,
    )
    from airflow.cli.cli_config import CLICommand
    from airflow.www.extensions.init_appbuilder import AirflowAppBuilder
    from airflow.www.security_manager import AirflowSecurityManagerV2

ResourceMethod = Literal["GET", "POST", "PUT", "DELETE"]


class BaseAuthManager(LoggingMixin):
    """
    Class to derive in order to implement concrete auth managers.

    Auth managers are responsible for any user management related operation such as login, logout, authz, ...

    :param appbuilder: the flask app builder
    """

    def __init__(self, appbuilder: AirflowAppBuilder) -> None:
        super().__init__()
        self.appbuilder = appbuilder

    @staticmethod
    def get_cli_commands() -> list[CLICommand]:
        """Vends CLI commands to be included in Airflow CLI.

        Override this method to expose commands via Airflow CLI to manage this auth manager.
        """
        return []

    def get_api_endpoints(self) -> None | FlaskApi:
        """Return API endpoint(s) definition for the auth manager."""
        return None

    def get_user_name(self) -> str:
        """Return the username associated to the user in session."""
        user = self.get_user()
        if not user:
            self.log.error("Calling 'get_user_name()' but the user is not signed in.")
            raise AirflowException("The user must be signed in.")
        return user.get_name()

    def get_user_display_name(self) -> str:
        """Return the user's display name associated to the user in session."""
        return self.get_user_name()

    @abstractmethod
    def get_user(self) -> BaseUser | None:
        """Return the user associated to the user in session."""

    def get_user_id(self) -> str:
        """Return the user ID associated to the user in session."""
        user = self.get_user()
        if not user:
            self.log.error("Calling 'get_user_id()' but the user is not signed in.")
            raise AirflowException("The user must be signed in.")
        return str(user.get_id())

    def init(self) -> None:
        """
        Run operations when Airflow is initializing.

        By default, do nothing.
        """

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
    def is_authorized_view(
        self,
        *,
        access_view: AccessView,
        user: BaseUser | None = None,
    ) -> bool:
        """
        Return whether the user is authorized to access a read-only state of the installation.

        :param access_view: the specific read-only view/state the authorization request is about.
        :param user: the user to perform the action on. If not provided (or None), it uses the current user
        """

    def is_authorized_custom_view(
        self, *, fab_action_name: str, fab_resource_name: str, user: BaseUser | None = None
    ):
        """
        Return whether the user is authorized to perform a given action on a custom view.

        A custom view is a view defined as part of the auth manager. This view is then only available when
        the auth manager is used as part of the environment.

        By default, it throws an exception because auth managers do not define custom views by default.
        If an auth manager defines some custom views, it needs to override this method.

        :param fab_action_name: the name of the FAB action defined in the view in ``base_permissions``
        :param fab_resource_name: the name of the FAB resource defined in the view in
            ``class_permission_name``
        :param user: the user to perform the action on. If not provided (or None), it uses the current user
        """
        raise AirflowException(f"The resource `{fab_resource_name}` does not exist in the environment.")

    def batch_is_authorized_dag(
        self,
        requests: Sequence[IsAuthorizedDagRequest],
    ) -> bool:
        """
        Batch version of ``is_authorized_dag``.

        By default, calls individually the ``is_authorized_dag`` API on each item in the list of requests.
        Can lead to some poor performance. It is recommended to override this method in the auth manager
        implementation to provide a more efficient implementation.

        :param requests: a list of requests containing the parameters for ``is_authorized_dag``
        """
        return all(
            self.is_authorized_dag(
                method=request["method"],
                access_entity=request.get("access_entity"),
                details=request.get("details"),
                user=request.get("user"),
            )
            for request in requests
        )

    def batch_is_authorized_connection(
        self,
        requests: Sequence[IsAuthorizedConnectionRequest],
    ) -> bool:
        """
        Batch version of ``is_authorized_connection``.

        By default, calls individually the ``is_authorized_connection`` API on each item in the list of
        requests. Can lead to some poor performance. It is recommended to override this method in the auth
        manager implementation to provide a more efficient implementation.

        :param requests: a list of requests containing the parameters for ``is_authorized_connection``
        """
        return all(
            self.is_authorized_connection(
                method=request["method"], details=request.get("details"), user=request.get("user")
            )
            for request in requests
        )

    def batch_is_authorized_pool(
        self,
        requests: Sequence[IsAuthorizedPoolRequest],
    ) -> bool:
        """
        Batch version of ``is_authorized_pool``.

        By default, calls individually the ``is_authorized_pool`` API on each item in the list of
        requests. Can lead to some poor performance. It is recommended to override this method in the auth
        manager implementation to provide a more efficient implementation.

        :param requests: a list of requests containing the parameters for ``is_authorized_pool``
        """
        return all(
            self.is_authorized_pool(
                method=request["method"], details=request.get("details"), user=request.get("user")
            )
            for request in requests
        )

    def batch_is_authorized_variable(
        self,
        requests: Sequence[IsAuthorizedVariableRequest],
    ) -> bool:
        """
        Batch version of ``is_authorized_variable``.

        By default, calls individually the ``is_authorized_variable`` API on each item in the list of
        requests. Can lead to some poor performance. It is recommended to override this method in the auth
        manager implementation to provide a more efficient implementation.

        :param requests: a list of requests containing the parameters for ``is_authorized_variable``
        """
        return all(
            self.is_authorized_variable(
                method=request["method"], details=request.get("details"), user=request.get("user")
            )
            for request in requests
        )

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

    def get_url_user_profile(self) -> str | None:
        """
        Return the url to a page displaying info about the current user.

        By default, return None.
        """
        return None

    @cached_property
    def security_manager(self) -> AirflowSecurityManagerV2:
        """
        Return the security manager.

        By default, Airflow comes with the default security manager
        ``airflow.www.security_manager.AirflowSecurityManagerV2``. The auth manager might need to extend this
        default security manager for its own purposes.

        By default, return the default AirflowSecurityManagerV2.
        """
        from airflow.www.security_manager import AirflowSecurityManagerV2

        return AirflowSecurityManagerV2(self.appbuilder)
