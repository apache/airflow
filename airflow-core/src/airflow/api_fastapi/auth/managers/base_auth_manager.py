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

import logging
from abc import ABCMeta, abstractmethod
from functools import cache
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar

from jwt import InvalidTokenError
from sqlalchemy import select

from airflow.api_fastapi.auth.managers.models.base_user import BaseUser
from airflow.api_fastapi.auth.managers.models.resource_details import BackfillDetails, DagDetails
from airflow.api_fastapi.auth.tokens import (
    JWTGenerator,
    JWTValidator,
    get_sig_validation_args,
    get_signing_args,
)
from airflow.api_fastapi.common.types import ExtraMenuItem, MenuItem
from airflow.configuration import conf
from airflow.models import DagModel
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from collections.abc import Sequence

    from fastapi import FastAPI
    from sqlalchemy.orm import Session

    from airflow.api_fastapi.auth.managers.models.batch_apis import (
        IsAuthorizedConnectionRequest,
        IsAuthorizedDagRequest,
        IsAuthorizedPoolRequest,
        IsAuthorizedVariableRequest,
    )
    from airflow.api_fastapi.auth.managers.models.resource_details import (
        AccessView,
        AssetAliasDetails,
        AssetDetails,
        ConfigurationDetails,
        ConnectionDetails,
        DagAccessEntity,
        PoolDetails,
        VariableDetails,
    )
    from airflow.cli.cli_config import CLICommand

# This cannot be in the TYPE_CHECKING block since some providers import it globally.
# TODO: Move this inside once all providers drop Airflow 2.x support.
# List of methods (or actions) a user can do against a resource
ResourceMethod = Literal["GET", "POST", "PUT", "DELETE"]
# Extends ``ResourceMethod`` to include "MENU". The method "MENU" is only supported with specific resources (menu items)
ExtendedResourceMethod = Literal["GET", "POST", "PUT", "DELETE", "MENU"]

log = logging.getLogger(__name__)
T = TypeVar("T", bound=BaseUser)


COOKIE_NAME_JWT_TOKEN = "_token"


class BaseAuthManager(Generic[T], LoggingMixin, metaclass=ABCMeta):
    """
    Class to derive in order to implement concrete auth managers.

    Auth managers are responsible for any user management related operation such as login, logout, authz, ...
    """

    def init(self) -> None:
        """
        Run operations when Airflow is initializing.

        By default, do nothing.
        """

    @abstractmethod
    def deserialize_user(self, token: dict[str, Any]) -> T:
        """Create a user object from dict."""

    @abstractmethod
    def serialize_user(self, user: T) -> dict[str, Any]:
        """Create a subject and extra claims dict from a user object."""

    async def get_user_from_token(self, token: str) -> BaseUser:
        """Verify the JWT token is valid and create a user object from it if valid."""
        try:
            payload: dict[str, Any] = await self._get_token_validator().avalidated_claims(token)
        except InvalidTokenError as e:
            log.error("JWT token is not valid: %s", e)
            raise e

        try:
            return self.deserialize_user(payload)
        except (ValueError, KeyError) as e:
            log.error("Couldn't deserialize user from token, JWT token is not valid: %s", e)
            raise InvalidTokenError(str(e))

    def generate_jwt(
        self, user: T, *, expiration_time_in_seconds: int = conf.getint("api_auth", "jwt_expiration_time")
    ) -> str:
        """Return the JWT token from a user object."""
        return self._get_token_signer(expiration_time_in_seconds=expiration_time_in_seconds).generate(
            self.serialize_user(user)
        )

    @abstractmethod
    def get_url_login(self, **kwargs) -> str:
        """Return the login page url."""

    def get_url_logout(self) -> str | None:
        """
        Return the logout page url.

        The user is redirected to this URL when logging out. If None is returned (by default), no redirection
        is performed. This redirection is usually needed to invalidate resources when logging out, such as a
        session.
        """
        return None

    def refresh_user(self, *, user: T) -> T | None:
        """
        Refresh the user if needed.

        By default, does nothing. Some auth managers might need to refresh the user to, for instance,
        refresh some tokens that are needed to communicate with a service/tool.

        This method is called by every single request, it must be lightweight otherwise the overall API
        server latency will increase.
        """
        return None

    @abstractmethod
    def is_authorized_configuration(
        self,
        *,
        method: ResourceMethod,
        user: T,
        details: ConfigurationDetails | None = None,
    ) -> bool:
        """
        Return whether the user is authorized to perform a given action on configuration.

        :param method: the method to perform
        :param user: the user to performing the action
        :param details: optional details about the configuration
        """

    @abstractmethod
    def is_authorized_connection(
        self,
        *,
        method: ResourceMethod,
        user: T,
        details: ConnectionDetails | None = None,
    ) -> bool:
        """
        Return whether the user is authorized to perform a given action on a connection.

        :param method: the method to perform
        :param user: the user to performing the action
        :param details: optional details about the connection
        """

    @abstractmethod
    def is_authorized_dag(
        self,
        *,
        method: ResourceMethod,
        user: T,
        access_entity: DagAccessEntity | None = None,
        details: DagDetails | None = None,
    ) -> bool:
        """
        Return whether the user is authorized to perform a given action on a DAG.

        :param method: the method to perform
        :param user: the user to performing the action
        :param access_entity: the kind of DAG information the authorization request is about.
            If not provided, the authorization request is about the DAG itself
        :param details: optional details about the DAG
        """

    @abstractmethod
    def is_authorized_backfill(
        self,
        *,
        method: ResourceMethod,
        user: T,
        details: BackfillDetails | None = None,
    ) -> bool:
        """
        Return whether the user is authorized to perform a given action on a backfill.

        :param method: the method to perform
        :param user: the user to performing the action
        :param details: optional details about the backfill
        """

    @abstractmethod
    def is_authorized_asset(
        self,
        *,
        method: ResourceMethod,
        user: T,
        details: AssetDetails | None = None,
    ) -> bool:
        """
        Return whether the user is authorized to perform a given action on an asset.

        :param method: the method to perform
        :param user: the user to performing the action
        :param details: optional details about the asset
        """

    @abstractmethod
    def is_authorized_asset_alias(
        self,
        *,
        method: ResourceMethod,
        user: T,
        details: AssetAliasDetails | None = None,
    ) -> bool:
        """
        Return whether the user is authorized to perform a given action on an asset alias.

        :param method: the method to perform
        :param user: the user to perform the action on
        :param details: optional details about the asset alias
        """

    @abstractmethod
    def is_authorized_pool(
        self,
        *,
        method: ResourceMethod,
        user: T,
        details: PoolDetails | None = None,
    ) -> bool:
        """
        Return whether the user is authorized to perform a given action on a pool.

        :param method: the method to perform
        :param user: the user to performing the action
        :param details: optional details about the pool
        """

    @abstractmethod
    def is_authorized_variable(
        self,
        *,
        method: ResourceMethod,
        user: T,
        details: VariableDetails | None = None,
    ) -> bool:
        """
        Return whether the user is authorized to perform a given action on a variable.

        :param method: the method to perform
        :param user: the user to performing the action
        :param details: optional details about the variable
        """

    @abstractmethod
    def is_authorized_view(
        self,
        *,
        access_view: AccessView,
        user: T,
    ) -> bool:
        """
        Return whether the user is authorized to access a read-only state of the installation.

        :param access_view: the specific read-only view/state the authorization request is about.
        :param user: the user to performing the action
        """

    @abstractmethod
    def is_authorized_custom_view(self, *, method: ResourceMethod | str, resource_name: str, user: T) -> bool:
        """
        Return whether the user is authorized to perform a given action on a custom view.

        A custom view can be a view defined as part of the auth manager. This view is then only available when
        the auth manager is used as part of the environment. It can also be a view defined as part of a
        plugin defined by a user.

        :param method: the method to perform.
            The method can also be a string if the action has been defined in a plugin.
            In that case, the action can be anything (e.g. can_do).
            See https://github.com/apache/airflow/issues/39144
        :param resource_name: the name of the resource
        :param user: the user to performing the action
        """

    @abstractmethod
    def filter_authorized_menu_items(self, menu_items: list[MenuItem], *, user: T) -> list[MenuItem]:
        """
        Filter menu items based on user permissions.

        :param menu_items: list of all menu items
        :param user: the user
        """

    def batch_is_authorized_connection(
        self,
        requests: Sequence[IsAuthorizedConnectionRequest],
        *,
        user: T,
    ) -> bool:
        """
        Batch version of ``is_authorized_connection``.

        By default, calls individually the ``is_authorized_connection`` API on each item in the list of requests.
        Can lead to some poor performance. It is recommended to override this method in the auth manager
        implementation to provide a more efficient implementation.

        :param requests: a list of requests containing the parameters for ``is_authorized_connection``
        :param user: the user to performing the action
        """
        return all(
            self.is_authorized_connection(
                method=request["method"],
                details=request.get("details"),
                user=user,
            )
            for request in requests
        )

    def batch_is_authorized_dag(
        self,
        requests: Sequence[IsAuthorizedDagRequest],
        *,
        user: T,
    ) -> bool:
        """
        Batch version of ``is_authorized_dag``.

        By default, calls individually the ``is_authorized_dag`` API on each item in the list of requests.
        Can lead to some poor performance. It is recommended to override this method in the auth manager
        implementation to provide a more efficient implementation.

        :param requests: a list of requests containing the parameters for ``is_authorized_dag``
        :param user: the user to performing the action
        """
        return all(
            self.is_authorized_dag(
                method=request["method"],
                access_entity=request.get("access_entity"),
                details=request.get("details"),
                user=user,
            )
            for request in requests
        )

    def batch_is_authorized_pool(
        self,
        requests: Sequence[IsAuthorizedPoolRequest],
        *,
        user: T,
    ) -> bool:
        """
        Batch version of ``is_authorized_pool``.

        By default, calls individually the ``is_authorized_pool`` API on each item in the list of requests.
        Can lead to some poor performance. It is recommended to override this method in the auth manager
        implementation to provide a more efficient implementation.

        :param requests: a list of requests containing the parameters for ``is_authorized_pool``
        :param user: the user to performing the action
        """
        return all(
            self.is_authorized_pool(
                method=request["method"],
                details=request.get("details"),
                user=user,
            )
            for request in requests
        )

    def batch_is_authorized_variable(
        self,
        requests: Sequence[IsAuthorizedVariableRequest],
        *,
        user: T,
    ) -> bool:
        """
        Batch version of ``is_authorized_variable``.

        By default, calls individually the ``is_authorized_variable`` API on each item in the list of requests.
        Can lead to some poor performance. It is recommended to override this method in the auth manager
        implementation to provide a more efficient implementation.

        :param requests: a list of requests containing the parameters for ``is_authorized_variable``
        :param user: the user to performing the action
        """
        return all(
            self.is_authorized_variable(
                method=request["method"],
                details=request.get("details"),
                user=user,
            )
            for request in requests
        )

    @provide_session
    def get_authorized_dag_ids(
        self,
        *,
        user: T,
        method: ResourceMethod = "GET",
        session: Session = NEW_SESSION,
    ) -> set[str]:
        """
        Get DAGs the user has access to.

        By default, reads all the DAGs and check individually if the user has permissions to access the DAG.
        Can lead to some poor performance. It is recommended to override this method in the auth manager
        implementation to provide a more efficient implementation.

        :param user: the user
        :param method: the method to filter on
        :param session: the session
        """
        dag_ids = {dag.dag_id for dag in session.execute(select(DagModel.dag_id))}
        return self.filter_authorized_dag_ids(dag_ids=dag_ids, method=method, user=user)

    def filter_authorized_dag_ids(
        self,
        *,
        dag_ids: set[str],
        user: T,
        method: ResourceMethod = "GET",
    ) -> set[str]:
        """
        Filter DAGs the user has access to.

        :param dag_ids: the list of DAG ids
        :param user: the user
        :param method: the method to filter on
        """

        def _is_authorized_dag_id(method: ResourceMethod, dag_id: str):
            return self.is_authorized_dag(method=method, details=DagDetails(id=dag_id), user=user)

        return {dag_id for dag_id in dag_ids if _is_authorized_dag_id(method, dag_id)}

    @staticmethod
    def get_cli_commands() -> list[CLICommand]:
        """
        Vends CLI commands to be included in Airflow CLI.

        Override this method to expose commands via Airflow CLI to manage this auth manager.
        """
        return []

    def get_fastapi_app(self) -> FastAPI | None:
        """
        Specify a sub FastAPI application specific to the auth manager.

        This sub application, if specified, is mounted in the main FastAPI application.
        """
        return None

    def get_authorized_menu_items(self, *, user: T) -> list[MenuItem]:
        """Get all menu items the user has access to."""
        return self.filter_authorized_menu_items(list(MenuItem), user=user)

    def get_extra_menu_items(self, *, user: T) -> list[ExtraMenuItem]:
        """
        Provide additional links to be added to the menu.

        :param user: the user
        """
        return []

    @staticmethod
    def get_db_manager() -> str | None:
        """
        Specify the DB manager path needed to run the auth manager.

        This is optional and not all auth managers require a DB manager.
        """
        return None

    @classmethod
    @cache
    def _get_token_signer(
        cls,
        expiration_time_in_seconds: int = conf.getint("api_auth", "jwt_expiration_time"),
    ) -> JWTGenerator:
        """
        Return the signer used to sign JWT token.

        :meta private:

        :param expiration_time_in_seconds: expiration time in seconds of the token
        """
        return JWTGenerator(
            **get_signing_args(),
            valid_for=expiration_time_in_seconds,
            audience=conf.get("api", "jwt_audience", fallback="apache-airflow"),
        )

    @classmethod
    @cache
    def _get_token_validator(cls) -> JWTValidator:
        """
        Return the signer used to sign JWT token.

        :meta private:
        """
        return JWTValidator(
            **get_sig_validation_args(),
            leeway=conf.getint("api_auth", "jwt_leeway"),
            audience=conf.get("api_auth", "jwt_audience", fallback="apache-airflow"),
        )
