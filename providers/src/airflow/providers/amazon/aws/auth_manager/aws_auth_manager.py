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

import argparse
import warnings
from collections import defaultdict
from functools import cached_property
from typing import TYPE_CHECKING, Container, Sequence, cast

from flask import session, url_for

from airflow.cli.cli_config import CLICommand, DefaultHelpParser, GroupCommand
from airflow.exceptions import AirflowOptionalProviderFeatureException, AirflowProviderDeprecationWarning
from airflow.providers.amazon.aws.auth_manager.avp.entities import AvpEntities
from airflow.providers.amazon.aws.auth_manager.avp.facade import (
    AwsAuthManagerAmazonVerifiedPermissionsFacade,
    IsAuthorizedRequest,
)
from airflow.providers.amazon.aws.auth_manager.cli.definition import (
    AWS_AUTH_MANAGER_COMMANDS,
)
from airflow.providers.amazon.aws.auth_manager.security_manager.aws_security_manager_override import (
    AwsSecurityManagerOverride,
)
from airflow.providers.amazon.aws.auth_manager.views.auth import AwsAuthManagerAuthenticationViews

try:
    from airflow.auth.managers.base_auth_manager import BaseAuthManager, ResourceMethod
    from airflow.auth.managers.models.resource_details import (
        AccessView,
        ConnectionDetails,
        DagAccessEntity,
        DagDetails,
        PoolDetails,
        VariableDetails,
    )
except ImportError:
    raise AirflowOptionalProviderFeatureException(
        "Failed to import BaseUser. This feature is only available in Airflow versions >= 2.8.0"
    )

if TYPE_CHECKING:
    from flask_appbuilder.menu import MenuItem

    from airflow.auth.managers.models.base_user import BaseUser
    from airflow.auth.managers.models.batch_apis import (
        IsAuthorizedConnectionRequest,
        IsAuthorizedDagRequest,
        IsAuthorizedPoolRequest,
        IsAuthorizedVariableRequest,
    )
    from airflow.auth.managers.models.resource_details import AssetDetails, ConfigurationDetails
    from airflow.providers.amazon.aws.auth_manager.user import AwsAuthManagerUser
    from airflow.www.extensions.init_appbuilder import AirflowAppBuilder


class AwsAuthManager(BaseAuthManager):
    """
    AWS auth manager.

    Leverages AWS services such as Amazon Identity Center and Amazon Verified Permissions to perform
    authentication and authorization in Airflow.

    :param appbuilder: the flask app builder
    """

    def __init__(self, appbuilder: AirflowAppBuilder) -> None:
        from packaging.version import Version

        from airflow.version import version

        # TODO: remove this if block when min_airflow_version is set to higher than 2.9.0
        if Version(version) < Version("2.9"):
            raise AirflowOptionalProviderFeatureException(
                "``AwsAuthManager`` is compatible with Airflow versions >= 2.9."
            )

        super().__init__(appbuilder)
        self._check_avp_schema_version()

    @cached_property
    def avp_facade(self):
        return AwsAuthManagerAmazonVerifiedPermissionsFacade()

    def get_user(self) -> AwsAuthManagerUser | None:
        return session["aws_user"] if self.is_logged_in() else None

    def is_logged_in(self) -> bool:
        return "aws_user" in session

    def is_authorized_configuration(
        self,
        *,
        method: ResourceMethod,
        details: ConfigurationDetails | None = None,
        user: BaseUser | None = None,
    ) -> bool:
        config_section = details.section if details else None
        return self.avp_facade.is_authorized(
            method=method,
            entity_type=AvpEntities.CONFIGURATION,
            user=user or self.get_user(),
            entity_id=config_section,
        )

    def is_authorized_connection(
        self,
        *,
        method: ResourceMethod,
        details: ConnectionDetails | None = None,
        user: BaseUser | None = None,
    ) -> bool:
        connection_id = details.conn_id if details else None
        return self.avp_facade.is_authorized(
            method=method,
            entity_type=AvpEntities.CONNECTION,
            user=user or self.get_user(),
            entity_id=connection_id,
        )

    def is_authorized_dag(
        self,
        *,
        method: ResourceMethod,
        access_entity: DagAccessEntity | None = None,
        details: DagDetails | None = None,
        user: BaseUser | None = None,
    ) -> bool:
        dag_id = details.id if details else None
        context = (
            None
            if access_entity is None
            else {
                "dag_entity": {
                    "string": access_entity.value,
                },
            }
        )
        return self.avp_facade.is_authorized(
            method=method,
            entity_type=AvpEntities.DAG,
            user=user or self.get_user(),
            entity_id=dag_id,
            context=context,
        )

    def is_authorized_asset(
        self, *, method: ResourceMethod, details: AssetDetails | None = None, user: BaseUser | None = None
    ) -> bool:
        asset_uri = details.uri if details else None
        return self.avp_facade.is_authorized(
            method=method, entity_type=AvpEntities.ASSET, user=user or self.get_user(), entity_id=asset_uri
        )

    def is_authorized_dataset(
        self, *, method: ResourceMethod, details: AssetDetails | None = None, user: BaseUser | None = None
    ) -> bool:
        warnings.warn(
            "is_authorized_dataset will be renamed as is_authorized_asset in Airflow 3 and will be removed when the minimum Airflow version is set to 3.0 for the amazon provider",
            AirflowProviderDeprecationWarning,
            stacklevel=2,
        )
        return self.is_authorized_asset(method=method, user=user)

    def is_authorized_pool(
        self, *, method: ResourceMethod, details: PoolDetails | None = None, user: BaseUser | None = None
    ) -> bool:
        pool_name = details.name if details else None
        return self.avp_facade.is_authorized(
            method=method,
            entity_type=AvpEntities.POOL,
            user=user or self.get_user(),
            entity_id=pool_name,
        )

    def is_authorized_variable(
        self, *, method: ResourceMethod, details: VariableDetails | None = None, user: BaseUser | None = None
    ) -> bool:
        variable_key = details.key if details else None
        return self.avp_facade.is_authorized(
            method=method,
            entity_type=AvpEntities.VARIABLE,
            user=user or self.get_user(),
            entity_id=variable_key,
        )

    def is_authorized_view(
        self,
        *,
        access_view: AccessView,
        user: BaseUser | None = None,
    ) -> bool:
        return self.avp_facade.is_authorized(
            method="GET",
            entity_type=AvpEntities.VIEW,
            user=user or self.get_user(),
            entity_id=access_view.value,
        )

    def is_authorized_custom_view(
        self, *, method: ResourceMethod | str, resource_name: str, user: BaseUser | None = None
    ):
        return self.avp_facade.is_authorized(
            method=method,
            entity_type=AvpEntities.CUSTOM,
            user=user or self.get_user(),
            entity_id=resource_name,
        )

    def batch_is_authorized_connection(
        self,
        requests: Sequence[IsAuthorizedConnectionRequest],
    ) -> bool:
        """
        Batch version of ``is_authorized_connection``.

        :param requests: a list of requests containing the parameters for ``is_authorized_connection``
        """
        facade_requests: Sequence[IsAuthorizedRequest] = [
            {
                "method": request["method"],
                "entity_type": AvpEntities.CONNECTION,
                "entity_id": cast(ConnectionDetails, request["details"]).conn_id
                if request.get("details")
                else None,
            }
            for request in requests
        ]
        return self.avp_facade.batch_is_authorized(requests=facade_requests, user=self.get_user())

    def batch_is_authorized_dag(
        self,
        requests: Sequence[IsAuthorizedDagRequest],
    ) -> bool:
        """
        Batch version of ``is_authorized_dag``.

        :param requests: a list of requests containing the parameters for ``is_authorized_dag``
        """
        facade_requests: Sequence[IsAuthorizedRequest] = [
            {
                "method": request["method"],
                "entity_type": AvpEntities.DAG,
                "entity_id": cast(DagDetails, request["details"]).id if request.get("details") else None,
                "context": {
                    "dag_entity": {
                        "string": cast(DagAccessEntity, request["access_entity"]).value,
                    },
                }
                if request.get("access_entity")
                else None,
            }
            for request in requests
        ]
        return self.avp_facade.batch_is_authorized(requests=facade_requests, user=self.get_user())

    def batch_is_authorized_pool(
        self,
        requests: Sequence[IsAuthorizedPoolRequest],
    ) -> bool:
        """
        Batch version of ``is_authorized_pool``.

        :param requests: a list of requests containing the parameters for ``is_authorized_pool``
        """
        facade_requests: Sequence[IsAuthorizedRequest] = [
            {
                "method": request["method"],
                "entity_type": AvpEntities.POOL,
                "entity_id": cast(PoolDetails, request["details"]).name if request.get("details") else None,
            }
            for request in requests
        ]
        return self.avp_facade.batch_is_authorized(requests=facade_requests, user=self.get_user())

    def batch_is_authorized_variable(
        self,
        requests: Sequence[IsAuthorizedVariableRequest],
    ) -> bool:
        """
        Batch version of ``is_authorized_variable``.

        :param requests: a list of requests containing the parameters for ``is_authorized_variable``
        """
        facade_requests: Sequence[IsAuthorizedRequest] = [
            {
                "method": request["method"],
                "entity_type": AvpEntities.VARIABLE,
                "entity_id": cast(VariableDetails, request["details"]).key
                if request.get("details")
                else None,
            }
            for request in requests
        ]
        return self.avp_facade.batch_is_authorized(requests=facade_requests, user=self.get_user())

    def filter_permitted_dag_ids(
        self,
        *,
        dag_ids: set[str],
        methods: Container[ResourceMethod] | None = None,
        user=None,
    ):
        """
        Filter readable or writable DAGs for user.

        :param dag_ids: the list of DAG ids
        :param methods: whether filter readable or writable
        :param user: the current user
        """
        if not methods:
            methods = ["PUT", "GET"]

        if not user:
            user = self.get_user()

        requests: dict[str, dict[ResourceMethod, IsAuthorizedRequest]] = defaultdict(dict)
        requests_list: list[IsAuthorizedRequest] = []
        for dag_id in dag_ids:
            for method in ["GET", "PUT"]:
                if method in methods:
                    request: IsAuthorizedRequest = {
                        "method": cast(ResourceMethod, method),
                        "entity_type": AvpEntities.DAG,
                        "entity_id": dag_id,
                    }
                    requests[dag_id][cast(ResourceMethod, method)] = request
                    requests_list.append(request)

        batch_is_authorized_results = self.avp_facade.get_batch_is_authorized_results(
            requests=requests_list, user=user
        )

        def _has_access_to_dag(request: IsAuthorizedRequest):
            result = self.avp_facade.get_batch_is_authorized_single_result(
                batch_is_authorized_results=batch_is_authorized_results, request=request, user=user
            )
            return result["decision"] == "ALLOW"

        return {
            dag_id
            for dag_id in dag_ids
            if (
                "GET" in methods
                and _has_access_to_dag(requests[dag_id]["GET"])
                or "PUT" in methods
                and _has_access_to_dag(requests[dag_id]["PUT"])
            )
        }

    def filter_permitted_menu_items(self, menu_items: list[MenuItem]) -> list[MenuItem]:
        """
        Filter menu items based on user permissions.

        :param menu_items: list of all menu items
        """
        user = self.get_user()
        if not user:
            return []

        requests: dict[str, IsAuthorizedRequest] = {}
        for menu_item in menu_items:
            if menu_item.childs:
                for child in menu_item.childs:
                    requests[child.name] = self._get_menu_item_request(child.name)
            else:
                requests[menu_item.name] = self._get_menu_item_request(menu_item.name)

        batch_is_authorized_results = self.avp_facade.get_batch_is_authorized_results(
            requests=list(requests.values()), user=user
        )

        def _has_access_to_menu_item(request: IsAuthorizedRequest):
            result = self.avp_facade.get_batch_is_authorized_single_result(
                batch_is_authorized_results=batch_is_authorized_results, request=request, user=user
            )
            return result["decision"] == "ALLOW"

        accessible_items = []
        for menu_item in menu_items:
            if menu_item.childs:
                accessible_children = []
                for child in menu_item.childs:
                    if _has_access_to_menu_item(requests[child.name]):
                        accessible_children.append(child)
                menu_item.childs = accessible_children

                # Display the menu if the user has access to at least one sub item
                if len(accessible_children) > 0:
                    accessible_items.append(menu_item)
            elif _has_access_to_menu_item(requests[menu_item.name]):
                accessible_items.append(menu_item)

        return accessible_items

    def get_url_login(self, **kwargs) -> str:
        return url_for("AwsAuthManagerAuthenticationViews.login")

    def get_url_logout(self) -> str:
        return url_for("AwsAuthManagerAuthenticationViews.logout")

    @cached_property
    def security_manager(self) -> AwsSecurityManagerOverride:
        return AwsSecurityManagerOverride(self.appbuilder)

    @staticmethod
    def get_cli_commands() -> list[CLICommand]:
        """Vends CLI commands to be included in Airflow CLI."""
        return [
            GroupCommand(
                name="aws-auth-manager",
                help="Manage resources used by AWS auth manager",
                subcommands=AWS_AUTH_MANAGER_COMMANDS,
            ),
        ]

    def register_views(self) -> None:
        self.appbuilder.add_view_no_menu(AwsAuthManagerAuthenticationViews())

    @staticmethod
    def _get_menu_item_request(resource_name: str) -> IsAuthorizedRequest:
        return {
            "method": "MENU",
            "entity_type": AvpEntities.MENU,
            "entity_id": resource_name,
        }

    def _check_avp_schema_version(self):
        if not self.avp_facade.is_policy_store_schema_up_to_date():
            self.log.warning(
                "The Amazon Verified Permissions policy store schema is different from the latest version "
                "(https://github.com/apache/airflow/blob/main/airflow/providers/amazon/aws/auth_manager/avp/schema.json). "
                "Please update it to its latest version. "
                "See doc: https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/auth-manager/setup/amazon-verified-permissions.html#update-the-policy-store-schema."
            )


def get_parser() -> argparse.ArgumentParser:
    """Generate documentation; used by Sphinx argparse."""
    from airflow.cli.cli_parser import AirflowHelpFormatter, _add_command

    parser = DefaultHelpParser(prog="airflow", formatter_class=AirflowHelpFormatter)
    subparsers = parser.add_subparsers(dest="subcommand", metavar="GROUP_OR_COMMAND")
    for group_command in AwsAuthManager.get_cli_commands():
        _add_command(subparsers, group_command)
    return parser
