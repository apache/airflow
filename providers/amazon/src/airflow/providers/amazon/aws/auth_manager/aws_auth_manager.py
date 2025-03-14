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
from collections import defaultdict
from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urljoin

from fastapi import FastAPI

from airflow.api_fastapi.app import AUTH_MANAGER_FASTAPI_APP_PREFIX
from airflow.api_fastapi.auth.managers.base_auth_manager import BaseAuthManager
from airflow.api_fastapi.auth.managers.models.resource_details import (
    AccessView,
    BackfillDetails,
    ConnectionDetails,
    DagAccessEntity,
    DagDetails,
    PoolDetails,
    VariableDetails,
)
from airflow.cli.cli_config import CLICommand, DefaultHelpParser, GroupCommand
from airflow.configuration import conf
from airflow.exceptions import AirflowOptionalProviderFeatureException
from airflow.providers.amazon.aws.auth_manager.avp.entities import AvpEntities
from airflow.providers.amazon.aws.auth_manager.avp.facade import (
    AwsAuthManagerAmazonVerifiedPermissionsFacade,
    IsAuthorizedRequest,
)
from airflow.providers.amazon.aws.auth_manager.cli.definition import (
    AWS_AUTH_MANAGER_COMMANDS,
)
from airflow.providers.amazon.aws.auth_manager.user import AwsAuthManagerUser
from airflow.providers.amazon.version_compat import AIRFLOW_V_3_0_PLUS

if TYPE_CHECKING:
    from airflow.api_fastapi.auth.managers.base_auth_manager import ResourceMethod
    from airflow.api_fastapi.auth.managers.models.batch_apis import (
        IsAuthorizedConnectionRequest,
        IsAuthorizedDagRequest,
        IsAuthorizedPoolRequest,
        IsAuthorizedVariableRequest,
    )
    from airflow.api_fastapi.auth.managers.models.resource_details import (
        AssetAliasDetails,
        AssetDetails,
        ConfigurationDetails,
    )
    from airflow.api_fastapi.common.types import MenuItem


class AwsAuthManager(BaseAuthManager[AwsAuthManagerUser]):
    """
    AWS auth manager.

    Leverages AWS services such as Amazon Identity Center and Amazon Verified Permissions to perform
    authentication and authorization in Airflow.
    """

    def __init__(self) -> None:
        if not AIRFLOW_V_3_0_PLUS:
            raise AirflowOptionalProviderFeatureException(
                "AWS auth manager is only compatible with Airflow versions >= 3.0.0"
            )

        super().__init__()
        self._check_avp_schema_version()

    @cached_property
    def avp_facade(self):
        return AwsAuthManagerAmazonVerifiedPermissionsFacade()

    @cached_property
    def apiserver_endpoint(self) -> str:
        return conf.get("api", "base_url")

    def deserialize_user(self, token: dict[str, Any]) -> AwsAuthManagerUser:
        return AwsAuthManagerUser(user_id=token.pop("sub"), **token)

    def serialize_user(self, user: AwsAuthManagerUser) -> dict[str, Any]:
        return {
            "sub": user.get_id(),
            "groups": user.get_groups(),
            "username": user.username,
            "email": user.email,
        }

    def is_authorized_configuration(
        self,
        *,
        method: ResourceMethod,
        user: AwsAuthManagerUser,
        details: ConfigurationDetails | None = None,
    ) -> bool:
        config_section = details.section if details else None
        return self.avp_facade.is_authorized(
            method=method,
            entity_type=AvpEntities.CONFIGURATION,
            user=user,
            entity_id=config_section,
        )

    def is_authorized_connection(
        self,
        *,
        method: ResourceMethod,
        user: AwsAuthManagerUser,
        details: ConnectionDetails | None = None,
    ) -> bool:
        connection_id = details.conn_id if details else None
        return self.avp_facade.is_authorized(
            method=method,
            entity_type=AvpEntities.CONNECTION,
            user=user,
            entity_id=connection_id,
        )

    def is_authorized_dag(
        self,
        *,
        method: ResourceMethod,
        user: AwsAuthManagerUser,
        access_entity: DagAccessEntity | None = None,
        details: DagDetails | None = None,
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
            user=user,
            entity_id=dag_id,
            context=context,
        )

    def is_authorized_backfill(
        self, *, method: ResourceMethod, user: AwsAuthManagerUser, details: BackfillDetails | None = None
    ) -> bool:
        backfill_id = details.id if details else None
        return self.avp_facade.is_authorized(
            method=method, entity_type=AvpEntities.BACKFILL, user=user, entity_id=backfill_id
        )

    def is_authorized_asset(
        self, *, method: ResourceMethod, user: AwsAuthManagerUser, details: AssetDetails | None = None
    ) -> bool:
        asset_id = details.id if details else None
        return self.avp_facade.is_authorized(
            method=method, entity_type=AvpEntities.ASSET, user=user, entity_id=asset_id
        )

    def is_authorized_asset_alias(
        self, *, method: ResourceMethod, user: AwsAuthManagerUser, details: AssetAliasDetails | None = None
    ) -> bool:
        asset_alias_id = details.id if details else None
        return self.avp_facade.is_authorized(
            method=method, entity_type=AvpEntities.ASSET_ALIAS, user=user, entity_id=asset_alias_id
        )

    def is_authorized_pool(
        self, *, method: ResourceMethod, user: AwsAuthManagerUser, details: PoolDetails | None = None
    ) -> bool:
        pool_name = details.name if details else None
        return self.avp_facade.is_authorized(
            method=method,
            entity_type=AvpEntities.POOL,
            user=user,
            entity_id=pool_name,
        )

    def is_authorized_variable(
        self, *, method: ResourceMethod, user: AwsAuthManagerUser, details: VariableDetails | None = None
    ) -> bool:
        variable_key = details.key if details else None
        return self.avp_facade.is_authorized(
            method=method,
            entity_type=AvpEntities.VARIABLE,
            user=user,
            entity_id=variable_key,
        )

    def is_authorized_view(
        self,
        *,
        access_view: AccessView,
        user: AwsAuthManagerUser,
    ) -> bool:
        return self.avp_facade.is_authorized(
            method="GET",
            entity_type=AvpEntities.VIEW,
            user=user,
            entity_id=access_view.value,
        )

    def is_authorized_custom_view(
        self, *, method: ResourceMethod | str, resource_name: str, user: AwsAuthManagerUser
    ) -> bool:
        return self.avp_facade.is_authorized(
            method=method,
            entity_type=AvpEntities.CUSTOM,
            user=user,
            entity_id=resource_name,
        )

    def filter_authorized_menu_items(
        self, menu_items: list[MenuItem], *, user: AwsAuthManagerUser
    ) -> list[MenuItem]:
        requests: dict[str, IsAuthorizedRequest] = {}
        for menu_item in menu_items:
            requests[menu_item.value] = self._get_menu_item_request(menu_item.value)

        batch_is_authorized_results = self.avp_facade.get_batch_is_authorized_results(
            requests=list(requests.values()), user=user
        )

        def _has_access_to_menu_item(request: IsAuthorizedRequest):
            result = self.avp_facade.get_batch_is_authorized_single_result(
                batch_is_authorized_results=batch_is_authorized_results, request=request, user=user
            )
            return result["decision"] == "ALLOW"

        return [menu_item for menu_item in menu_items if _has_access_to_menu_item(requests[menu_item.value])]

    def batch_is_authorized_connection(
        self,
        requests: Sequence[IsAuthorizedConnectionRequest],
        *,
        user: AwsAuthManagerUser,
    ) -> bool:
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
        return self.avp_facade.batch_is_authorized(requests=facade_requests, user=user)

    def batch_is_authorized_dag(
        self,
        requests: Sequence[IsAuthorizedDagRequest],
        *,
        user: AwsAuthManagerUser,
    ) -> bool:
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
        return self.avp_facade.batch_is_authorized(requests=facade_requests, user=user)

    def batch_is_authorized_pool(
        self,
        requests: Sequence[IsAuthorizedPoolRequest],
        *,
        user: AwsAuthManagerUser,
    ) -> bool:
        facade_requests: Sequence[IsAuthorizedRequest] = [
            {
                "method": request["method"],
                "entity_type": AvpEntities.POOL,
                "entity_id": cast(PoolDetails, request["details"]).name if request.get("details") else None,
            }
            for request in requests
        ]
        return self.avp_facade.batch_is_authorized(requests=facade_requests, user=user)

    def batch_is_authorized_variable(
        self,
        requests: Sequence[IsAuthorizedVariableRequest],
        *,
        user: AwsAuthManagerUser,
    ) -> bool:
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
        return self.avp_facade.batch_is_authorized(requests=facade_requests, user=user)

    def filter_permitted_dag_ids(
        self,
        *,
        dag_ids: set[str],
        user: AwsAuthManagerUser,
        method: ResourceMethod = "GET",
    ):
        requests: dict[str, dict[ResourceMethod, IsAuthorizedRequest]] = defaultdict(dict)
        requests_list: list[IsAuthorizedRequest] = []
        for dag_id in dag_ids:
            request: IsAuthorizedRequest = {
                "method": method,
                "entity_type": AvpEntities.DAG,
                "entity_id": dag_id,
            }
            requests[dag_id][method] = request
            requests_list.append(request)

        batch_is_authorized_results = self.avp_facade.get_batch_is_authorized_results(
            requests=requests_list, user=user
        )

        def _has_access_to_dag(request: IsAuthorizedRequest):
            result = self.avp_facade.get_batch_is_authorized_single_result(
                batch_is_authorized_results=batch_is_authorized_results, request=request, user=user
            )
            return result["decision"] == "ALLOW"

        return {dag_id for dag_id in dag_ids if _has_access_to_dag(requests[dag_id][method])}

    def get_url_login(self, **kwargs) -> str:
        return urljoin(self.apiserver_endpoint, f"{AUTH_MANAGER_FASTAPI_APP_PREFIX}/login")

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

    def get_fastapi_app(self) -> FastAPI | None:
        from airflow.providers.amazon.aws.auth_manager.router.login import login_router

        app = FastAPI(
            title="AWS auth manager sub application",
            description=(
                "This is the AWS auth manager fastapi sub application. This API is only available if the "
                "auth manager used in the Airflow environment is AWS auth manager. "
                "This sub application provides login routes."
            ),
        )
        app.include_router(login_router)

        return app

    @staticmethod
    def _get_menu_item_request(menu_item_text: str) -> IsAuthorizedRequest:
        return {
            "method": "MENU",
            "entity_type": AvpEntities.MENU,
            "entity_id": menu_item_text,
        }

    def _check_avp_schema_version(self):
        if not self.avp_facade.is_policy_store_schema_up_to_date():
            self.log.warning(
                "The Amazon Verified Permissions policy store schema is different from the latest version "
                "(https://github.com/apache/airflow/blob/main/providers/amazon/aws/src/airflow/providers/amazon/aws/auth_manager/avp/schema.json). "
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
