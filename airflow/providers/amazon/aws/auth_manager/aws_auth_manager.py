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
from functools import cached_property
from typing import TYPE_CHECKING, Sequence, cast

from flask import session, url_for

from airflow.cli.cli_config import CLICommand, DefaultHelpParser, GroupCommand
from airflow.configuration import conf
from airflow.exceptions import AirflowException, AirflowOptionalProviderFeatureException
from airflow.providers.amazon.aws.auth_manager.avp.entities import AvpEntities
from airflow.providers.amazon.aws.auth_manager.avp.facade import (
    AwsAuthManagerAmazonVerifiedPermissionsFacade,
    IsAuthorizedRequest,
)
from airflow.providers.amazon.aws.auth_manager.cli.definition import (
    AWS_AUTH_MANAGER_COMMANDS,
)
from airflow.providers.amazon.aws.auth_manager.constants import (
    CONF_ENABLE_KEY,
    CONF_SECTION_NAME,
)
from airflow.providers.amazon.aws.auth_manager.security_manager.aws_security_manager_override import (
    AwsSecurityManagerOverride,
)
from airflow.security.permissions import (
    RESOURCE_AUDIT_LOG,
    RESOURCE_CLUSTER_ACTIVITY,
    RESOURCE_CONFIG,
    RESOURCE_CONNECTION,
    RESOURCE_DAG,
    RESOURCE_DAG_CODE,
    RESOURCE_DAG_DEPENDENCIES,
    RESOURCE_DAG_RUN,
    RESOURCE_DATASET,
    RESOURCE_DOCS,
    RESOURCE_JOB,
    RESOURCE_PLUGIN,
    RESOURCE_POOL,
    RESOURCE_PROVIDER,
    RESOURCE_SLA_MISS,
    RESOURCE_TASK_INSTANCE,
    RESOURCE_TASK_RESCHEDULE,
    RESOURCE_TRIGGER,
    RESOURCE_VARIABLE,
    RESOURCE_XCOM,
)

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
    from airflow.auth.managers.models.resource_details import (
        ConfigurationDetails,
        DatasetDetails,
    )
    from airflow.providers.amazon.aws.auth_manager.user import AwsAuthManagerUser
    from airflow.www.extensions.init_appbuilder import AirflowAppBuilder


_MENU_ITEM_REQUESTS: dict[str, IsAuthorizedRequest] = {
    RESOURCE_AUDIT_LOG: {
        "method": "GET",
        "entity_type": AvpEntities.DAG,
        "context": {
            "dag_entity": {
                "string": DagAccessEntity.AUDIT_LOG.value,
            },
        },
    },
    RESOURCE_CLUSTER_ACTIVITY: {
        "method": "GET",
        "entity_type": AvpEntities.VIEW,
        "entity_id": AccessView.CLUSTER_ACTIVITY.value,
    },
    RESOURCE_CONFIG: {
        "method": "GET",
        "entity_type": AvpEntities.CONFIGURATION,
    },
    RESOURCE_CONNECTION: {
        "method": "GET",
        "entity_type": AvpEntities.CONNECTION,
    },
    RESOURCE_DAG: {
        "method": "GET",
        "entity_type": AvpEntities.DAG,
    },
    RESOURCE_DAG_CODE: {
        "method": "GET",
        "entity_type": AvpEntities.DAG,
        "context": {
            "dag_entity": {
                "string": DagAccessEntity.CODE.value,
            },
        },
    },
    RESOURCE_DAG_DEPENDENCIES: {
        "method": "GET",
        "entity_type": AvpEntities.DAG,
        "context": {
            "dag_entity": {
                "string": DagAccessEntity.DEPENDENCIES.value,
            },
        },
    },
    RESOURCE_DAG_RUN: {
        "method": "GET",
        "entity_type": AvpEntities.DAG,
        "context": {
            "dag_entity": {
                "string": DagAccessEntity.RUN.value,
            },
        },
    },
    RESOURCE_DATASET: {
        "method": "GET",
        "entity_type": AvpEntities.DATASET,
    },
    RESOURCE_DOCS: {
        "method": "GET",
        "entity_type": AvpEntities.VIEW,
        "entity_id": AccessView.DOCS.value,
    },
    RESOURCE_PLUGIN: {
        "method": "GET",
        "entity_type": AvpEntities.VIEW,
        "entity_id": AccessView.PLUGINS.value,
    },
    RESOURCE_JOB: {
        "method": "GET",
        "entity_type": AvpEntities.VIEW,
        "entity_id": AccessView.JOBS.value,
    },
    RESOURCE_POOL: {
        "method": "GET",
        "entity_type": AvpEntities.POOL,
    },
    RESOURCE_PROVIDER: {
        "method": "GET",
        "entity_type": AvpEntities.VIEW,
        "entity_id": AccessView.PROVIDERS.value,
    },
    RESOURCE_SLA_MISS: {
        "method": "GET",
        "entity_type": AvpEntities.DAG,
        "context": {
            "dag_entity": {
                "string": DagAccessEntity.SLA_MISS.value,
            },
        },
    },
    RESOURCE_TASK_INSTANCE: {
        "method": "GET",
        "entity_type": AvpEntities.DAG,
        "context": {
            "dag_entity": {
                "string": DagAccessEntity.TASK_INSTANCE.value,
            },
        },
    },
    RESOURCE_TASK_RESCHEDULE: {
        "method": "GET",
        "entity_type": AvpEntities.DAG,
        "context": {
            "dag_entity": {
                "string": DagAccessEntity.TASK_RESCHEDULE.value,
            },
        },
    },
    RESOURCE_TRIGGER: {
        "method": "GET",
        "entity_type": AvpEntities.VIEW,
        "entity_id": AccessView.TRIGGERS.value,
    },
    RESOURCE_VARIABLE: {
        "method": "GET",
        "entity_type": AvpEntities.VARIABLE,
    },
    RESOURCE_XCOM: {
        "method": "GET",
        "entity_type": AvpEntities.DAG,
        "context": {
            "dag_entity": {
                "string": DagAccessEntity.XCOM.value,
            },
        },
    },
}


class AwsAuthManager(BaseAuthManager):
    """
    AWS auth manager.

    Leverages AWS services such as Amazon Identity Center and Amazon Verified Permissions to perform
    authentication and authorization in Airflow.

    :param appbuilder: the flask app builder
    """

    def __init__(self, appbuilder: AirflowAppBuilder) -> None:
        super().__init__(appbuilder)
        enable = conf.getboolean(CONF_SECTION_NAME, CONF_ENABLE_KEY)
        if not enable:
            raise NotImplementedError(
                "The AWS auth manager is currently being built. It is not finalized. It is not intended to be used yet."
            )

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

    def is_authorized_dataset(
        self, *, method: ResourceMethod, details: DatasetDetails | None = None, user: BaseUser | None = None
    ) -> bool:
        dataset_uri = details.uri if details else None
        return self.avp_facade.is_authorized(
            method=method,
            entity_type=AvpEntities.DATASET,
            user=user or self.get_user(),
            entity_id=dataset_uri,
        )

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

        accessible_items = []
        for menu_item in menu_items:
            if menu_item.childs:
                accessible_children = []
                for child in menu_item.childs:
                    if self._has_access_to_menu_item(batch_is_authorized_results, requests[child.name], user):
                        accessible_children.append(child)
                menu_item.childs = accessible_children

                # Display the menu if the user has access to at least one sub item
                if len(accessible_children) > 0:
                    accessible_items.append(menu_item)
            elif self._has_access_to_menu_item(batch_is_authorized_results, requests[menu_item.name], user):
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

    @staticmethod
    def _get_menu_item_request(fab_resource_name: str) -> IsAuthorizedRequest:
        menu_item_request = _MENU_ITEM_REQUESTS.get(fab_resource_name)
        if menu_item_request:
            return menu_item_request
        else:
            raise AirflowException(f"Unknown resource name {fab_resource_name}")

    def _has_access_to_menu_item(
        self, batch_is_authorized_results: list[dict], request: IsAuthorizedRequest, user: AwsAuthManagerUser
    ):
        result = self.avp_facade.get_batch_is_authorized_single_result(
            batch_is_authorized_results=batch_is_authorized_results, request=request, user=user
        )
        return result["decision"] == "ALLOW"


def get_parser() -> argparse.ArgumentParser:
    """Generate documentation; used by Sphinx argparse."""
    from airflow.cli.cli_parser import AirflowHelpFormatter, _add_command

    parser = DefaultHelpParser(prog="airflow", formatter_class=AirflowHelpFormatter)
    subparsers = parser.add_subparsers(dest="subcommand", metavar="GROUP_OR_COMMAND")
    for group_command in AwsAuthManager.get_cli_commands():
        _add_command(subparsers, group_command)
    return parser
