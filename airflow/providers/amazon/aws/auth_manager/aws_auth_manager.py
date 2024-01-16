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
from typing import TYPE_CHECKING

from flask import session, url_for

from airflow.cli.cli_config import CLICommand, DefaultHelpParser, GroupCommand
from airflow.configuration import conf
from airflow.exceptions import AirflowOptionalProviderFeatureException
from airflow.providers.amazon.aws.auth_manager.avp.entities import AvpEntities
from airflow.providers.amazon.aws.auth_manager.avp.facade import AwsAuthManagerAmazonVerifiedPermissionsFacade
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

try:
    from airflow.auth.managers.base_auth_manager import BaseAuthManager, ResourceMethod
except ImportError:
    raise AirflowOptionalProviderFeatureException(
        "Failed to import BaseUser. This feature is only available in Airflow versions >= 2.8.0"
    )

if TYPE_CHECKING:
    from airflow.auth.managers.models.base_user import BaseUser
    from airflow.auth.managers.models.resource_details import (
        AccessView,
        ConfigurationDetails,
        ConnectionDetails,
        DagAccessEntity,
        DagDetails,
        DatasetDetails,
        PoolDetails,
        VariableDetails,
    )
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


def get_parser() -> argparse.ArgumentParser:
    """Generate documentation; used by Sphinx argparse."""
    from airflow.cli.cli_parser import AirflowHelpFormatter, _add_command

    parser = DefaultHelpParser(prog="airflow", formatter_class=AirflowHelpFormatter)
    subparsers = parser.add_subparsers(dest="subcommand", metavar="GROUP_OR_COMMAND")
    for group_command in AwsAuthManager.get_cli_commands():
        _add_command(subparsers, group_command)
    return parser
