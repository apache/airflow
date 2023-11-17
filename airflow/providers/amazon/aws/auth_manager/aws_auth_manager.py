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

from functools import cached_property
from typing import TYPE_CHECKING

from flask import session, url_for

from airflow.configuration import conf
from airflow.exceptions import AirflowOptionalProviderFeatureException
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
        return self.is_logged_in()

    def is_authorized_cluster_activity(self, *, method: ResourceMethod, user: BaseUser | None = None) -> bool:
        return self.is_logged_in()

    def is_authorized_connection(
        self,
        *,
        method: ResourceMethod,
        details: ConnectionDetails | None = None,
        user: BaseUser | None = None,
    ) -> bool:
        return self.is_logged_in()

    def is_authorized_dag(
        self,
        *,
        method: ResourceMethod,
        access_entity: DagAccessEntity | None = None,
        details: DagDetails | None = None,
        user: BaseUser | None = None,
    ) -> bool:
        return self.is_logged_in()

    def is_authorized_dataset(
        self, *, method: ResourceMethod, details: DatasetDetails | None = None, user: BaseUser | None = None
    ) -> bool:
        return self.is_logged_in()

    def is_authorized_pool(
        self, *, method: ResourceMethod, details: PoolDetails | None = None, user: BaseUser | None = None
    ) -> bool:
        return self.is_logged_in()

    def is_authorized_variable(
        self, *, method: ResourceMethod, details: VariableDetails | None = None, user: BaseUser | None = None
    ) -> bool:
        return self.is_logged_in()

    def is_authorized_view(
        self,
        *,
        access_view: AccessView,
        user: BaseUser | None = None,
    ) -> bool:
        return self.is_logged_in()

    def get_url_login(self, **kwargs) -> str:
        return url_for("AwsAuthManagerAuthenticationViews.login")

    def get_url_logout(self) -> str:
        return url_for("AwsAuthManagerAuthenticationViews.logout")

    @cached_property
    def security_manager(self) -> AwsSecurityManagerOverride:
        return AwsSecurityManagerOverride(self.appbuilder)
