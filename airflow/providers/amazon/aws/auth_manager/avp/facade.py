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

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.auth_manager.avp.entities import AvpEntities, get_action_id, get_entity_type
from airflow.providers.amazon.aws.auth_manager.constants import (
    CONF_AVP_POLICY_STORE_ID_KEY,
    CONF_CONN_ID_KEY,
    CONF_REGION_NAME_KEY,
    CONF_SECTION_NAME,
)
from airflow.providers.amazon.aws.hooks.verified_permissions import VerifiedPermissionsHook
from airflow.utils.helpers import prune_dict
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from airflow.auth.managers.base_auth_manager import ResourceMethod
    from airflow.providers.amazon.aws.auth_manager.user import AwsAuthManagerUser


class AwsAuthManagerAmazonVerifiedPermissionsFacade(LoggingMixin):
    """
    Facade for Amazon Verified Permissions.

    Used as an intermediate layer between AWS auth manager and Amazon Verified Permissions.
    """

    @cached_property
    def avp_client(self):
        """Build Amazon Verified Permissions client."""
        aws_conn_id = conf.get(CONF_SECTION_NAME, CONF_CONN_ID_KEY)
        region_name = conf.get(CONF_SECTION_NAME, CONF_REGION_NAME_KEY)
        return VerifiedPermissionsHook(aws_conn_id=aws_conn_id, region_name=region_name).conn

    @cached_property
    def avp_policy_store_id(self):
        """Get the Amazon Verified Permission policy store ID from config."""
        return conf.get_mandatory_value(CONF_SECTION_NAME, CONF_AVP_POLICY_STORE_ID_KEY)

    def is_authorized(
        self,
        *,
        method: ResourceMethod,
        entity_type: AvpEntities,
        user: AwsAuthManagerUser | None,
        entity_id: str | None = None,
        context: dict | None = None,
    ) -> bool:
        """
        Make an authorization decision against Amazon Verified Permissions.

        Check whether the user has permissions to access given resource.

        :param method: the method to perform
        :param entity_type: the entity type the user accesses
        :param user: the user
        :param entity_id: the entity ID the user accesses. If not provided, all entities of the type will be
            considered.
        :param context: optional additional context to pass to Amazon Verified Permissions.
        """
        if user is None:
            return False

        entity_list = self._get_user_role_entities(user)

        self.log.debug(
            "Making authorization request for user=%s, method=%s, entity_type=%s, entity_id=%s",
            user.get_id(),
            method,
            entity_type,
            entity_id,
        )

        request_params = prune_dict(
            {
                "policyStoreId": self.avp_policy_store_id,
                "principal": {"entityType": get_entity_type(AvpEntities.USER), "entityId": user.get_id()},
                "action": {
                    "actionType": get_entity_type(AvpEntities.ACTION),
                    "actionId": get_action_id(entity_type, method),
                },
                "resource": {"entityType": get_entity_type(entity_type), "entityId": entity_id or "*"},
                "entities": {"entityList": entity_list},
                "context": self._build_context(context),
            }
        )

        resp = self.avp_client.is_authorized(**request_params)

        self.log.debug("Authorization response: %s", resp)

        if len(resp.get("errors", [])) > 0:
            self.log.error(
                "Error occurred while making an authorization decision. Errors: %s", resp["errors"]
            )
            raise AirflowException("Error occurred while making an authorization decision.")

        return resp["decision"] == "ALLOW"

    @staticmethod
    def _get_user_role_entities(user: AwsAuthManagerUser) -> list[dict]:
        user_entity = {
            "identifier": {"entityType": get_entity_type(AvpEntities.USER), "entityId": user.get_id()},
            "parents": [
                {"entityType": get_entity_type(AvpEntities.ROLE), "entityId": group}
                for group in user.get_groups()
            ],
        }
        role_entities = [
            {"identifier": {"entityType": get_entity_type(AvpEntities.ROLE), "entityId": group}}
            for group in user.get_groups()
        ]
        return [user_entity, *role_entities]

    @staticmethod
    def _build_context(context: dict | None) -> dict | None:
        if context is None or len(context) == 0:
            return None

        return {
            "contextMap": context,
        }
