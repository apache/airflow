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

import json
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Sequence, TypedDict

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.auth_manager.avp.entities import (
    AvpEntities,
    get_action_id,
    get_entity_type,
)
from airflow.providers.amazon.aws.auth_manager.constants import (
    CONF_AVP_POLICY_STORE_ID_KEY,
    CONF_CONN_ID_KEY,
    CONF_REGION_NAME_KEY,
    CONF_SECTION_NAME,
)
from airflow.providers.amazon.aws.hooks.verified_permissions import (
    VerifiedPermissionsHook,
)
from airflow.utils.helpers import prune_dict
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from airflow.auth.managers.base_auth_manager import ResourceMethod
    from airflow.providers.amazon.aws.auth_manager.user import AwsAuthManagerUser


# Amazon Verified Permissions allows only up to 30 requests per batch_is_authorized call. See
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/verifiedpermissions/client/batch_is_authorized.html
NB_REQUESTS_PER_BATCH = 30


class IsAuthorizedRequest(TypedDict, total=False):
    """Represent the parameters of ``is_authorized`` method in AVP facade."""

    method: ResourceMethod
    entity_type: AvpEntities
    entity_id: str | None
    context: dict | None


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
        return VerifiedPermissionsHook(
            aws_conn_id=aws_conn_id, region_name=region_name
        ).conn

    @cached_property
    def avp_policy_store_id(self):
        """Get the Amazon Verified Permission policy store ID from config."""
        return conf.get_mandatory_value(CONF_SECTION_NAME, CONF_AVP_POLICY_STORE_ID_KEY)

    def is_authorized(
        self,
        *,
        method: ResourceMethod | str,
        entity_type: AvpEntities,
        user: AwsAuthManagerUser | None,
        entity_id: str | None = None,
        context: dict | None = None,
    ) -> bool:
        """
        Make an authorization decision against Amazon Verified Permissions.

        Check whether the user has permissions to access given resource.

        :param method: the method to perform.
            The method can also be a string if the action has been defined in a plugin.
            In that case, the action can be anything (e.g. can_do).
            See https://github.com/apache/airflow/issues/39144
        :param entity_type: the entity type the user accesses
        :param user: the user
        :param entity_id: the entity ID the user accesses. If not provided, all entities of the type will be
            considered.
        :param context: optional additional context to pass to Amazon Verified Permissions.
        """
        if user is None:
            return False

        entity_list = self._get_user_group_entities(user)

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
                "principal": {
                    "entityType": get_entity_type(AvpEntities.USER),
                    "entityId": user.get_id(),
                },
                "action": {
                    "actionType": get_entity_type(AvpEntities.ACTION),
                    "actionId": get_action_id(entity_type, method),
                },
                "resource": {
                    "entityType": get_entity_type(entity_type),
                    "entityId": entity_id or "*",
                },
                "entities": {"entityList": entity_list},
                "context": self._build_context(context),
            }
        )

        resp = self.avp_client.is_authorized(**request_params)

        self.log.debug("Authorization response: %s", resp)

        if len(resp.get("errors", [])) > 0:
            self.log.error(
                "Error occurred while making an authorization decision. Errors: %s",
                resp["errors"],
            )
            raise AirflowException(
                "Error occurred while making an authorization decision."
            )

        return resp["decision"] == "ALLOW"

    def get_batch_is_authorized_results(
        self,
        *,
        requests: Sequence[IsAuthorizedRequest],
        user: AwsAuthManagerUser,
    ) -> list[dict]:
        """
        Make a batch authorization decision against Amazon Verified Permissions.

        Return a list of results for each request.

        :param requests: the list of requests containing the method, the entity_type and the entity ID
        :param user: the user
        """
        entity_list = self._get_user_group_entities(user)

        self.log.debug(
            "Making batch authorization request for user=%s, requests=%s",
            user.get_id(),
            requests,
        )

        avp_requests = [
            self._build_is_authorized_request_payload(request, user)
            for request in requests
        ]
        avp_requests_chunks = [
            avp_requests[i : i + NB_REQUESTS_PER_BATCH]
            for i in range(0, len(avp_requests), NB_REQUESTS_PER_BATCH)
        ]

        results = []
        for avp_requests in avp_requests_chunks:
            resp = self.avp_client.batch_is_authorized(
                policyStoreId=self.avp_policy_store_id,
                requests=avp_requests,
                entities={"entityList": entity_list},
            )

            self.log.debug("Authorization response: %s", resp)

            has_errors = any(
                len(result.get("errors", [])) > 0 for result in resp["results"]
            )

            if has_errors:
                self.log.error(
                    "Error occurred while making a batch authorization decision. Result: %s",
                    resp["results"],
                )
                raise AirflowException(
                    "Error occurred while making a batch authorization decision."
                )

            results.extend(resp["results"])

        return results

    def batch_is_authorized(
        self,
        *,
        requests: Sequence[IsAuthorizedRequest],
        user: AwsAuthManagerUser | None,
    ) -> bool:
        """
        Make a batch authorization decision against Amazon Verified Permissions.

        Check whether the user has permissions to access all resources.

        :param requests: the list of requests containing the method, the entity_type and the entity ID
        :param user: the user
        """
        if user is None:
            return False
        results = self.get_batch_is_authorized_results(requests=requests, user=user)
        return all(result["decision"] == "ALLOW" for result in results)

    def get_batch_is_authorized_single_result(
        self,
        *,
        batch_is_authorized_results: list[dict],
        request: IsAuthorizedRequest,
        user: AwsAuthManagerUser,
    ) -> dict:
        """
        Get a specific authorization result from the output of ``get_batch_is_authorized_results``.

        :param batch_is_authorized_results: the response from the ``batch_is_authorized`` API
        :param request: the request information. Used to find the result in the response.
        :param user: the user
        """
        request_payload = self._build_is_authorized_request_payload(request, user)

        for result in batch_is_authorized_results:
            if result["request"] == request_payload:
                return result

        self.log.error(
            "Could not find the authorization result for request %s in results %s.",
            request_payload,
            batch_is_authorized_results,
        )
        raise AirflowException("Could not find the authorization result.")

    def is_policy_store_schema_up_to_date(self) -> bool:
        """Return whether the policy store schema equals the latest version of the schema."""
        resp = self.avp_client.get_schema(
            policyStoreId=self.avp_policy_store_id,
        )
        policy_store_schema = json.loads(resp["schema"])

        schema_path = Path(__file__).parents[0] / "schema.json"
        with open(schema_path) as schema_file:
            latest_schema = json.load(schema_file)

        return policy_store_schema == latest_schema

    @staticmethod
    def _get_user_group_entities(user: AwsAuthManagerUser) -> list[dict]:
        user_entity = {
            "identifier": {
                "entityType": get_entity_type(AvpEntities.USER),
                "entityId": user.get_id(),
            },
            "parents": [
                {"entityType": get_entity_type(AvpEntities.GROUP), "entityId": group}
                for group in user.get_groups()
            ],
        }
        group_entities = [
            {
                "identifier": {
                    "entityType": get_entity_type(AvpEntities.GROUP),
                    "entityId": group,
                }
            }
            for group in user.get_groups()
        ]
        return [user_entity, *group_entities]

    @staticmethod
    def _build_context(context: dict | None) -> dict | None:
        if context is None or len(context) == 0:
            return None

        return {
            "contextMap": context,
        }

    def _build_is_authorized_request_payload(
        self, request: IsAuthorizedRequest, user: AwsAuthManagerUser
    ):
        """
        Build a payload of an individual authorization request that could be sent through the ``batch_is_authorized`` API.

        :param request: the request information
        :param user: the user
        """
        return prune_dict(
            {
                "principal": {
                    "entityType": get_entity_type(AvpEntities.USER),
                    "entityId": user.get_id(),
                },
                "action": {
                    "actionType": get_entity_type(AvpEntities.ACTION),
                    "actionId": get_action_id(request["entity_type"], request["method"]),
                },
                "resource": {
                    "entityType": get_entity_type(request["entity_type"]),
                    "entityId": request.get("entity_id", "*"),
                },
                "context": self._build_context(request.get("context")),
            }
        )
