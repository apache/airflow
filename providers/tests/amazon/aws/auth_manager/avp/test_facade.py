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
from typing import TYPE_CHECKING
from unittest.mock import Mock

import pytest
from tests_common.test_utils.config import conf_vars

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.auth_manager.avp.entities import AvpEntities, get_action_id, get_entity_type
from airflow.providers.amazon.aws.auth_manager.avp.facade import AwsAuthManagerAmazonVerifiedPermissionsFacade
from airflow.providers.amazon.aws.auth_manager.user import AwsAuthManagerUser
from airflow.utils.helpers import prune_dict

if TYPE_CHECKING:
    from airflow.auth.managers.base_auth_manager import ResourceMethod

REGION_NAME = "us-east-1"
AVP_POLICY_STORE_ID = "store_id"

test_user = AwsAuthManagerUser(user_id="test_user", groups=["group1", "group2"])
test_user_no_group = AwsAuthManagerUser(user_id="test_user_no_group", groups=[])


@pytest.fixture
def facade():
    with conf_vars(
        {
            ("aws_auth_manager", "conn_id"): "aws_default",
            ("aws_auth_manager", "region_name"): REGION_NAME,
            ("aws_auth_manager", "avp_policy_store_id"): AVP_POLICY_STORE_ID,
        }
    ):
        yield AwsAuthManagerAmazonVerifiedPermissionsFacade()


class TestAwsAuthManagerAmazonVerifiedPermissionsFacade:
    def test_avp_client(self, facade):
        assert hasattr(facade, "avp_client")

    def test_avp_policy_store_id(self, facade):
        assert hasattr(facade, "avp_policy_store_id")

    def test_is_authorized_no_user(self, facade):
        method: ResourceMethod = "GET"
        entity_type = AvpEntities.VARIABLE

        result = facade.is_authorized(
            method=method,
            entity_type=entity_type,
            user=None,
        )

        assert result is False

    @pytest.mark.parametrize(
        "entity_id, context, user, expected_entities, expected_context, avp_response, expected",
        [
            # User with groups with no permissions
            (
                None,
                None,
                test_user,
                [
                    {
                        "identifier": {"entityType": "Airflow::User", "entityId": "test_user"},
                        "parents": [
                            {"entityType": "Airflow::Group", "entityId": "group1"},
                            {"entityType": "Airflow::Group", "entityId": "group2"},
                        ],
                    },
                    {
                        "identifier": {"entityType": "Airflow::Group", "entityId": "group1"},
                    },
                    {
                        "identifier": {"entityType": "Airflow::Group", "entityId": "group2"},
                    },
                ],
                None,
                {"decision": "DENY"},
                False,
            ),
            # User with groups with permissions
            (
                "dummy_id",
                None,
                test_user,
                [
                    {
                        "identifier": {"entityType": "Airflow::User", "entityId": "test_user"},
                        "parents": [
                            {"entityType": "Airflow::Group", "entityId": "group1"},
                            {"entityType": "Airflow::Group", "entityId": "group2"},
                        ],
                    },
                    {
                        "identifier": {"entityType": "Airflow::Group", "entityId": "group1"},
                    },
                    {
                        "identifier": {"entityType": "Airflow::Group", "entityId": "group2"},
                    },
                ],
                None,
                {"decision": "ALLOW"},
                True,
            ),
            # User without group without permission
            (
                None,
                None,
                test_user_no_group,
                [
                    {
                        "identifier": {"entityType": "Airflow::User", "entityId": "test_user_no_group"},
                        "parents": [],
                    },
                ],
                None,
                {"decision": "DENY"},
                False,
            ),
            # With context
            (
                "dummy_id",
                {"context_param": {"string": "value"}},
                test_user,
                [
                    {
                        "identifier": {"entityType": "Airflow::User", "entityId": "test_user"},
                        "parents": [
                            {"entityType": "Airflow::Group", "entityId": "group1"},
                            {"entityType": "Airflow::Group", "entityId": "group2"},
                        ],
                    },
                    {
                        "identifier": {"entityType": "Airflow::Group", "entityId": "group1"},
                    },
                    {
                        "identifier": {"entityType": "Airflow::Group", "entityId": "group2"},
                    },
                ],
                {"contextMap": {"context_param": {"string": "value"}}},
                {"decision": "ALLOW"},
                True,
            ),
        ],
    )
    def test_is_authorized_successful(
        self, facade, entity_id, context, user, expected_entities, expected_context, avp_response, expected
    ):
        mock_is_authorized = Mock(return_value=avp_response)
        facade.avp_client.is_authorized = mock_is_authorized

        method: ResourceMethod = "GET"
        entity_type = AvpEntities.VARIABLE

        result = facade.is_authorized(
            method=method,
            entity_type=entity_type,
            entity_id=entity_id,
            user=user,
            context=context,
        )

        params = prune_dict(
            {
                "policyStoreId": AVP_POLICY_STORE_ID,
                "principal": {"entityType": "Airflow::User", "entityId": user.get_id()},
                "action": {"actionType": "Airflow::Action", "actionId": get_action_id(entity_type, method)},
                "resource": {"entityType": get_entity_type(entity_type), "entityId": entity_id or "*"},
                "entities": {"entityList": expected_entities},
                "context": expected_context,
            }
        )

        mock_is_authorized.assert_called_once_with(**params)

        assert result == expected

    def test_is_authorized_unsuccessful(self, facade):
        avp_response = {"errors": ["Error"]}
        mock_is_authorized = Mock(return_value=avp_response)
        facade.avp_client.is_authorized = mock_is_authorized

        with pytest.raises(AirflowException, match="Error occurred while making an authorization decision."):
            facade.is_authorized(method="GET", entity_type=AvpEntities.VARIABLE, user=test_user)

    @pytest.mark.parametrize(
        "user, avp_response, expected",
        [
            (
                test_user,
                {"results": [{"decision": "ALLOW"}, {"decision": "DENY"}]},
                False,
            ),
            (
                test_user,
                {"results": [{"decision": "ALLOW"}, {"decision": "ALLOW"}]},
                True,
            ),
            (
                None,
                {"results": [{"decision": "ALLOW"}, {"decision": "ALLOW"}]},
                False,
            ),
        ],
    )
    def test_batch_is_authorized_successful(self, facade, user, avp_response, expected):
        mock_batch_is_authorized = Mock(return_value=avp_response)
        facade.avp_client.batch_is_authorized = mock_batch_is_authorized

        result = facade.batch_is_authorized(
            requests=[
                {"method": "GET", "entity_type": AvpEntities.VARIABLE, "entity_id": "var1"},
                {"method": "GET", "entity_type": AvpEntities.VARIABLE, "entity_id": "var1"},
            ],
            user=user,
        )

        assert result == expected

    def test_batch_is_authorized_unsuccessful(self, facade):
        avp_response = {"results": [{}, {"errors": []}, {"errors": [{"errorDescription": "Error"}]}]}
        mock_batch_is_authorized = Mock(return_value=avp_response)
        facade.avp_client.batch_is_authorized = mock_batch_is_authorized

        with pytest.raises(
            AirflowException, match="Error occurred while making a batch authorization decision."
        ):
            facade.batch_is_authorized(
                requests=[
                    {"method": "GET", "entity_type": AvpEntities.VARIABLE, "entity_id": "var1"},
                    {"method": "GET", "entity_type": AvpEntities.VARIABLE, "entity_id": "var1"},
                ],
                user=test_user,
            )

    def test_get_batch_is_authorized_single_result_successful(self, facade):
        single_result = {
            "request": {
                "principal": {"entityType": "Airflow::User", "entityId": "test_user"},
                "action": {"actionType": "Airflow::Action", "actionId": "Connection.GET"},
                "resource": {"entityType": "Airflow::Connection", "entityId": "*"},
            },
            "decision": "ALLOW",
        }

        result = facade.get_batch_is_authorized_single_result(
            batch_is_authorized_results=[
                {
                    "request": {
                        "principal": {"entityType": "Airflow::User", "entityId": "test_user"},
                        "action": {"actionType": "Airflow::Action", "actionId": "Variable.GET"},
                        "resource": {"entityType": "Airflow::Variable", "entityId": "*"},
                    },
                    "decision": "ALLOW",
                },
                single_result,
            ],
            request={
                "method": "GET",
                "entity_type": AvpEntities.CONNECTION,
            },
            user=test_user,
        )

        assert result == single_result

    def test_get_batch_is_authorized_single_result_unsuccessful(self, facade):
        with pytest.raises(AirflowException, match="Could not find the authorization result."):
            facade.get_batch_is_authorized_single_result(
                batch_is_authorized_results=[
                    {
                        "request": {
                            "principal": {"entityType": "Airflow::User", "entityId": "test_user"},
                            "action": {"actionType": "Airflow::Action", "actionId": "Variable.GET"},
                            "resource": {"entityType": "Airflow::Variable", "entityId": "*"},
                        },
                        "decision": "ALLOW",
                    },
                    {
                        "request": {
                            "principal": {"entityType": "Airflow::User", "entityId": "test_user"},
                            "action": {"actionType": "Airflow::Action", "actionId": "Variable.POST"},
                            "resource": {"entityType": "Airflow::Variable", "entityId": "*"},
                        },
                        "decision": "ALLOW",
                    },
                ],
                request={
                    "method": "GET",
                    "entity_type": AvpEntities.CONNECTION,
                },
                user=test_user,
            )

    def test_is_policy_store_schema_up_to_date_when_schema_up_to_date(self, facade, providers_src_folder):
        schema_path = providers_src_folder.joinpath(
            "airflow", "providers", "amazon", "aws", "auth_manager", "avp", "schema.json"
        ).resolve()
        with open(schema_path) as schema_file:
            avp_response = {"schema": schema_file.read()}
            mock_get_schema = Mock(return_value=avp_response)
            facade.avp_client.get_schema = mock_get_schema

            assert facade.is_policy_store_schema_up_to_date()

    def test_is_policy_store_schema_up_to_date_when_schema_is_modified(self, facade, providers_src_folder):
        schema_path = providers_src_folder.joinpath(
            "airflow", "providers", "amazon", "aws", "auth_manager", "avp", "schema.json"
        ).resolve()
        with open(schema_path) as schema_file:
            schema = json.loads(schema_file.read())
            schema["new_field"] = "new_value"
            avp_response = {"schema": json.dumps(schema)}
            mock_get_schema = Mock(return_value=avp_response)
            facade.avp_client.get_schema = mock_get_schema

            assert not facade.is_policy_store_schema_up_to_date()
