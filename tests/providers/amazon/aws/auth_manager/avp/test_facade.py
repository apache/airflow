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

from typing import TYPE_CHECKING
from unittest.mock import Mock

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.auth_manager.avp.entities import AvpEntities, get_action_id, get_entity_type
from airflow.providers.amazon.aws.auth_manager.avp.facade import AwsAuthManagerAmazonVerifiedPermissionsFacade
from airflow.providers.amazon.aws.auth_manager.user import AwsAuthManagerUser
from tests.test_utils.config import conf_vars

if TYPE_CHECKING:
    from airflow.auth.managers.base_auth_manager import ResourceMethod

AVP_POLICY_STORE_ID = "store_id"

test_user = AwsAuthManagerUser(user_id="test_user", groups=["group1", "group2"])
test_user_no_group = AwsAuthManagerUser(user_id="test_user_no_group", groups=[])


def simple_entity_fetcher():
    return [
        {"identifier": {"entityType": "Airflow::Variable", "entityId": "var1"}},
        {"identifier": {"entityType": "Airflow::Variable", "entityId": "var2"}},
    ]


@pytest.fixture
def facade():
    return AwsAuthManagerAmazonVerifiedPermissionsFacade()


class TestAwsAuthManagerAmazonVerifiedPermissionsFacade:
    def test_avp_client(self, facade):
        assert hasattr(facade, "avp_client")

    def test_avp_policy_store_id(self, facade):
        with conf_vars(
            {
                ("aws_auth_manager", "avp_policy_store_id"): AVP_POLICY_STORE_ID,
            }
        ):
            assert hasattr(facade, "avp_policy_store_id")

    @pytest.mark.parametrize(
        "entity_id, user, entity_fetcher, expected_entities, avp_response, expected",
        [
            # User with groups with no permissions
            (
                None,
                test_user,
                None,
                [
                    {
                        "identifier": {"entityType": "Airflow::User", "entityId": "test_user"},
                        "parents": [
                            {"entityType": "Airflow::Role", "entityId": "group1"},
                            {"entityType": "Airflow::Role", "entityId": "group2"},
                        ],
                    },
                    {
                        "identifier": {"entityType": "Airflow::Role", "entityId": "group1"},
                    },
                    {
                        "identifier": {"entityType": "Airflow::Role", "entityId": "group2"},
                    },
                ],
                {"decision": "DENY"},
                False,
            ),
            # User with groups with permissions
            (
                "dummy_id",
                test_user,
                None,
                [
                    {
                        "identifier": {"entityType": "Airflow::User", "entityId": "test_user"},
                        "parents": [
                            {"entityType": "Airflow::Role", "entityId": "group1"},
                            {"entityType": "Airflow::Role", "entityId": "group2"},
                        ],
                    },
                    {
                        "identifier": {"entityType": "Airflow::Role", "entityId": "group1"},
                    },
                    {
                        "identifier": {"entityType": "Airflow::Role", "entityId": "group2"},
                    },
                ],
                {"decision": "ALLOW"},
                True,
            ),
            # User without group without permission
            (
                None,
                test_user_no_group,
                None,
                [
                    {
                        "identifier": {"entityType": "Airflow::User", "entityId": "test_user_no_group"},
                        "parents": [],
                    },
                ],
                {"decision": "DENY"},
                False,
            ),
            # With entity fetcher but no resource ID
            (
                None,
                test_user_no_group,
                simple_entity_fetcher,
                [
                    {
                        "identifier": {"entityType": "Airflow::User", "entityId": "test_user_no_group"},
                        "parents": [],
                    },
                ],
                {"decision": "DENY"},
                False,
            ),
            # With entity fetcher and resource ID
            (
                "resource_id",
                test_user_no_group,
                simple_entity_fetcher,
                [
                    {
                        "identifier": {"entityType": "Airflow::User", "entityId": "test_user_no_group"},
                        "parents": [],
                    },
                    {"identifier": {"entityType": "Airflow::Variable", "entityId": "var1"}},
                    {"identifier": {"entityType": "Airflow::Variable", "entityId": "var2"}},
                ],
                {"decision": "DENY"},
                False,
            ),
        ],
    )
    def test_is_authorized_successful(
        self, facade, entity_id, user, entity_fetcher, expected_entities, avp_response, expected
    ):
        mock_is_authorized = Mock(return_value=avp_response)
        facade.avp_client.is_authorized = mock_is_authorized

        method: ResourceMethod = "GET"
        entity_type = AvpEntities.VARIABLE

        with conf_vars(
            {
                ("aws_auth_manager", "avp_policy_store_id"): AVP_POLICY_STORE_ID,
            }
        ):
            result = facade.is_authorized(
                method=method,
                entity_type=entity_type,
                entity_id=entity_id,
                user=user,
                entity_fetcher=entity_fetcher,
            )

        mock_is_authorized.assert_called_once_with(
            policyStoreId=AVP_POLICY_STORE_ID,
            principal={"entityType": "Airflow::User", "entityId": user.get_id()},
            action={"actionType": "Airflow::Action", "actionId": get_action_id(entity_type, method)},
            resource={"entityType": get_entity_type(entity_type), "entityId": entity_id or "*"},
            entities={"entityList": expected_entities},
        )

        assert result == expected

    def test_is_authorized_unsuccessful(self, facade):
        avp_response = {"errors": ["Error"]}
        mock_is_authorized = Mock(return_value=avp_response)
        facade.avp_client.is_authorized = mock_is_authorized

        with conf_vars(
            {
                ("aws_auth_manager", "avp_policy_store_id"): AVP_POLICY_STORE_ID,
            }
        ):
            with pytest.raises(
                AirflowException, match="Error occurred while making an authorization decision."
            ):
                facade.is_authorized(method="GET", entity_type=AvpEntities.VARIABLE, user=test_user)
