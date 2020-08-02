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

from unittest import TestCase
from unittest.mock import Mock

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.elasticache_replication_group import ElastiCacheReplicationGroupHook


class TestElastiCacheHook(TestCase):
    """
    Test ElastiCacheHook
    """
    REPLICATION_GROUP_ID = "test-elasticache-hook"

    REPLICATION_GROUP_CONFIG = {
        'ReplicationGroupId': REPLICATION_GROUP_ID,
        'ReplicationGroupDescription': REPLICATION_GROUP_ID,
        'AutomaticFailoverEnabled': False,
        'NumCacheClusters': 1,
        'CacheNodeType': 'cache.m5.large',
        'Engine': 'redis',
        'EngineVersion': '5.0.4',
        'CacheParameterGroupName': 'default.redis5.0'
    }

    VALID_STATES = frozenset({
        'creating', 'available', 'modifying', 'deleting', 'create - failed', 'snapshotting'
    })

    # Track calls to describe when deleting replication group
    # First call will return status as `available` and we will initiate delete
    # Second call with return status as `deleting`
    # Subsequent call will raise ReplicationGroupNotFoundFault exception
    describe_call_count_for_delete = 0

    def setUp(self):
        self.hook = ElastiCacheReplicationGroupHook()
        setattr(self.hook, 'conn', Mock())

        # We need this for every test
        self.hook.conn.create_replication_group.return_value = {
            "ReplicationGroup": {
                "ReplicationGroupId": self.REPLICATION_GROUP_ID,
                "Status": "creating"
            }
        }

    def _create_replication_group(self):
        return self.hook.create_replication_group(config=self.REPLICATION_GROUP_CONFIG)

    def test_get_conn_not_none(self):
        """
        Test connection is not None
        """
        self.assertIsNotNone(self.hook.conn)

    def test_create_replication_group(self):
        """
        Test creation of replication group
        """
        response = self._create_replication_group()
        assert response["ReplicationGroup"]["ReplicationGroupId"] == self.REPLICATION_GROUP_ID
        assert response["ReplicationGroup"]["Status"] == "creating"

    def test_describe_replication_group(self):
        """
        Test describing replication group
        """
        self._create_replication_group()

        self.hook.conn.describe_replication_groups.return_value = {
            "ReplicationGroups": [
                {
                    "ReplicationGroupId": self.REPLICATION_GROUP_ID
                }
            ]
        }

        response = self.hook.describe_replication_group(replication_group_id=self.REPLICATION_GROUP_ID)
        assert response["ReplicationGroups"][0]["ReplicationGroupId"] == self.REPLICATION_GROUP_ID

    def test_get_replication_group_status(self):
        """
        Test getting status of replication group
        """
        self._create_replication_group()

        self.hook.conn.describe_replication_groups.return_value = {
            "ReplicationGroups": [
                {
                    "ReplicationGroupId": self.REPLICATION_GROUP_ID,
                    "Status": "available"
                }
            ]
        }

        response = self.hook.get_replication_group_status(replication_group_id=self.REPLICATION_GROUP_ID)
        assert response in self.VALID_STATES

    def test_is_replication_group_available(self):
        """
        Test checking availability of replication group
        """
        self._create_replication_group()

        self.hook.conn.describe_replication_groups.return_value = {
            "ReplicationGroups": [
                {
                    "ReplicationGroupId": self.REPLICATION_GROUP_ID,
                    "Status": "available"
                }
            ]
        }

        response = self.hook.is_replication_group_available(replication_group_id=self.REPLICATION_GROUP_ID)
        assert response in (True, False)

    def test_has_reached_terminal_state(self):
        """
        Test if we replication group has reached a terminal state or not
        """
        self._create_replication_group()

        # Terminal state reached
        self.hook.conn.describe_replication_groups.return_value = {
            "ReplicationGroups": [
                {
                    "ReplicationGroupId": self.REPLICATION_GROUP_ID,
                    "Status": "available"
                }
            ]
        }

        stop_poking, status = self.hook._has_reached_terminal_state(self.REPLICATION_GROUP_ID)
        assert stop_poking is True
        assert status in self.VALID_STATES

        # Terminal state not reached
        self.hook.conn.describe_replication_groups.return_value = {
            "ReplicationGroups": [
                {
                    "ReplicationGroupId": self.REPLICATION_GROUP_ID,
                    "Status": "modifying"
                }
            ]
        }

        stop_poking, status = self.hook._has_reached_terminal_state(self.REPLICATION_GROUP_ID)
        assert stop_poking is False
        assert status in self.VALID_STATES

    def test_wait_for_availability(self):
        """
        Test waiting for availability of replication group
        """
        self._create_replication_group()

        # Test non availability
        self.hook.conn.describe_replication_groups.return_value = {
            "ReplicationGroups": [
                {
                    "ReplicationGroupId": self.REPLICATION_GROUP_ID,
                    "Status": "creating"
                }
            ]
        }

        response = self.hook.wait_for_availability(
            replication_group_id=self.REPLICATION_GROUP_ID,
            max_retries=1,
            initial_sleep_time=1,  # seconds
        )
        assert response is False

        # Test availability
        self.hook.conn.describe_replication_groups.return_value = {
            "ReplicationGroups": [
                {
                    "ReplicationGroupId": self.REPLICATION_GROUP_ID,
                    "Status": "available"
                }
            ]
        }

        response = self.hook.wait_for_availability(
            replication_group_id=self.REPLICATION_GROUP_ID,
            max_retries=1,
            initial_sleep_time=1,  # seconds
        )
        assert response is True

    def test_delete_replication_group(self):
        """
        Test deletion of replication group
        """
        self._create_replication_group()

        self.hook.conn.delete_replication_group.return_value = {
            "ReplicationGroup": {
                "ReplicationGroupId": self.REPLICATION_GROUP_ID,
                "Status": "deleting"
            }
        }

        # Wait for availability, can only delete when replication group is available
        self.hook.conn.describe_replication_groups.return_value = {
            "ReplicationGroups": [
                {
                    "ReplicationGroupId": self.REPLICATION_GROUP_ID,
                    "Status": "available"
                }
            ]
        }

        response = self.hook.wait_for_availability(
            replication_group_id=self.REPLICATION_GROUP_ID,
            max_retries=1,
            initial_sleep_time=1,  # seconds
        )
        assert response is True

        response = self.hook.delete_replication_group(replication_group_id=self.REPLICATION_GROUP_ID)
        assert response["ReplicationGroup"]["ReplicationGroupId"] == self.REPLICATION_GROUP_ID
        assert response["ReplicationGroup"]["Status"] == "deleting"

    # noinspection PyUnusedLocal
    def _mock_describe_side_effect(self, *args, **kwargs):
        """
        Mock describe calls to replication group for testing delete calls
        """
        # On first call replication group is in available state, this will allow to initiate a delete
        # A replication group can only be deleted when it is in `available` state
        if self.describe_call_count_for_delete == 0:
            self.describe_call_count_for_delete = self.describe_call_count_for_delete + 1

            return {
                "ReplicationGroups": [
                    {
                        "ReplicationGroupId": self.REPLICATION_GROUP_ID,
                        "Status": "available"
                    }
                ]
            }

        # On second call replication group is in deleting state
        if self.describe_call_count_for_delete == 1:
            self.describe_call_count_for_delete = self.describe_call_count_for_delete + 1

            return {
                "ReplicationGroups": [
                    {
                        "ReplicationGroupId": self.REPLICATION_GROUP_ID,
                        "Status": "deleting"
                    }
                ]
            }

        # On further calls we will assume the replication group is deleted
        class MockReplicationGroupNotFoundFault(BaseException):
            pass

        self.hook.conn.exceptions.ReplicationGroupNotFoundFault = MockReplicationGroupNotFoundFault

        raise self.hook.conn.exceptions.ReplicationGroupNotFoundFault

    def test_wait_for_deletion(self):
        """
        Test waiting for deletion of replication group
        """
        self.describe_call_count_for_delete = 0

        self._create_replication_group()

        self.hook.conn.describe_replication_groups.side_effect = self._mock_describe_side_effect

        self.hook.conn.delete_replication_group.return_value = {
            "ReplicationGroup": {
                "ReplicationGroupId": self.REPLICATION_GROUP_ID
            }
        }

        response, deleted = self.hook.wait_for_deletion(
            replication_group_id=self.REPLICATION_GROUP_ID,
            # Initial call - status is `available`
            # 1st retry - status is `deleting`
            # 2nd retry - Replication group is deleted
            max_retries=2,
            initial_sleep_time=1
        )
        assert response["ReplicationGroup"]["ReplicationGroupId"] == self.REPLICATION_GROUP_ID
        assert deleted is True

    def test_ensure_delete_replication_group_success(self):
        """
        Test deletion of replication group with surety that it is deleted
        """
        self.describe_call_count_for_delete = 0

        self._create_replication_group()

        self.hook.conn.describe_replication_groups.side_effect = self._mock_describe_side_effect

        self.hook.conn.delete_replication_group.return_value = {
            "ReplicationGroup": {
                "ReplicationGroupId": self.REPLICATION_GROUP_ID
            }
        }

        response = self.hook.ensure_delete_replication_group(
            replication_group_id=self.REPLICATION_GROUP_ID,
            initial_sleep_time=1,
            max_retries=2
        )

        assert response["ReplicationGroup"]["ReplicationGroupId"] == self.REPLICATION_GROUP_ID

    def test_ensure_delete_replication_group_failure(self):
        """
        Test failure case for deletion of replication group with surety that it is deleted
        """
        self.describe_call_count_for_delete = 0

        self._create_replication_group()

        self.hook.conn.describe_replication_groups.side_effect = self._mock_describe_side_effect

        self.hook.conn.delete_replication_group.return_value = {
            "ReplicationGroup": {
                "ReplicationGroupId": self.REPLICATION_GROUP_ID
            }
        }

        with self.assertRaises(AirflowException):
            # Try only 1 once with 1 sec buffer time. This will ensure that the `wait_for_deletion` loop
            # breaks quickly before the group is deleted and we get the Airflow exception
            self.hook.ensure_delete_replication_group(
                replication_group_id=self.REPLICATION_GROUP_ID,
                initial_sleep_time=1,
                max_retries=1
            )
