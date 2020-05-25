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


class TestElastiCacheHook(TestCase):
    """
    Test ElastiCacheHook
    """
    replication_group_id = "test-elasticache-hook"

    replication_group_config = {
        'ReplicationGroupId': replication_group_id,
        'ReplicationGroupDescription': replication_group_id,
        'AutomaticFailoverEnabled': False,
        'NumCacheClusters': 1,
        'CacheNodeType': 'cache.m5.large',
        'Engine': 'redis',
        'EngineVersion': '5.0.4',
        'CacheParameterGroupName': 'default.redis5.0'
    }

    valid_status = ('creating', 'available', 'modifying', 'deleting', 'create - failed', 'snapshotting')

    hook = Mock()

    def test_get_conn_not_none(self):
        """
        Test connection is not None
        """
        self.assertIsNotNone(self.hook.conn)

    def test_create_replication_group(self):
        """
        Test creation of replication group
        """
        self.hook.create_replication_group.return_value = {
            "ReplicationGroup": {
                "ReplicationGroupId": self.replication_group_id,
                "Status": "creating"
            }
        }
        response = self.hook.create_replication_group(config=self.replication_group_config)

        assert response["ReplicationGroup"]["ReplicationGroupId"] == self.replication_group_id
        assert response["ReplicationGroup"]["Status"] == "creating"

    def test_describe_replication_group(self):
        """
        Test describing replication group
        """
        self.hook.describe_replication_group.return_value = {
            "ReplicationGroups": [
                {
                    "ReplicationGroupId": self.replication_group_id
                }
            ]
        }

        self.hook.create_replication_group(config=self.replication_group_config)
        response = self.hook.describe_replication_group(replication_group_id=self.replication_group_id)

        assert response["ReplicationGroups"][0]["ReplicationGroupId"] == self.replication_group_id

    def test_get_replication_group_status(self):
        """
        Test getting status of replication group
        """
        self.hook.get_replication_group_status.return_value = 'creating'
        self.hook.create_replication_group(config=self.replication_group_config)
        response = self.hook.get_replication_group_status(replication_group_id=self.replication_group_id)

        assert response in self.valid_status

    def test_is_replication_group_available(self):
        """
        Test checking availability of replication group
        """
        self.hook.is_replication_group_available.return_value = False
        self.hook.create_replication_group(config=self.replication_group_config)
        response = self.hook.is_replication_group_available(replication_group_id=self.replication_group_id)

        assert response in (True, False)

    def test_should_stop_poking(self):
        """
        Test if we should stop poking replication group for availability
        """
        self.hook._has_reached_terminal_state.return_value = True, 'available'
        self.hook.create_replication_group(config=self.replication_group_config)
        stop_poking, status = self.hook._has_reached_terminal_state(self.replication_group_id)

        assert stop_poking in (True, False)
        assert status in self.valid_status

    def test_wait_for_availability(self):
        """
        Test waiting for availability of replication group
        """
        self.hook.wait_for_availability.return_value = True
        self.hook.create_replication_group(config=self.replication_group_config)
        response = self.hook.wait_for_availability(replication_group_id=self.replication_group_id)

        assert response in (True, False)

    def test_delete_replication_group(self):
        """
        Test deletion of replication group
        """
        self.hook.delete_replication_group.return_value = {
            "ReplicationGroup": {
                "ReplicationGroupId": self.replication_group_id,
                "Status": "deleting"
            }
        }
        self.hook.create_replication_group(config=self.replication_group_config)
        self.hook.wait_for_availability(replication_group_id=self.replication_group_id)
        response = self.hook.delete_replication_group(replication_group_id=self.replication_group_id)

        assert response["ReplicationGroup"]["ReplicationGroupId"] == self.replication_group_id
        assert response["ReplicationGroup"]["Status"] == "deleting"

    def test_wait_for_deletion(self):
        """
        Test waiting for deletion of replication group
        """
        self.hook.wait_for_deletion.return_value = {
            "ReplicationGroup": {
                "ReplicationGroupId": self.replication_group_id
            }
        }, True
        self.hook.create_replication_group(config=self.replication_group_config)
        response, deleted = self.hook.wait_for_deletion(replication_group_id=self.replication_group_id)

        assert response["ReplicationGroup"]["ReplicationGroupId"] == self.replication_group_id
        assert deleted in (True, False)

    def test_ensure_delete_replication_groups_success(self):
        """
        Test deletion of replication group with surety that it is deleted
        """
        self.hook.ensure_delete_replication_group.return_value = {
            "ReplicationGroup": {
                "ReplicationGroupId": self.replication_group_id
            }
        }
        self.hook.create_replication_group(config=self.replication_group_config)

        with self.assertRaises(AirflowException):
            response = self.hook.ensure_delete_replication_group(self.replication_group_id)

            assert response["ReplicationGroup"]["ReplicationGroupId"] == self.replication_group_id

    def test_ensure_delete_replication_groups_failure(self):
        """
        Test failure case for deletion of replication group with surety that it is deleted
        """
        self.hook.ensure_delete_replication_group.side_effect = AirflowException
        self.hook.create_replication_group(config=self.replication_group_config)

        with self.assertRaises(AirflowException):
            # Try only 1 once with 5 sec buffer time. This will ensure that the `wait_for_deletion` loop
            # breaks quickly before the group is deleted and we get the Airflow exception
            self.hook.ensure_delete_replication_group(
                replication_group_id=self.replication_group_id,
                initial_sleep_time=5,
                exponential_back_off_factor=1,
                max_retries=1
            )
