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

import unittest

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.elasticache import ElastiCacheHook

try:
    from moto import mock_elasticache
except ImportError:
    mock_elasticache = None


@unittest.skipIf(mock_elasticache is None, 'mock_elasticache package not available')
class TestElastiCacheHook(unittest.TestCase):
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

    hook = ElastiCacheHook()

    @mock_elasticache
    def test_get_conn_not_none(self):
        """
        Test connection is not None
        """
        self.assertIsNotNone(self.hook.conn)

    @mock_elasticache
    def test_create_replication_group(self):
        """
        Test creation of replication group
        """
        response = self.hook.create_replication_group(config=self.replication_group_config)

        assert response["ReplicationGroup"]["ReplicationGroupId"] == self.replication_group_id
        assert response["ReplicationGroup"]["Status"] == "creating"

    @mock_elasticache
    def test_describe_replication_group(self):
        """
        Test describing replication group
        """
        self.hook.create_replication_group(config=self.replication_group_config)
        response = self.hook.describe_replication_group(replication_group_id=self.replication_group_id)

        assert response["ReplicationGroups"][0]["ReplicationGroupId"] == self.replication_group_id

    @mock_elasticache
    def test_get_replication_group_status(self):
        """
        Test getting status of replication group
        """
        self.hook.create_replication_group(config=self.replication_group_config)
        response = self.hook.get_replication_group_status(replication_group_id=self.replication_group_id)

        assert response in self.valid_status

    @mock_elasticache
    def test_is_replication_group_available(self):
        """
        Test checking availability of replication group
        """
        self.hook.create_replication_group(config=self.replication_group_config)
        response = self.hook.is_replication_group_available(replication_group_id=self.replication_group_id)

        assert response in (True, False)

    @mock_elasticache
    def test_should_stop_poking(self):
        """
        Test if we should stop poking replication group for availability
        """
        self.hook.create_replication_group(config=self.replication_group_config)
        stop_poking, status = self.hook._has_reached_terminal_state(self.replication_group_id)

        assert stop_poking in (True, False)
        assert status in self.valid_status

    @mock_elasticache
    def test_wait_for_availability(self):
        """
        Test waiting for availability of replication group
        """
        self.hook.create_replication_group(config=self.replication_group_config)
        response = self.hook.wait_for_availability(replication_group_id=self.replication_group_id)

        assert response in (True, False)

    @mock_elasticache
    def test_delete_replication_group(self):
        """
        Test deletion of replication group
        """
        self.hook.create_replication_group(config=self.replication_group_config)
        self.hook.wait_for_availability(replication_group_id=self.replication_group_id)
        response = self.hook.delete_replication_group(replication_group_id=self.replication_group_id)

        assert response["ReplicationGroup"]["ReplicationGroupId"] == self.replication_group_id
        assert response["ReplicationGroup"]["Status"] == "deleting"

    @mock_elasticache
    def test_wait_for_deletion(self):
        """
        Test waiting for deletion of replication group
        """
        self.hook.create_replication_group(config=self.replication_group_config)
        response, deleted = self.hook.wait_for_deletion(replication_group_id=self.replication_group_id)

        assert response["ReplicationGroup"]["ReplicationGroupId"] == self.replication_group_id
        assert deleted in (True, False)

    @mock_elasticache
    def test_ensure_delete_replication_groups_success(self):
        """
        Test deletion of replication group with surety that it is deleted
        """
        self.hook.create_replication_group(config=self.replication_group_config)
        response = self.hook.ensure_delete_replication_group(replication_group_id=self.replication_group_id)

        assert response["ReplicationGroup"]["ReplicationGroupId"] == self.replication_group_id

    @mock_elasticache
    def test_ensure_delete_replication_groups_failure(self):
        """
        Test failure case for deletion of replication group with surety that it is deleted
        """
        self.hook.create_replication_group(config=self.replication_group_config)

        self.assertRaises(
            AirflowException,
            # Try only 1 once with 5 sec buffer time. This will ensure that the `wait_for_deletion` loop
            # breaks quickly before the group is deleted and we get the Airflow exception
            self.hook.ensure_delete_replication_group(
                replication_group_id=self.replication_group_id,
                initial_sleep_time=5,
                exponential_back_off_factor=1,
                max_retries=1
            )
        )
