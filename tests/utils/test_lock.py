# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import mock
import unittest
import time

from airflow.utils import lock

import fakeredis


class TestLock(unittest.TestCase):

    def setUp(self):
        self.key = "test_airflow_key"
        self.redis_server = fakeredis.FakeServer()
        self.redis_nodes = [
            fakeredis.FakeStrictRedis(server=self.redis_server,
                                      decode_responses=True)
            for i in range(3)
        ]
        self.redis_cli = fakeredis.FakeStrictRedis(server=self.redis_server,
                                                   decode_responses=True)
        self.lock_retry = 3
        self.lock_ttl = 10

    def gen_lock(self):
        """Generate lock
        """
        return lock.Lock(self.key,
                         connection_details=self.redis_nodes,
                         retry_times=self.lock_retry,
                         ttl=self.lock_ttl)

    def test_lock(self):
        """
        :return:
        """
        # Test lock
        l1 = self.gen_lock()
        for i in range(10):
            self.assertEqual(l1.acquire(), True)
            time.sleep(1.2)
        # assert ttl time
        self.assertLess(0, self.redis_cli.ttl(self.key))
        l2 = self.gen_lock()
        self.assertEqual(l2.acquire(), False)

        # Test release
        self.assertEqual(True, self.redis_cli.exists(self.key))
        l1.release()
        self.assertEqual(False, self.redis_cli.exists(self.key))


if __name__ == '__main__':
    unittest.main()
