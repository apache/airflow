# -*- coding: utf-8 -*-
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

from mock import Mock, patch

from airflow.contrib.hooks.redis_hook import RedisHook


class TestRedisHook(unittest.TestCase):
    def test_get_conn(self):
        hook = RedisHook(redis_conn_id='redis_default')

        self.assertEqual(hook.redis, None)

        self.assertEqual(hook.host, None, 'host initialised as None.')
        self.assertEqual(hook.port, None, 'port initialised as None.')
        self.assertEqual(hook.password, None, 'password initialised as None.')
        self.assertEqual(hook.db, None, 'db initialised as None.')

        self.assertIs(hook.get_conn(), hook.get_conn(), 'Connection initialized only if None.')

    def test_get_conn_password_stays_none(self):
        hook = RedisHook(redis_conn_id='redis_default')
        hook.get_conn()
        self.assertEqual(hook.password, None)

    @patch('redis.Redis.ping', Mock(return_value=True))
    def test_ping(self):
        hook = RedisHook(redis_conn_id='redis_default')

        self.assertTrue(hook.ping_redis(), 'Connection to Redis with PING works.')

    @patch('redis.Redis.set', Mock(return_value=True))
    @patch('redis.Redis.get', Mock(return_value=b'_value'))
    @patch('redis.Redis.delete', Mock(return_value=1))
    def test_get_and_set(self):
        hook = RedisHook(redis_conn_id='redis_default')

        self.assertTrue(hook.set_key_value('_dummy', '_value'), 'Connection to Redis with SET works.')
        self.assertEqual(hook.get_key('_dummy'), b'_value', 'Connection to Redis with GET works.')
        self.assertEqual(hook.delete_key('_dummy'), 1, 'Connection to Redis with DELETE works.')

    @patch('redis.Redis.exists', Mock(return_value=True))
    def test_exists(self):
        hook = RedisHook(redis_conn_id='redis_default')

        assert hook.check_if_key_exists('_dummy')

    @patch('redis.Redis.exists', Mock(return_value=False))
    def test_exists_false(self):
        hook = RedisHook(redis_conn_id='redis_default')

        assert not hook.check_if_key_exists('_dummy')


if __name__ == '__main__':
    unittest.main()
