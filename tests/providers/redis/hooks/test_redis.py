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
from unittest import mock

import pytest

from airflow.models import Connection
from airflow.providers.redis.hooks.redis import RedisHook


class TestRedisHook(unittest.TestCase):
    def test_get_conn(self):
        hook = RedisHook(redis_conn_id='redis_default')
        self.assertEqual(hook.redis, None)

        self.assertEqual(hook.host, None, 'host initialised as None.')
        self.assertEqual(hook.port, None, 'port initialised as None.')
        self.assertEqual(hook.password, None, 'password initialised as None.')
        self.assertEqual(hook.db, None, 'db initialised as None.')
        self.assertIs(hook.get_conn(), hook.get_conn(), 'Connection initialized only if None.')

    @mock.patch('airflow.providers.redis.hooks.redis.Redis')
    @mock.patch('airflow.providers.redis.hooks.redis.RedisHook.get_connection',
                return_value=Connection(
                    password='password',
                    host='remote_host',
                    port=1234,
                    extra="""{
                        "db": 2,
                        "ssl": true,
                        "ssl_cert_reqs": "required",
                        "ssl_ca_certs": "/path/to/custom/ca-cert",
                        "ssl_keyfile": "/path/to/key-file",
                        "ssl_cert_file": "/path/to/cert-file",
                        "ssl_check_hostname": true
                    }"""
                ))
    def test_get_conn_with_extra_config(self, mock_get_connection, mock_redis):
        connection = mock_get_connection.return_value
        hook = RedisHook()

        hook.get_conn()
        mock_redis.assert_called_once_with(
            host=connection.host,
            password=connection.password,
            port=connection.port,
            db=connection.extra_dejson["db"],
            ssl=connection.extra_dejson["ssl"],
            ssl_cert_reqs=connection.extra_dejson["ssl_cert_reqs"],
            ssl_ca_certs=connection.extra_dejson["ssl_ca_certs"],
            ssl_keyfile=connection.extra_dejson["ssl_keyfile"],
            ssl_cert_file=connection.extra_dejson["ssl_cert_file"],
            ssl_check_hostname=connection.extra_dejson["ssl_check_hostname"]
        )

    def test_get_conn_password_stays_none(self):
        hook = RedisHook(redis_conn_id='redis_default')
        hook.get_conn()
        self.assertEqual(hook.password, None)

    @pytest.mark.integration("redis")
    def test_real_ping(self):
        hook = RedisHook(redis_conn_id='redis_default')
        redis = hook.get_conn()

        self.assertTrue(redis.ping(), 'Connection to Redis with PING works.')

    @pytest.mark.integration("redis")
    def test_real_get_and_set(self):
        hook = RedisHook(redis_conn_id='redis_default')
        redis = hook.get_conn()

        self.assertTrue(redis.set('test_key', 'test_value'), 'Connection to Redis with SET works.')
        self.assertEqual(redis.get('test_key'), b'test_value', 'Connection to Redis with GET works.')
        self.assertEqual(redis.delete('test_key'), 1, 'Connection to Redis with DELETE works.')


if __name__ == '__main__':
    unittest.main()
