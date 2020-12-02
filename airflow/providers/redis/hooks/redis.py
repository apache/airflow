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

"""RedisHook module"""
from redis import Redis

from airflow.hooks.base_hook import BaseHook


class RedisHook(BaseHook):
    """
    Wrapper for connection to interact with Redis in-memory data structure store

    You can set your db in the extra field of your connection as ``{"db": 3}``.
    Also you can set ssl parameters as:
    ``{"ssl": true, "ssl_cert_reqs": "require", "ssl_cert_file": "/path/to/cert.pem", etc}``.
    """

    conn_name_attr = 'redis_conn_id'
    default_conn_name = 'redis_default'
    conn_type = 'redis'

    def __init__(self, redis_conn_id: str = default_conn_name) -> None:
        """
        Prepares hook to connect to a Redis database.

        :param conn_id:     the name of the connection that has the parameters
                            we need to connect to Redis.
        """
        super().__init__()
        self.redis_conn_id = redis_conn_id
        self.redis = None
        self.host = None
        self.port = None
        self.password = None
        self.db = None

    def get_conn(self):
        """Returns a Redis connection."""
        conn = self.get_connection(self.redis_conn_id)
        self.host = conn.host
        self.port = conn.port
        self.password = None if str(conn.password).lower() in ['none', 'false', ''] else conn.password
        self.db = conn.extra_dejson.get('db')

        # check for ssl parameters in conn.extra
        ssl_arg_names = [
            "ssl",
            "ssl_cert_reqs",
            "ssl_ca_certs",
            "ssl_keyfile",
            "ssl_cert_file",
            "ssl_check_hostname",
        ]
        ssl_args = {name: val for name, val in conn.extra_dejson.items() if name in ssl_arg_names}

        if not self.redis:
            self.log.debug(
                'Initializing redis object for conn_id "%s" on %s:%s:%s',
                self.redis_conn_id,
                self.host,
                self.port,
                self.db,
            )
            self.redis = Redis(host=self.host, port=self.port, password=self.password, db=self.db, **ssl_args)

        return self.redis
