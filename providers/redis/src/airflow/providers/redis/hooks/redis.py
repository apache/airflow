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
"""RedisHook module."""

from __future__ import annotations

import inspect
from typing import Any

import redis
from redis import Redis
from redis.cluster import ClusterNode, RedisCluster

from airflow.providers.common.compat.sdk import BaseHook
from airflow.providers.redis import __version__ as provider_version

DriverInfo = getattr(redis, "DriverInfo", None)

DEFAULT_SSL_CERT_REQS = "required"
ALLOWED_SSL_CERT_REQS = [DEFAULT_SSL_CERT_REQS, "optional", "none"]
DEFAULT_REDIS_PORT = 6379

# Check at module import time what Redis client identification features are supported
_REDIS_PARAMS = inspect.signature(Redis.__init__).parameters
_SUPPORTS_LIB_NAME = "lib_name" in _REDIS_PARAMS


class RedisHook(BaseHook):
    """
    Wrapper for connection to interact with Redis in-memory data structure store.

    You can set your db in the extra field of your connection as ``{"db": 3}``.
    Also you can set ssl parameters as:
    ``{"ssl": true, "ssl_cert_reqs": "require", "ssl_certfile": "/path/to/cert.pem", etc}``.

    To talk to a Redis deployment running in cluster mode, set ``{"cluster": true}``. Additional
    bootstrap nodes may be listed as Connection Extras
    ``{"startup_nodes": "node-2:6379,node-3:6379"}`` so that a single unreachable node does not
    leave the whole connection unusable. Cluster mode only supports database 0, so ``db`` must be
    left unset or 0.
    """

    conn_name_attr = "redis_conn_id"
    default_conn_name = "redis_default"
    conn_type = "redis"
    hook_name = "Redis"

    def __init__(self, redis_conn_id: str = default_conn_name, **kwargs) -> None:
        """
        Prepare hook to connect to a Redis database.

        :param conn_id:     the name of the connection that has the parameters
                            we need to connect to Redis.
        """
        super().__init__()
        self.redis_conn_id = redis_conn_id
        self.redis = None
        self.host = kwargs.get("host", None)
        self.port = kwargs.get("port", None)
        self.username = kwargs.get("username", None)
        self.password = kwargs.get("password", None)
        self.db = kwargs.get("db", None)
        self.cluster = kwargs.get("cluster", False)
        self.startup_nodes = kwargs.get("startup_nodes", None)

    def get_conn(self):
        """Return a Redis connection."""
        conn = self.get_connection(self.redis_conn_id)
        self.host = conn.host
        self.port = conn.port
        self.username = conn.login
        self.password = None if str(conn.password).lower() in ["none", "false", ""] else conn.password
        self.db = conn.extra_dejson.get("db")
        self.cluster = conn.extra_dejson.get("cluster", False)
        self.startup_nodes = conn.extra_dejson.get("startup_nodes")

        # https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/#implemented-subset
        if self.cluster and self.db not in (None, 0):
            raise ValueError(
                f"Redis connection {self.redis_conn_id!r} sets `db` to {self.db!r}, but Redis in cluster "
                "mode only supports database 0. Remove `db` from the connection extra."
            )

        # check for ssl parameters in conn.extra
        ssl_arg_names = [
            "ssl",
            "ssl_cert_reqs",
            "ssl_ca_certs",
            "ssl_keyfile",
            "ssl_certfile",
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

            # Add driver info for client identification if supported
            # This allows Redis server to identify the Redis provider as the upstream driver.
            # See: https://redis.io/docs/latest/commands/client-setinfo/
            driver_info_options: dict[str, Any] = {}
            if DriverInfo is not None:
                driver_info = DriverInfo().add_upstream_driver(
                    "apache-airflow-providers-redis", provider_version
                )
                driver_info_options = {"driver_info": driver_info}
            elif _SUPPORTS_LIB_NAME:
                driver_info_options = {
                    "lib_name": f"redis-py(apache-airflow-providers-redis_v{provider_version})",
                }

            if self.cluster:
                self.redis = RedisCluster(
                    host=self.host,
                    port=self.port,
                    startup_nodes=self._build_startup_nodes(),
                    username=self.username,
                    password=self.password,
                    **ssl_args,
                    **driver_info_options,
                )
            else:
                self.redis = Redis(
                    host=self.host,
                    port=self.port,
                    username=self.username,
                    password=self.password,
                    db=self.db,
                    **ssl_args,
                    **driver_info_options,
                )

        return self.redis

    def _build_startup_nodes(self) -> list[ClusterNode]:
        """Build redis-py cluster nodes from the ``startup_nodes`` extra, given as ``host`` or ``host:port``."""
        if not self.startup_nodes:
            return []

        if not isinstance(self.startup_nodes, str):
            raise ValueError(
                "The `startup_nodes` parameter value must be a comma-separated string of "
                f"`host:port` entries, got {self.startup_nodes!r}."
            )

        nodes = []
        for entry in self.startup_nodes.split(","):
            host, _, port = entry.strip().partition(":")
            if not host:
                raise ValueError(
                    f"Missing host in `startup_nodes` parameter value for entry {entry!r}; "
                    "expected `host:port`."
                )
            try:
                parsed_port = int(port) if port else DEFAULT_REDIS_PORT
            except ValueError:
                raise ValueError(
                    f"Invalid port in `startup_nodes` parameter value for entry {entry!r}; "
                    "expected `host:port`."
                ) from None
            nodes.append(ClusterNode(host, parsed_port))
        return nodes

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom UI field behaviour for Redis connection."""
        return {
            "hidden_fields": ["schema", "extra"],
            "relabeling": {},
        }

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to Redis connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import BooleanField, IntegerField, StringField
        from wtforms.validators import Optional, any_of

        return {
            "db": IntegerField(lazy_gettext("DB"), widget=BS3TextFieldWidget(), default=0),
            "cluster": BooleanField(lazy_gettext("Is cluster"), default=False),
            "startup_nodes": StringField(
                lazy_gettext("Startup nodes"),
                widget=BS3TextFieldWidget(),
                validators=[Optional()],
                description=(
                    "Comma-separated extra bootstrap nodes as host:port. Only for cluster Redis deployments."
                ),
                default=None,
            ),
            "ssl": BooleanField(lazy_gettext("Enable SSL"), default=False),
            "ssl_cert_reqs": StringField(
                lazy_gettext("SSL verify mode"),
                validators=[any_of(ALLOWED_SSL_CERT_REQS)],
                widget=BS3TextFieldWidget(),
                description=f"Must be one of: {', '.join(ALLOWED_SSL_CERT_REQS)}.",
                default=DEFAULT_SSL_CERT_REQS,
            ),
            "ssl_ca_certs": StringField(
                lazy_gettext("CA certificate path"),
                widget=BS3TextFieldWidget(),
                validators=[Optional()],
                default=None,
            ),
            "ssl_keyfile": StringField(
                lazy_gettext("Private key path"),
                widget=BS3TextFieldWidget(),
                validators=[Optional()],
                default=None,
            ),
            "ssl_certfile": StringField(
                lazy_gettext("Certificate path"),
                widget=BS3TextFieldWidget(),
                validators=[Optional()],
                default=None,
            ),
            "ssl_check_hostname": BooleanField(lazy_gettext("Enable hostname check"), default=False),
        }
