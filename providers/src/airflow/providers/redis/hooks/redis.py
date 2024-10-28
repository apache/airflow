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

import warnings
from typing import Any

from redis import Redis

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.hooks.base import BaseHook

DEFAULT_SSL_CERT_REQS = "required"
ALLOWED_SSL_CERT_REQS = [DEFAULT_SSL_CERT_REQS, "optional", "none"]


class RedisHook(BaseHook):
    """
    Wrapper for connection to interact with Redis in-memory data structure store.

    You can set your db in the extra field of your connection as ``{"db": 3}``.
    Also you can set ssl parameters as:
    ``{"ssl": true, "ssl_cert_reqs": "require", "ssl_cert_file": "/path/to/cert.pem", etc}``.
    """

    conn_name_attr = "redis_conn_id"
    default_conn_name = "redis_default"
    conn_type = "redis"
    hook_name = "Redis"

    def __init__(self, redis_conn_id: str = default_conn_name) -> None:
        """
        Prepare hook to connect to a Redis database.

        :param conn_id:     the name of the connection that has the parameters
                            we need to connect to Redis.
        """
        super().__init__()
        self.redis_conn_id = redis_conn_id
        self.redis = None
        self.host = None
        self.port = None
        self.username = None
        self.password = None
        self.db = None

    def get_conn(self):
        """Return a Redis connection."""
        conn = self.get_connection(self.redis_conn_id)
        self.host = conn.host
        self.port = conn.port
        self.username = conn.login
        self.password = (
            None if str(conn.password).lower() in ["none", "false", ""] else conn.password
        )
        self.db = conn.extra_dejson.get("db")

        # check for ssl parameters in conn.extra
        ssl_arg_names = [
            "ssl",
            "ssl_cert_reqs",
            "ssl_ca_certs",
            "ssl_keyfile",
            "ssl_certfile",
            "ssl_check_hostname",
        ]
        ssl_args = {
            name: val for name, val in conn.extra_dejson.items() if name in ssl_arg_names
        }

        # This logic is for backward compatibility only
        if (
            "ssl_cert_file" in conn.extra_dejson
            and "ssl_certfile" not in conn.extra_dejson
        ):
            warnings.warn(
                "Extra parameter `ssl_cert_file` deprecated and will be removed "
                "in a future release. Please use `ssl_certfile` instead.",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            ssl_args["ssl_certfile"] = conn.extra_dejson.get("ssl_cert_file")

        if not self.redis:
            self.log.debug(
                'Initializing redis object for conn_id "%s" on %s:%s:%s',
                self.redis_conn_id,
                self.host,
                self.port,
                self.db,
            )
            self.redis = Redis(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                db=self.db,
                **ssl_args,
            )

        return self.redis

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
            "db": IntegerField(
                lazy_gettext("DB"), widget=BS3TextFieldWidget(), default=0
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
            "ssl_check_hostname": BooleanField(
                lazy_gettext("Enable hostname check"), default=False
            ),
        }
