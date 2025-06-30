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
from __future__ import annotations

from unittest import mock

import pytest

from airflow.models import Connection
from airflow.providers.redis.hooks.redis import DEFAULT_SSL_CERT_REQS, RedisHook


class TestRedisHook:
    @pytest.mark.db_test
    def test_get_conn(self):
        hook = RedisHook(redis_conn_id="redis_default")
        assert hook.redis is None

        assert hook.host is None, "host initialised as None."
        assert hook.port is None, "port initialised as None."
        assert hook.password is None, "password initialised as None."
        assert hook.db is None, "db initialised as None."
        assert hook.get_conn() is hook.get_conn(), "Connection initialized only if None."

    @mock.patch("airflow.providers.redis.hooks.redis.Redis")
    @mock.patch(
        "airflow.providers.redis.hooks.redis.RedisHook.get_connection",
    )
    def test_get_conn_with_extra_config(self, mock_get_connection, mock_redis):
        connection = Connection(
            login="user",
            password="password",
            host="remote_host",
            port=1234,
        )
        connection.set_extra(
            """{
                "db": 2,
                "ssl": true,
                "ssl_cert_reqs": "required",
                "ssl_ca_certs": "/path/to/custom/ca-cert",
                "ssl_keyfile": "/path/to/key-file",
                "ssl_certfile": "/path/to/cert-file",
                "ssl_check_hostname": true
            }"""
        )
        mock_get_connection.return_value = connection
        hook = RedisHook()

        hook.get_conn()
        mock_redis.assert_called_once_with(
            host=connection.host,
            username=connection.login,
            password=connection.password,
            port=connection.port,
            db=connection.extra_dejson["db"],
            ssl=connection.extra_dejson["ssl"],
            ssl_cert_reqs=connection.extra_dejson["ssl_cert_reqs"],
            ssl_ca_certs=connection.extra_dejson["ssl_ca_certs"],
            ssl_keyfile=connection.extra_dejson["ssl_keyfile"],
            ssl_certfile=connection.extra_dejson["ssl_certfile"],
            ssl_check_hostname=connection.extra_dejson["ssl_check_hostname"],
        )

    @pytest.mark.db_test
    def test_get_conn_password_stays_none(self):
        hook = RedisHook(redis_conn_id="redis_default")
        hook.get_conn()
        assert hook.password is None

    def test_get_ui_field_behaviour(self):
        expected = {"hidden_fields": ["schema", "extra"], "relabeling": {}}
        assert RedisHook.get_ui_field_behaviour() == expected

    @mock.patch("wtforms.validators.any_of")
    @mock.patch("wtforms.validators.Optional")
    @mock.patch("flask_appbuilder.fieldwidgets.BS3TextFieldWidget")
    @mock.patch("wtforms.StringField")
    @mock.patch("wtforms.BooleanField")
    @mock.patch("wtforms.IntegerField")
    @mock.patch("flask_babel.lazy_gettext")
    def test_get_connection_form_widgets(
        self,
        mock_lazy_gettext,
        mock_int_field,
        mock_bool_field,
        mock_string_field,
        mock_bs3,
        mock_optional,
        mock_any_of,
    ):
        mock_lazy_gettext.side_effect = lambda x: x
        mock_int_field.return_value = "INT_OBJ"
        mock_bool_field.return_value = "BOOL_OBJ"
        mock_string_field.return_value = "STR_OBJ"
        mock_any_of.return_value = "ANYOF_VALIDATOR"
        mock_optional.return_value = "OPTIONAL_VALIDATOR"

        widgets = RedisHook.get_connection_form_widgets()

        expected_keys = {
            "db",
            "ssl",
            "ssl_cert_reqs",
            "ssl_ca_certs",
            "ssl_keyfile",
            "ssl_certfile",
            "ssl_check_hostname",
        }
        assert set(widgets) == expected_keys

        assert widgets["db"] == "INT_OBJ"
        assert widgets["ssl"] == "BOOL_OBJ"
        assert widgets["ssl_check_hostname"] == "BOOL_OBJ"
        for key in ("ssl_cert_reqs", "ssl_ca_certs", "ssl_keyfile", "ssl_certfile"):
            assert widgets[key] == "STR_OBJ"

        mock_int_field.assert_called_once_with(
            "DB",
            widget=mock_bs3(),
            default=0,
        )

        assert mock_bool_field.call_count == 2
        mock_bool_field.assert_any_call("Enable SSL", default=False)
        mock_bool_field.assert_any_call("Enable hostname check", default=False)

        assert mock_string_field.call_count == 4

        mock_string_field.assert_any_call(
            "SSL verify mode",
            validators=["ANYOF_VALIDATOR"],
            widget=mock_bs3(),
            description=mock.ANY,
            default=DEFAULT_SSL_CERT_REQS,
        )

        calls = [
            c
            for c in mock_string_field.call_args_list
            if c.kwargs.get("validators") == ["OPTIONAL_VALIDATOR"]
        ]
        assert len(calls) == 3
