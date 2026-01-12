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

import textwrap
from collections.abc import Generator
from unittest.mock import patch

import pytest

from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test

HEADERS_NONE = None
HEADERS_ANY = {"Accept": "*/*"}
HEADERS_JSON = {"Accept": "application/json"}
HEADERS_TEXT = {"Accept": "text/plain"}
HEADERS_INVALID = {"Accept": "invalid"}
HEADERS_JSON_UTF8 = {"Accept": "application/json; charset=utf-8"}
SECTION_CORE = "core"
SECTION_SMTP = "smtp"
SECTION_DATABASE = "database"
SECTION_NOT_EXIST = "not_exist_section"
OPTION_KEY_PARALLELISM = "parallelism"
OPTION_KEY_SMTP_HOST = "smtp_host"
OPTION_KEY_SMTP_MAIL_FROM = "smtp_mail_from"
OPTION_KEY_SQL_ALCHEMY_CONN = "sql_alchemy_conn"
OPTION_VALUE_PARALLELISM = "1024"
OPTION_VALUE_SMTP_HOST = "smtp.example.com"
OPTION_VALUE_SMTP_MAIL_FROM = "airflow@example.com"
OPTION_VALUE_SQL_ALCHEMY_CONN = "sqlite:///example.db"
OPTION_NOT_EXIST = "not_exist_option"
OPTION_VALUE_SENSITIVE_HIDDEN = "< hidden >"

MOCK_CONFIG_DICT = {
    SECTION_CORE: {
        OPTION_KEY_PARALLELISM: OPTION_VALUE_PARALLELISM,
    },
    SECTION_SMTP: {
        OPTION_KEY_SMTP_HOST: OPTION_VALUE_SMTP_HOST,
        OPTION_KEY_SMTP_MAIL_FROM: OPTION_VALUE_SMTP_MAIL_FROM,
    },
    SECTION_DATABASE: {
        OPTION_KEY_SQL_ALCHEMY_CONN: OPTION_VALUE_SENSITIVE_HIDDEN,
    },
}
MOCK_CONFIG_DICT_SENSITIVE_HIDDEN = {
    SECTION_CORE: {
        OPTION_KEY_PARALLELISM: OPTION_VALUE_PARALLELISM,
    },
    SECTION_SMTP: {
        OPTION_KEY_SMTP_HOST: OPTION_VALUE_SMTP_HOST,
        OPTION_KEY_SMTP_MAIL_FROM: OPTION_VALUE_SMTP_MAIL_FROM,
    },
    SECTION_DATABASE: {
        OPTION_KEY_SQL_ALCHEMY_CONN: OPTION_VALUE_SENSITIVE_HIDDEN,
    },
}
MOCK_CONFIG_OVERRIDE = {
    (SECTION_CORE, OPTION_KEY_PARALLELISM): OPTION_VALUE_PARALLELISM,
    (SECTION_SMTP, OPTION_KEY_SMTP_HOST): OPTION_VALUE_SMTP_HOST,
    (SECTION_SMTP, OPTION_KEY_SMTP_MAIL_FROM): OPTION_VALUE_SMTP_MAIL_FROM,
}

AIRFLOW_CONFIG_ENABLE_EXPOSE_CONFIG = {("api", "expose_config"): "True"}
AIRFLOW_CONFIG_DISABLE_EXPOSE_CONFIG = {("api", "expose_config"): "False"}
AIRFLOW_CONFIG_NON_SENSITIVE_ONLY_CONFIG = {("api", "expose_config"): "non-sensitive-only"}
FORBIDDEN_RESPONSE = {
    "detail": "Your Airflow administrator chose not to expose the configuration, most likely for security reasons."
}

GET_CONFIG_ALL_JSON_RESPONSE = {
    "sections": [
        {
            "name": SECTION_CORE,
            "options": [
                {"key": OPTION_KEY_PARALLELISM, "value": OPTION_VALUE_PARALLELISM},
            ],
        },
        {
            "name": SECTION_SMTP,
            "options": [
                {"key": OPTION_KEY_SMTP_HOST, "value": OPTION_VALUE_SMTP_HOST},
                {"key": OPTION_KEY_SMTP_MAIL_FROM, "value": OPTION_VALUE_SMTP_MAIL_FROM},
            ],
        },
        {
            "name": SECTION_DATABASE,
            "options": [
                {"key": OPTION_KEY_SQL_ALCHEMY_CONN, "value": OPTION_VALUE_SENSITIVE_HIDDEN},
            ],
        },
    ],
}
GET_CONFIG_NON_SENSITIVE_ONLY_JSON_RESPONSE = {
    "sections": [
        {
            "name": SECTION_CORE,
            "options": [
                {"key": OPTION_KEY_PARALLELISM, "value": OPTION_VALUE_PARALLELISM},
            ],
        },
        {
            "name": SECTION_SMTP,
            "options": [
                {"key": OPTION_KEY_SMTP_HOST, "value": OPTION_VALUE_SMTP_HOST},
                {"key": OPTION_KEY_SMTP_MAIL_FROM, "value": OPTION_VALUE_SMTP_MAIL_FROM},
            ],
        },
        {
            "name": SECTION_DATABASE,
            "options": [
                {"key": OPTION_KEY_SQL_ALCHEMY_CONN, "value": OPTION_VALUE_SENSITIVE_HIDDEN},
            ],
        },
    ],
}
GET_CONFIG_VALUE_CORE_PARALLELISM_JSON_RESPONSE = {
    "sections": [
        {
            "name": SECTION_CORE,
            "options": [
                {"key": OPTION_KEY_PARALLELISM, "value": OPTION_VALUE_PARALLELISM},
            ],
        },
    ],
}
GET_CONFIG_VALUE_NON_SENSITIVE_ONLY_DATABASE_SQL_ALCHEMY_CONN_JSON_RESPONSE = {
    "sections": [
        {
            "name": SECTION_DATABASE,
            "options": [
                {"key": OPTION_KEY_SQL_ALCHEMY_CONN, "value": OPTION_VALUE_SENSITIVE_HIDDEN},
            ],
        },
    ],
}


class TestConfigEndpoint:
    def _validate_response(self, headers, expected_response, expected_status_code, response):
        assert response.status_code == expected_status_code
        if headers == HEADERS_TEXT:
            assert response.text == expected_response
        else:
            assert response.json() == expected_response

    @pytest.fixture(autouse=True)
    def setup(self) -> Generator[None, None, None]:
        with conf_vars(AIRFLOW_CONFIG_ENABLE_EXPOSE_CONFIG | MOCK_CONFIG_OVERRIDE):
            # since the endpoint calls `conf_dict.clear()` to remove extra keys,
            # use `new` instead of `return_value` to avoid side effects
            def _mock_conf_as_dict(display_sensitive: bool, **_):
                return (
                    MOCK_CONFIG_DICT_SENSITIVE_HIDDEN.copy()
                    if not display_sensitive
                    else MOCK_CONFIG_DICT.copy()
                )

            with patch(
                "airflow.api_fastapi.core_api.routes.public.config.conf.as_dict",
                new=_mock_conf_as_dict,
            ):
                yield


class TestGetConfig(TestConfigEndpoint):
    @pytest.mark.parametrize(
        ("section", "headers", "expected_status_code", "expected_response"),
        [
            (
                None,
                HEADERS_JSON,
                200,
                GET_CONFIG_ALL_JSON_RESPONSE,
            ),
            (None, HEADERS_JSON_UTF8, 200, GET_CONFIG_ALL_JSON_RESPONSE),
            (None, HEADERS_ANY, 200, GET_CONFIG_ALL_JSON_RESPONSE),
            (None, HEADERS_NONE, 200, GET_CONFIG_ALL_JSON_RESPONSE),
            (
                None,
                HEADERS_TEXT,
                200,
                textwrap.dedent(
                    f"""\
                    [{SECTION_CORE}]
                    {OPTION_KEY_PARALLELISM} = {OPTION_VALUE_PARALLELISM}

                    [{SECTION_SMTP}]
                    {OPTION_KEY_SMTP_HOST} = {OPTION_VALUE_SMTP_HOST}
                    {OPTION_KEY_SMTP_MAIL_FROM} = {OPTION_VALUE_SMTP_MAIL_FROM}

                    [{SECTION_DATABASE}]
                    {OPTION_KEY_SQL_ALCHEMY_CONN} = {OPTION_VALUE_SENSITIVE_HIDDEN}
                    """
                ),
            ),
            (
                None,
                HEADERS_INVALID,
                406,
                {"detail": "Only application/json or text/plain is supported"},
            ),
            (
                SECTION_CORE,
                HEADERS_JSON,
                200,
                {
                    "sections": [
                        {
                            "name": SECTION_CORE,
                            "options": [
                                {"key": OPTION_KEY_PARALLELISM, "value": OPTION_VALUE_PARALLELISM},
                            ],
                        },
                    ],
                },
            ),
            (
                SECTION_SMTP,
                HEADERS_TEXT,
                200,
                textwrap.dedent(
                    f"""\
                    [{SECTION_SMTP}]
                    {OPTION_KEY_SMTP_HOST} = {OPTION_VALUE_SMTP_HOST}
                    {OPTION_KEY_SMTP_MAIL_FROM} = {OPTION_VALUE_SMTP_MAIL_FROM}
                    """
                ),
            ),
            (
                SECTION_DATABASE,
                HEADERS_JSON,
                200,
                {
                    "sections": [
                        {
                            "name": SECTION_DATABASE,
                            "options": [
                                {"key": OPTION_KEY_SQL_ALCHEMY_CONN, "value": OPTION_VALUE_SENSITIVE_HIDDEN},
                            ],
                        },
                    ],
                },
            ),
            (None, HEADERS_JSON, 403, FORBIDDEN_RESPONSE),
            (SECTION_CORE, HEADERS_JSON, 403, FORBIDDEN_RESPONSE),
            (SECTION_NOT_EXIST, HEADERS_JSON, 404, {"detail": f"Section {SECTION_NOT_EXIST} not found."}),
        ],
    )
    def test_get_config(self, test_client, section, headers, expected_status_code, expected_response):
        query_params = {"section": section} if section else None
        if expected_status_code == 403:
            with conf_vars(AIRFLOW_CONFIG_DISABLE_EXPOSE_CONFIG):
                response = test_client.get("/config", headers=headers, params=query_params)
        else:
            response = test_client.get("/config", headers=headers, params=query_params)
        self._validate_response(headers, expected_response, expected_status_code, response)

    @pytest.mark.parametrize(
        ("headers", "expected_status_code", "expected_response"),
        [
            (HEADERS_JSON, 200, GET_CONFIG_NON_SENSITIVE_ONLY_JSON_RESPONSE),
            (HEADERS_JSON_UTF8, 200, GET_CONFIG_NON_SENSITIVE_ONLY_JSON_RESPONSE),
            (HEADERS_ANY, 200, GET_CONFIG_NON_SENSITIVE_ONLY_JSON_RESPONSE),
            (HEADERS_NONE, 200, GET_CONFIG_NON_SENSITIVE_ONLY_JSON_RESPONSE),
            (
                HEADERS_TEXT,
                200,
                textwrap.dedent(
                    f"""\
                    [{SECTION_CORE}]
                    {OPTION_KEY_PARALLELISM} = {OPTION_VALUE_PARALLELISM}

                    [{SECTION_SMTP}]
                    {OPTION_KEY_SMTP_HOST} = {OPTION_VALUE_SMTP_HOST}
                    {OPTION_KEY_SMTP_MAIL_FROM} = {OPTION_VALUE_SMTP_MAIL_FROM}

                    [{SECTION_DATABASE}]
                    {OPTION_KEY_SQL_ALCHEMY_CONN} = {OPTION_VALUE_SENSITIVE_HIDDEN}
                    """
                ),
            ),
        ],
    )
    def test_get_config_non_sensitive_only(
        self, test_client, headers, expected_status_code, expected_response
    ):
        with conf_vars(AIRFLOW_CONFIG_NON_SENSITIVE_ONLY_CONFIG):
            response = test_client.get("/config", headers=headers)
        self._validate_response(headers, expected_response, expected_status_code, response)

    def test_get_config_should_response_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/config")
        assert response.status_code == 401

    def test_get_config_should_response_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/config")
        assert response.status_code == 403


class TestGetConfigValue(TestConfigEndpoint):
    @pytest.mark.parametrize(
        ("section", "option", "headers", "expected_status_code", "expected_response"),
        [
            (
                SECTION_CORE,
                OPTION_KEY_PARALLELISM,
                HEADERS_JSON,
                200,
                GET_CONFIG_VALUE_CORE_PARALLELISM_JSON_RESPONSE,
            ),
            (
                SECTION_CORE,
                OPTION_KEY_PARALLELISM,
                HEADERS_JSON_UTF8,
                200,
                GET_CONFIG_VALUE_CORE_PARALLELISM_JSON_RESPONSE,
            ),
            (
                SECTION_CORE,
                OPTION_KEY_PARALLELISM,
                HEADERS_ANY,
                200,
                GET_CONFIG_VALUE_CORE_PARALLELISM_JSON_RESPONSE,
            ),
            (
                SECTION_CORE,
                OPTION_KEY_PARALLELISM,
                HEADERS_NONE,
                200,
                GET_CONFIG_VALUE_CORE_PARALLELISM_JSON_RESPONSE,
            ),
            (
                SECTION_SMTP,
                OPTION_KEY_SMTP_HOST,
                HEADERS_TEXT,
                200,
                textwrap.dedent(
                    f"""\
                    [{SECTION_SMTP}]
                    {OPTION_KEY_SMTP_HOST} = {OPTION_VALUE_SMTP_HOST}
                    """
                ),
            ),
            (
                SECTION_SMTP,
                OPTION_KEY_SMTP_MAIL_FROM,
                HEADERS_JSON,
                200,
                {
                    "sections": [
                        {
                            "name": SECTION_SMTP,
                            "options": [
                                {"key": OPTION_KEY_SMTP_MAIL_FROM, "value": OPTION_VALUE_SMTP_MAIL_FROM},
                            ],
                        },
                    ],
                },
            ),
            (
                SECTION_DATABASE,
                OPTION_KEY_SQL_ALCHEMY_CONN,
                HEADERS_JSON,
                200,
                {
                    "sections": [
                        {
                            "name": SECTION_DATABASE,
                            "options": [
                                {"key": OPTION_KEY_SQL_ALCHEMY_CONN, "value": OPTION_VALUE_SENSITIVE_HIDDEN},
                            ],
                        },
                    ],
                },
            ),
            (
                SECTION_DATABASE,
                OPTION_KEY_SQL_ALCHEMY_CONN,
                HEADERS_TEXT,
                200,
                textwrap.dedent(
                    f"""\
                    [{SECTION_DATABASE}]
                    {OPTION_KEY_SQL_ALCHEMY_CONN} = {OPTION_VALUE_SENSITIVE_HIDDEN}
                    """
                ),
            ),
            (SECTION_CORE, OPTION_KEY_PARALLELISM, HEADERS_JSON, 403, FORBIDDEN_RESPONSE),
            (
                SECTION_NOT_EXIST,
                OPTION_KEY_PARALLELISM,
                HEADERS_JSON,
                404,
                {"detail": f"Option [{SECTION_NOT_EXIST}/{OPTION_KEY_PARALLELISM}] not found."},
            ),
            (
                SECTION_CORE,
                OPTION_NOT_EXIST,
                HEADERS_JSON,
                404,
                {"detail": f"Option [{SECTION_CORE}/{OPTION_NOT_EXIST}] not found."},
            ),
        ],
    )
    def test_get_config_value(
        self, test_client, section, option, headers, expected_status_code, expected_response
    ):
        if expected_status_code == 403:
            with conf_vars(AIRFLOW_CONFIG_DISABLE_EXPOSE_CONFIG):
                response = test_client.get(f"/config/section/{section}/option/{option}", headers=headers)
        else:
            response = test_client.get(f"/config/section/{section}/option/{option}", headers=headers)
        self._validate_response(headers, expected_response, expected_status_code, response)

    @pytest.mark.parametrize(
        ("section", "option", "headers", "expected_status_code", "expected_response"),
        [
            (
                SECTION_DATABASE,
                OPTION_KEY_SQL_ALCHEMY_CONN,
                HEADERS_JSON,
                200,
                GET_CONFIG_VALUE_NON_SENSITIVE_ONLY_DATABASE_SQL_ALCHEMY_CONN_JSON_RESPONSE,
            ),
            (
                SECTION_DATABASE,
                OPTION_KEY_SQL_ALCHEMY_CONN,
                HEADERS_JSON_UTF8,
                200,
                GET_CONFIG_VALUE_NON_SENSITIVE_ONLY_DATABASE_SQL_ALCHEMY_CONN_JSON_RESPONSE,
            ),
            (
                SECTION_DATABASE,
                OPTION_KEY_SQL_ALCHEMY_CONN,
                HEADERS_ANY,
                200,
                GET_CONFIG_VALUE_NON_SENSITIVE_ONLY_DATABASE_SQL_ALCHEMY_CONN_JSON_RESPONSE,
            ),
            (
                SECTION_DATABASE,
                OPTION_KEY_SQL_ALCHEMY_CONN,
                HEADERS_NONE,
                200,
                GET_CONFIG_VALUE_NON_SENSITIVE_ONLY_DATABASE_SQL_ALCHEMY_CONN_JSON_RESPONSE,
            ),
            (
                SECTION_DATABASE,
                OPTION_KEY_SQL_ALCHEMY_CONN,
                HEADERS_TEXT,
                200,
                textwrap.dedent(
                    f"""\
                    [{SECTION_DATABASE}]
                    {OPTION_KEY_SQL_ALCHEMY_CONN} = {OPTION_VALUE_SENSITIVE_HIDDEN}
                    """
                ),
            ),
        ],
    )
    def test_get_config_value_non_sensitive_only(
        self, test_client, section, option, headers, expected_status_code, expected_response
    ):
        with conf_vars(AIRFLOW_CONFIG_NON_SENSITIVE_ONLY_CONFIG):
            response = test_client.get(f"/config/section/{section}/option/{option}", headers=headers)
        self._validate_response(headers, expected_response, expected_status_code, response)

    def test_get_config_value_should_response_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get(
            f"/config/section/{SECTION_DATABASE}/option/{OPTION_KEY_SQL_ALCHEMY_CONN}"
        )
        assert response.status_code == 401

    def test_get_config_value_should_response_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get(
            f"/config/section/{SECTION_DATABASE}/option/{OPTION_KEY_SQL_ALCHEMY_CONN}"
        )
        assert response.status_code == 403
