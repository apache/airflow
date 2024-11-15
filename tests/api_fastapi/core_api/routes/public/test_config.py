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
from typing import Generator
from unittest.mock import patch

import pytest

from tests_common.test_utils.config import conf_vars

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
OPTION_VALUE_SQL_ALCHEMY_CONN = "mock_sql_alchemy_conn"
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
        OPTION_KEY_SQL_ALCHEMY_CONN: OPTION_VALUE_SQL_ALCHEMY_CONN,
    },
}
MOCK_CONFIG_OVERRIDE = {
    (SECTION_CORE, OPTION_KEY_PARALLELISM): OPTION_VALUE_PARALLELISM,
    (SECTION_SMTP, OPTION_KEY_SMTP_HOST): OPTION_VALUE_SMTP_HOST,
    (SECTION_SMTP, OPTION_KEY_SMTP_MAIL_FROM): OPTION_VALUE_SMTP_MAIL_FROM,
}

AIRFLOW_CONFIG_ENABLE_EXPOSE_CONFIG = {("webserver", "expose_config"): "True"}
AIRFLOW_CONFIG_DISABLE_EXPOSE_CONFIG = {("webserver", "expose_config"): "False"}
FORBIDDEN_RESPONSE = {
    "detail": "Your Airflow administrator chose not to expose the configuration, most likely for security reasons."
}


class TestConfigEndpoint:
    @pytest.fixture(autouse=True)
    def setup(self) -> Generator[None, None, None]:
        with conf_vars(AIRFLOW_CONFIG_ENABLE_EXPOSE_CONFIG | MOCK_CONFIG_OVERRIDE):
            # since the endpoint calls `conf_dict.clear()` to remove extra keys,
            # use `new` instead of `return_value` to avoid side effects
            with patch(
                "airflow.api_fastapi.core_api.routes.public.config.conf.as_dict",
                new=lambda **_: MOCK_CONFIG_DICT.copy(),
            ):
                yield


class TestGetConfig(TestConfigEndpoint):
    @pytest.mark.parametrize(
        "section, headers, expected_status_code, expected_response",
        [
            (
                None,
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
                                {"key": OPTION_KEY_SQL_ALCHEMY_CONN, "value": OPTION_VALUE_SQL_ALCHEMY_CONN},
                            ],
                        },
                    ],
                },
            ),
            (
                None,
                HEADERS_JSON_UTF8,
                200,
                {
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
                                {"key": OPTION_KEY_SQL_ALCHEMY_CONN, "value": OPTION_VALUE_SQL_ALCHEMY_CONN},
                            ],
                        },
                    ],
                },
            ),
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
                    {OPTION_KEY_SQL_ALCHEMY_CONN} = {OPTION_VALUE_SQL_ALCHEMY_CONN}
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
                                {"key": OPTION_KEY_SQL_ALCHEMY_CONN, "value": OPTION_VALUE_SQL_ALCHEMY_CONN},
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
                response = test_client.get("/public/config/", headers=headers, params=query_params)
        else:
            response = test_client.get("/public/config/", headers=headers, params=query_params)
        assert response.status_code == expected_status_code
        if headers == HEADERS_TEXT:
            assert response.text == expected_response
        else:
            assert response.json() == expected_response


class TestGetConfigValue(TestConfigEndpoint):
    @pytest.mark.parametrize(
        "section, option, headers, expected_status_code, expected_response",
        [
            (
                SECTION_CORE,
                OPTION_KEY_PARALLELISM,
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
                response = test_client.get(
                    f"/public/config/section/{section}/option/{option}", headers=headers
                )
        else:
            response = test_client.get(f"/public/config/section/{section}/option/{option}", headers=headers)
        assert response.status_code == expected_status_code
        if response.status_code != 200 or headers == HEADERS_JSON:
            assert response.json() == expected_response
        else:
            assert response.text == expected_response
