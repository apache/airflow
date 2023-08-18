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

import re
from unittest import mock

import pytest

from airflow.exceptions import AirflowConfigException
from airflow.utils import net
from airflow.utils.net import replace_items_from_url, replace_password_from_url
from tests.test_utils.config import conf_vars


def get_hostname():
    return "awesomehostname"


class TestGetHostname:
    @mock.patch("airflow.utils.net.getfqdn", return_value="first")
    @conf_vars({("core", "hostname_callable"): None})
    def test_get_hostname_unset(self, mock_getfqdn):
        assert "first" == net.get_hostname()

    @conf_vars({("core", "hostname_callable"): "tests.utils.test_net.get_hostname"})
    def test_get_hostname_set(self):
        assert "awesomehostname" == net.get_hostname()

    @conf_vars({("core", "hostname_callable"): "tests.utils.test_net"})
    def test_get_hostname_set_incorrect(self):
        with pytest.raises(TypeError):
            net.get_hostname()

    @conf_vars({("core", "hostname_callable"): "tests.utils.test_net.missing_func"})
    def test_get_hostname_set_missing(self):
        with pytest.raises(
            AirflowConfigException,
            match=re.escape(
                'The object could not be loaded. Please check "hostname_callable" key in "core" section. '
                'Current value: "tests.utils.test_net.missing_func"'
            ),
        ):
            net.get_hostname()


def replace_username_and_password(username, password) -> (str, str):
    return "USERNAME", "PASSWORD"


def replace_with_none(username, password) -> (str, str):
    return None, None


def replace_username(username, password) -> (str, str):
    return "USERNAME", password


def replace_password(username, password) -> (str, str):
    return username, "PASSWORD"


def no_replacement(username, password) -> (str, str):
    return username, password


class TestReplaceItemsFromUrl:
    @pytest.mark.parametrize(
        "before, after, function",
        [
            (
                "postgresql+psycopg2://postgres:airflow@postgres/airflow",
                "postgresql+psycopg2://USERNAME:PASSWORD@postgres/airflow",
                replace_username_and_password,
            ),
            (
                "postgresql+psycopg2://postgres:airflow@postgres/airflow",
                "postgresql+psycopg2://USERNAME:airflow@postgres/airflow",
                replace_username,
            ),
            (
                "postgresql+psycopg2://postgres:airflow@postgres/airflow",
                "postgresql+psycopg2://postgres:PASSWORD@postgres/airflow",
                replace_password,
            ),
            (
                "postgresql+psycopg2://postgres:airflow@postgres/airflow",
                "postgresql+psycopg2://postgres:airflow@postgres/airflow",
                no_replacement,
            ),
            (
                "postgresql+psycopg2://postgres@postgres/airflow",
                "postgresql+psycopg2://USERNAME:PASSWORD@postgres/airflow",
                replace_username_and_password,
            ),
            (
                "postgresql+psycopg2://:airflow@postgres/airflow",
                "postgresql+psycopg2://USERNAME:PASSWORD@postgres/airflow",
                replace_username_and_password,
            ),
            (
                "postgresql+psycopg2://postgres/airflow",
                "postgresql+psycopg2://USERNAME:PASSWORD@postgres/airflow",
                replace_username_and_password
            ),
            (
                None,
                None,
                replace_username_and_password
            ),
            (
                "postgresql+psycopg2://postgres:airflow@postgres/airflow",
                "postgresql+psycopg2://postgres/airflow",
                replace_with_none,
            ),
            (
                # test output of no username, password, or host - seems like an invalid case
                "postgresql+psycopg2://postgres:airflow@/airflow",
                # this looks like an invalid url
                "postgresql+psycopg2:/airflow",
                replace_with_none,
            ),
        ],
    )
    def test_should_replace_items_from_url(self, before, after, function):

        assert after == replace_items_from_url(before, function)


class TestReplacePasswordFromUrl:
    @pytest.mark.parametrize(
        "before, after",
        [
            (
                "postgresql+psycopg2://postgres:airflow@postgres/airflow",
                "postgresql+psycopg2://postgres:PASSWORD@postgres/airflow",
            ),
            (
                "postgresql+psycopg2://postgres@postgres/airflow",
                "postgresql+psycopg2://postgres:PASSWORD@postgres/airflow",
            ),
            (
                "postgresql+psycopg2://:airflow@postgres/airflow",
                "postgresql+psycopg2://:PASSWORD@postgres/airflow",
            ),
            (
                "postgresql+psycopg2://postgres/airflow",
                "postgresql+psycopg2://:PASSWORD@postgres/airflow",
            ),
            (
                None,
                None,
            ),
        ],
    )
    def test_should_replace_password_from_url(self, before, after):
        assert after == replace_password_from_url(before, "PASSWORD")
