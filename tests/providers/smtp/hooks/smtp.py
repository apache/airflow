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

import json
import smtplib
from unittest.mock import Mock, patch

import pytest

from airflow.models import Connection
from airflow.providers.smtp.hooks.smtp import SmtpHook
from airflow.utils import db

smtplib_string = "airflow.providers.smtp.hooks.smtp.smtplib"

TEST_EMAILS = ["test1@example.com", "test2@example.com"]


def _create_fake_smtp(mock_smtplib, use_ssl=True):
    if use_ssl:
        mock_conn = Mock(spec=smtplib.SMTP_SSL)
        mock_smtplib.SMTP_SSL.return_value = mock_conn
    else:
        mock_conn = Mock(spec=smtplib.SMTP)
        mock_smtplib.SMTP.return_value = mock_conn

    mock_conn.close.return_value = ("OK", [])

    return mock_conn


class TestSmtpHook:
    def setup_method(self):
        db.merge_conn(
            Connection(
                conn_id="smtp_default",
                conn_type="smtp",
                host="smtp_server_address",
                login="smtp_user",
                password="smtp_password",
                port=465,
            )
        )
        db.merge_conn(
            Connection(
                conn_id="smtp_nonssl",
                conn_type="smtp",
                host="smtp_server_address",
                login="smtp_user",
                password="smtp_password",
                port=587,
                extra=json.dumps(dict(disable_ssl=True)),
            )
        )

    @patch(smtplib_string)
    def test_connect_and_disconnect(self, mock_smtplib):
        mock_conn = _create_fake_smtp(mock_smtplib)

        with SmtpHook():
            pass

        mock_smtplib.SMTP_SSL.assert_called_once_with(host="smtp_server_address", port=465, timeout=30)
        mock_conn.login.assert_called_once_with("smtp_user", "smtp_password")
        assert mock_conn.close.call_count == 1

    @patch(smtplib_string)
    def test_connect_and_disconnect_via_nonssl(self, mock_smtplib):
        mock_conn = _create_fake_smtp(mock_smtplib, use_ssl=False)

        with SmtpHook(smtp_conn_id="smtp_nonssl"):
            pass

        mock_smtplib.SMTP.assert_called_once_with(host="smtp_server_address", port=587, timeout=30)
        mock_conn.login.assert_called_once_with("smtp_user", "smtp_password")
        assert mock_conn.close.call_count == 1

    @patch(smtplib_string)
    def test_get_email_address_single_email(self, mock_smtplib):
        with SmtpHook() as smtp_hook:
            assert smtp_hook._get_email_address_list("test1@example.com") == ["test1@example.com"]

    @patch(smtplib_string)
    def test_get_email_address_comma_sep_string(self, mock_smtplib):
        with SmtpHook() as smtp_hook:
            assert smtp_hook._get_email_address_list("test1@example.com, test2@example.com") == TEST_EMAILS

    @patch(smtplib_string)
    def test_get_email_address_colon_sep_string(self, mock_smtplib):
        with SmtpHook() as smtp_hook:
            assert smtp_hook._get_email_address_list("test1@example.com; test2@example.com") == TEST_EMAILS

    @patch(smtplib_string)
    def test_get_email_address_list(self, mock_smtplib):
        with SmtpHook() as smtp_hook:
            assert (
                smtp_hook._get_email_address_list(["test1@example.com", "test2@example.com"]) == TEST_EMAILS
            )

    @patch(smtplib_string)
    def test_get_email_address_tuple(self, mock_smtplib):
        with SmtpHook() as smtp_hook:
            assert (
                smtp_hook._get_email_address_list(("test1@example.com", "test2@example.com")) == TEST_EMAILS
            )

    @patch(smtplib_string)
    def test_get_email_address_invalid_type(self, mock_smtplib):
        with pytest.raises(TypeError):
            with SmtpHook() as smtp_hook:
                smtp_hook._get_email_address_list(1)

    @patch(smtplib_string)
    def test_get_email_address_invalid_type_in_iterable(self, mock_smtplib):
        with pytest.raises(TypeError):
            with SmtpHook() as smtp_hook:
                smtp_hook._get_email_address_list(["test1@example.com", 2])
