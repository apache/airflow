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

import imaplib
import json
from unittest.mock import Mock, mock_open, patch

import pytest

from airflow.models import Connection
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.imap.hooks.imap import ImapHook

from tests_common.test_utils.config import conf_vars

imaplib_string = "airflow.providers.imap.hooks.imap.imaplib"
open_string = "airflow.providers.imap.hooks.imap.open"


def _create_fake_imap(mock_imaplib, with_mail=False, attachment_name="test1.csv", use_ssl=True):
    if use_ssl:
        mock_conn = Mock(spec=imaplib.IMAP4_SSL)
        mock_imaplib.IMAP4_SSL.return_value = mock_conn
    else:
        mock_conn = Mock(spec=imaplib.IMAP4)
        mock_imaplib.IMAP4.return_value = mock_conn

    mock_conn.login.return_value = ("OK", [])

    if with_mail:
        mock_conn.select.return_value = ("OK", [])
        mock_conn.search.return_value = ("OK", [b"1"])
        mail_string = (
            f"Content-Type: multipart/mixed; "
            f"boundary=123\r\n--123\r\n"
            f"Content-Disposition: attachment; "
            f'filename="{attachment_name}";'
            f"Content-Transfer-Encoding: base64\r\nSWQsTmFtZQoxLEZlbGl4\r\n--123--"
        )
        mock_conn.fetch.return_value = ("OK", [(b"", mail_string.encode("utf-8"))])
        mock_conn.close.return_value = ("OK", [])

    mock_conn.logout.return_value = ("OK", [])

    return mock_conn


class TestImapHook:
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id="imap_default",
                conn_type="imap",
                host="imap_server_address",
                login="imap_user",
                password="imap_password",
                port=1993,
            )
        )
        create_connection_without_db(
            Connection(
                conn_id="imap_nonssl",
                conn_type="imap",
                host="imap_server_address",
                login="imap_user",
                password="imap_password",
                port=1143,
                extra=json.dumps(dict(use_ssl=False)),
            )
        )

    @patch(imaplib_string)
    @patch("ssl.create_default_context")
    def test_connect_and_disconnect(self, create_default_context, mock_imaplib):
        mock_conn = _create_fake_imap(mock_imaplib)

        with ImapHook():
            pass

        assert create_default_context.called
        mock_imaplib.IMAP4_SSL.assert_called_once_with(
            "imap_server_address", 1993, ssl_context=create_default_context.return_value
        )
        mock_conn.login.assert_called_once_with("imap_user", "imap_password")
        assert mock_conn.logout.call_count == 1

    @patch(imaplib_string)
    @patch("ssl.create_default_context")
    def test_connect_and_disconnect_imap_ssl_context_none(self, create_default_context, mock_imaplib):
        mock_conn = _create_fake_imap(mock_imaplib)

        with conf_vars({("imap", "ssl_context"): "none"}):
            with ImapHook():
                pass

        assert not create_default_context.called
        mock_imaplib.IMAP4_SSL.assert_called_once_with("imap_server_address", 1993, ssl_context=None)
        mock_conn.login.assert_called_once_with("imap_user", "imap_password")
        assert mock_conn.logout.call_count == 1

    @patch(imaplib_string)
    @patch("ssl.create_default_context")
    def test_connect_and_disconnect_imap_ssl_context_from_extra(
        self, create_default_context, mock_imaplib, create_connection_without_db
    ):
        mock_conn = _create_fake_imap(mock_imaplib)
        create_connection_without_db(
            Connection(
                conn_id="imap_ssl_context_from_extra",
                conn_type="imap",
                host="imap_server_address",
                login="imap_user",
                password="imap_password",
                port=1993,
                extra=json.dumps(dict(use_ssl=True, ssl_context="default")),
            )
        )

        with conf_vars({("imap", "ssl_context"): "none"}):
            with ImapHook(imap_conn_id="imap_ssl_context_from_extra"):
                pass

        assert create_default_context.called
        mock_imaplib.IMAP4_SSL.assert_called_once_with(
            "imap_server_address", 1993, ssl_context=create_default_context.return_value
        )
        mock_conn.login.assert_called_once_with("imap_user", "imap_password")
        assert mock_conn.logout.call_count == 1

    @patch(imaplib_string)
    @patch("ssl.create_default_context")
    def test_connect_and_disconnect_imap_ssl_context_default(self, create_default_context, mock_imaplib):
        mock_conn = _create_fake_imap(mock_imaplib)

        with conf_vars({("imap", "ssl_context"): "default"}):
            with ImapHook():
                pass

        assert create_default_context.called
        mock_imaplib.IMAP4_SSL.assert_called_once_with(
            "imap_server_address", 1993, ssl_context=create_default_context.return_value
        )
        mock_conn.login.assert_called_once_with("imap_user", "imap_password")
        assert mock_conn.logout.call_count == 1

    @patch(imaplib_string)
    @patch("ssl.create_default_context")
    def test_connect_and_disconnect_email_ssl_context_none(self, create_default_context, mock_imaplib):
        mock_conn = _create_fake_imap(mock_imaplib)

        with conf_vars({("email", "ssl_context"): "none"}):
            with ImapHook():
                pass

        assert not create_default_context.called
        mock_imaplib.IMAP4_SSL.assert_called_once_with("imap_server_address", 1993, ssl_context=None)
        mock_conn.login.assert_called_once_with("imap_user", "imap_password")
        assert mock_conn.logout.call_count == 1

    @patch(imaplib_string)
    @patch("ssl.create_default_context")
    def test_connect_and_disconnect_imap_ssl_context_override(self, create_default_context, mock_imaplib):
        mock_conn = _create_fake_imap(mock_imaplib)

        with conf_vars({("email", "ssl_context"): "none", ("imap", "ssl_context"): "default"}):
            with ImapHook():
                pass

        assert create_default_context.called
        mock_imaplib.IMAP4_SSL.assert_called_once_with(
            "imap_server_address", 1993, ssl_context=create_default_context.return_value
        )
        mock_conn.login.assert_called_once_with("imap_user", "imap_password")
        assert mock_conn.logout.call_count == 1

    @patch(imaplib_string)
    def test_connect_and_disconnect_via_nonssl(self, mock_imaplib):
        mock_conn = _create_fake_imap(mock_imaplib, use_ssl=False)

        with ImapHook(imap_conn_id="imap_nonssl"):
            pass

        mock_imaplib.IMAP4.assert_called_once_with("imap_server_address", 1143)
        mock_conn.login.assert_called_once_with("imap_user", "imap_password")
        assert mock_conn.logout.call_count == 1

    @patch(imaplib_string)
    def test_has_mail_attachment_found(self, mock_imaplib):
        _create_fake_imap(mock_imaplib, with_mail=True)

        with ImapHook() as imap_hook:
            has_attachment_in_inbox = imap_hook.has_mail_attachment("test1.csv")

        assert has_attachment_in_inbox

    @patch(imaplib_string)
    def test_has_mail_attachment_not_found(self, mock_imaplib):
        _create_fake_imap(mock_imaplib, with_mail=True)

        with ImapHook() as imap_hook:
            has_attachment_in_inbox = imap_hook.has_mail_attachment("test1.txt")

        assert not has_attachment_in_inbox

    @patch(imaplib_string)
    def test_has_mail_attachment_with_regex_found(self, mock_imaplib):
        _create_fake_imap(mock_imaplib, with_mail=True)

        with ImapHook() as imap_hook:
            has_attachment_in_inbox = imap_hook.has_mail_attachment(name=r"test(\d+).csv", check_regex=True)

        assert has_attachment_in_inbox

    @patch(imaplib_string)
    def test_has_mail_attachment_with_regex_not_found(self, mock_imaplib):
        _create_fake_imap(mock_imaplib, with_mail=True)

        with ImapHook() as imap_hook:
            has_attachment_in_inbox = imap_hook.has_mail_attachment(name=r"test_(\d+).csv", check_regex=True)

        assert not has_attachment_in_inbox

    @patch(imaplib_string)
    def test_has_mail_attachment_with_mail_filter(self, mock_imaplib):
        _create_fake_imap(mock_imaplib, with_mail=True)
        mail_filter = '(SINCE "01-Jan-2019")'

        with ImapHook() as imap_hook:
            imap_hook.has_mail_attachment(name="test1.csv", mail_filter=mail_filter)

        mock_imaplib.IMAP4_SSL.return_value.search.assert_called_once_with(None, mail_filter)

    @patch(imaplib_string)
    def test_retrieve_mail_attachments_found(self, mock_imaplib):
        _create_fake_imap(mock_imaplib, with_mail=True)

        with ImapHook() as imap_hook:
            attachments_in_inbox = imap_hook.retrieve_mail_attachments("test1.csv")

        assert attachments_in_inbox == [("test1.csv", b"SWQsTmFtZQoxLEZlbGl4")]

    @patch(imaplib_string)
    def test_retrieve_mail_attachments_not_found(self, mock_imaplib):
        _create_fake_imap(mock_imaplib, with_mail=True)

        with ImapHook() as imap_hook:
            with pytest.raises(AirflowException):
                imap_hook.retrieve_mail_attachments("test1.txt")

    @patch(imaplib_string)
    def test_retrieve_mail_attachments_with_regex_found(self, mock_imaplib):
        _create_fake_imap(mock_imaplib, with_mail=True)

        with ImapHook() as imap_hook:
            attachments_in_inbox = imap_hook.retrieve_mail_attachments(
                name=r"test(\d+).csv", check_regex=True
            )

        assert attachments_in_inbox == [("test1.csv", b"SWQsTmFtZQoxLEZlbGl4")]

    @patch(imaplib_string)
    def test_retrieve_mail_attachments_with_regex_not_found(self, mock_imaplib):
        _create_fake_imap(mock_imaplib, with_mail=True)

        with ImapHook() as imap_hook:
            with pytest.raises(AirflowException):
                imap_hook.retrieve_mail_attachments(
                    name=r"test_(\d+).csv",
                    check_regex=True,
                )

    @patch(imaplib_string)
    def test_retrieve_mail_attachments_latest_only(self, mock_imaplib):
        _create_fake_imap(mock_imaplib, with_mail=True)

        with ImapHook() as imap_hook:
            attachments_in_inbox = imap_hook.retrieve_mail_attachments(name="test1.csv", latest_only=True)

        assert attachments_in_inbox == [("test1.csv", b"SWQsTmFtZQoxLEZlbGl4")]

    @patch(imaplib_string)
    def test_retrieve_mail_attachments_with_mail_filter(self, mock_imaplib):
        _create_fake_imap(mock_imaplib, with_mail=True)
        mail_filter = '(SINCE "01-Jan-2019")'

        with ImapHook() as imap_hook:
            imap_hook.retrieve_mail_attachments(name="test1.csv", mail_filter=mail_filter)

        mock_imaplib.IMAP4_SSL.return_value.search.assert_called_once_with(None, mail_filter)

    @patch(open_string, new_callable=mock_open)
    @patch(imaplib_string)
    def test_download_mail_attachments_found(self, mock_imaplib, mock_open_method):
        _create_fake_imap(mock_imaplib, with_mail=True)

        with ImapHook() as imap_hook:
            imap_hook.download_mail_attachments("test1.csv", "test_directory")

        mock_open_method.assert_called_once_with("test_directory/test1.csv", "wb")
        mock_open_method.return_value.write.assert_called_once_with(b"SWQsTmFtZQoxLEZlbGl4")

    @patch(open_string, new_callable=mock_open)
    @patch(imaplib_string)
    def test_download_mail_attachments_not_found(self, mock_imaplib, mock_open_method):
        _create_fake_imap(mock_imaplib, with_mail=True)

        with ImapHook() as imap_hook:
            with pytest.raises(AirflowException):
                imap_hook.download_mail_attachments("test1.txt", "test_directory")

        mock_open_method.assert_not_called()
        mock_open_method.return_value.write.assert_not_called()

    @patch(open_string, new_callable=mock_open)
    @patch(imaplib_string)
    def test_download_mail_attachments_with_regex_found(self, mock_imaplib, mock_open_method):
        _create_fake_imap(mock_imaplib, with_mail=True)

        with ImapHook() as imap_hook:
            imap_hook.download_mail_attachments(
                name=r"test(\d+).csv", local_output_directory="test_directory", check_regex=True
            )

        mock_open_method.assert_called_once_with("test_directory/test1.csv", "wb")
        mock_open_method.return_value.write.assert_called_once_with(b"SWQsTmFtZQoxLEZlbGl4")

    @patch(open_string, new_callable=mock_open)
    @patch(imaplib_string)
    def test_download_mail_attachments_with_regex_not_found(self, mock_imaplib, mock_open_method):
        _create_fake_imap(mock_imaplib, with_mail=True)

        with ImapHook() as imap_hook:
            with pytest.raises(AirflowException):
                imap_hook.download_mail_attachments(
                    name=r"test_(\d+).csv",
                    local_output_directory="test_directory",
                    check_regex=True,
                )

        mock_open_method.assert_not_called()
        mock_open_method.return_value.write.assert_not_called()

    @patch(open_string, new_callable=mock_open)
    @patch(imaplib_string)
    def test_download_mail_attachments_with_latest_only(self, mock_imaplib, mock_open_method):
        _create_fake_imap(mock_imaplib, with_mail=True)

        with ImapHook() as imap_hook:
            imap_hook.download_mail_attachments(
                name="test1.csv", local_output_directory="test_directory", latest_only=True
            )

        mock_open_method.assert_called_once_with("test_directory/test1.csv", "wb")
        mock_open_method.return_value.write.assert_called_once_with(b"SWQsTmFtZQoxLEZlbGl4")

    @patch(open_string, new_callable=mock_open)
    @patch(imaplib_string)
    def test_download_mail_attachments_with_escaping_chars(self, mock_imaplib, mock_open_method):
        _create_fake_imap(mock_imaplib, with_mail=True, attachment_name="../test1.csv")

        with ImapHook() as imap_hook:
            imap_hook.download_mail_attachments(name="../test1.csv", local_output_directory="test_directory")

        mock_open_method.assert_not_called()
        mock_open_method.return_value.write.assert_not_called()

    @patch("airflow.providers.imap.hooks.imap.os.path.islink", return_value=True)
    @patch(open_string, new_callable=mock_open)
    @patch(imaplib_string)
    def test_download_mail_attachments_with_symlink(self, mock_imaplib, mock_open_method, mock_is_symlink):
        _create_fake_imap(mock_imaplib, with_mail=True, attachment_name="symlink")

        with ImapHook() as imap_hook:
            imap_hook.download_mail_attachments(name="symlink", local_output_directory="test_directory")

        assert mock_is_symlink.call_count == 1
        mock_open_method.assert_not_called()
        mock_open_method.return_value.write.assert_not_called()

    @patch(open_string, new_callable=mock_open)
    @patch(imaplib_string)
    def test_download_mail_attachments_with_mail_filter(self, mock_imaplib, mock_open_method):
        _create_fake_imap(mock_imaplib, with_mail=True)
        mail_filter = '(SINCE "01-Jan-2019")'

        with ImapHook() as imap_hook:
            imap_hook.download_mail_attachments(
                name="test1.csv", local_output_directory="test_directory", mail_filter=mail_filter
            )

        mock_imaplib.IMAP4_SSL.return_value.search.assert_called_once_with(None, mail_filter)
        assert mock_open_method.call_count == 1
