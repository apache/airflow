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

import pytest

from airflow.providers.slack.utils import ConnectionExtraConfig, parse_filename


class TestConnectionExtra:
    @pytest.mark.parametrize("conn_type", ["slack", "slack_incoming_webhook"])
    def test_get_extra_field(self, conn_type):
        """Test get arguments from connection extra: prefixed and not."""
        extra_config = ConnectionExtraConfig(
            conn_type=conn_type,
            conn_id="test-conn-id",
            extra={"arg1": "foo", f"extra__{conn_type}__arg2": "bar"},
        )
        assert extra_config.get("arg1") == "foo"
        assert extra_config.get("arg2") == "bar"

    def test_missing_extra_field(self):
        """Test missing field in extra."""
        extra_config = ConnectionExtraConfig(conn_type="slack", conn_id="test-conn-id", extra={})
        error_message = (
            r"Couldn't find 'extra__slack__arg_missing' or 'arg_missing' "
            r"in Connection \('test-conn-id'\) Extra and no default value specified\."
        )
        with pytest.raises(KeyError, match=error_message):
            extra_config.get("arg_missing")

    @pytest.mark.parametrize("value", [0, False, "", None], ids=lambda x: f"bool_false_{type(x).__name__}")
    def test_default_extra_field(self, value):
        """Test default value for missing field in extra."""
        extra_config = ConnectionExtraConfig(conn_type="slack", extra={})
        assert extra_config.get("arg_missing", default=value) == value

    @pytest.mark.parametrize("conn_type", ["slack", "slack_incoming_webhook"])
    def test_both_prefixed_and_not_in_extra_field(self, conn_type):
        """Test resolve field from extra when both specified prefixed and not for single field."""
        extra_config = ConnectionExtraConfig(
            conn_type=conn_type,
            conn_id="test-conn-id",
            extra={"arg1": "foo", f"extra__{conn_type}__arg1": "bar"},
        )
        assert extra_config.get("arg1") == "foo"

    @pytest.mark.parametrize("conn_type", ["slack", "slack_incoming_webhook"])
    @pytest.mark.parametrize("empty_value", [None, ""])
    def test_prefixed_extra_created_in_ui_connections(self, conn_type, empty_value):
        """Test that empty strings or None values in UI ignored."""
        extra_config = ConnectionExtraConfig(
            conn_type=conn_type,
            conn_id="test-conn-id",
            extra={
                f"extra__{conn_type}__arg_missing": empty_value,
                "arg_extra": "bar",
                f"extra__{conn_type}__arg_extra": empty_value,
            },
        )
        error_message = (
            r"Couldn't find '.*' or '.*' in Connection \('.*'\) Extra and no default value specified\."
        )
        with pytest.raises(KeyError, match=error_message):
            # No fallback should raise an error
            extra_config.get("arg_missing")

        assert extra_config.get("arg_missing", default="foo") == "foo"
        assert extra_config.get("arg_extra") == "bar"

    def test_get_parse_int(self):
        extra_config = ConnectionExtraConfig(
            conn_type="slack",
            extra={
                "int_arg_1": "42",
                "int_arg_2": 9000,
            },
        )
        assert extra_config.getint("int_arg_1") == 42
        assert extra_config.getint("int_arg_2") == 9000


class TestParseFilename:
    SUPPORTED_FORMAT = ("so", "dll", "exe", "sh")

    def test_error_parse_without_extension(self):
        with pytest.raises(ValueError, match="No file extension specified in filename"):
            assert parse_filename("Untitled File", self.SUPPORTED_FORMAT)

    @pytest.mark.parametrize(
        "filename,expected_format",
        [
            ("libc.so", "so"),
            ("kernel32.dll", "dll"),
            ("xxx.mp4.exe", "exe"),
            ("init.sh", "sh"),
        ],
    )
    def test_parse_first_level(self, filename, expected_format):
        assert parse_filename(filename, self.SUPPORTED_FORMAT) == (expected_format, None)

    @pytest.mark.parametrize("filename", ["New File.txt", "cats-memes.mp4"])
    def test_error_parse_first_level(self, filename):
        with pytest.raises(ValueError, match="Unsupported file format"):
            assert parse_filename(filename, self.SUPPORTED_FORMAT)

    @pytest.mark.parametrize(
        "filename,expected",
        [
            ("libc.so.6", ("so", "6")),
            ("kernel32.dll.zip", ("dll", "zip")),
            ("explorer.exe.7z", ("exe", "7z")),
            ("init.sh.gz", ("sh", "gz")),
        ],
    )
    def test_parse_second_level(self, filename, expected):
        assert parse_filename(filename, self.SUPPORTED_FORMAT) == expected

    @pytest.mark.parametrize("filename", ["example.so.tar.gz", "w.i.e.r.d"])
    def test_error_parse_second_level(self, filename):
        with pytest.raises(ValueError, match="Unsupported file format.*with compression extension."):
            assert parse_filename(filename, self.SUPPORTED_FORMAT)

    @pytest.mark.parametrize("filename", ["Untitled File", "New File.txt", "example.so.tar.gz"])
    @pytest.mark.parametrize("fallback", SUPPORTED_FORMAT)
    def test_fallback(self, filename, fallback):
        assert parse_filename(filename, self.SUPPORTED_FORMAT, fallback) == (fallback, None)

    @pytest.mark.parametrize("filename", ["Untitled File", "New File.txt", "example.so.tar.gz"])
    def test_wrong_fallback(self, filename):
        with pytest.raises(ValueError, match="Invalid fallback value"):
            assert parse_filename(filename, self.SUPPORTED_FORMAT, "mp4")
