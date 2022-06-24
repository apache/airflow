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

import pytest

from airflow.providers.snowflake.utils.common import enclose_param, parse_filename


@pytest.mark.parametrize(
    "param,expected",
    [
        ("without quotes", "'without quotes'"),
        ("'with quotes'", "'''with quotes'''"),
        ("Today's sales projections", "'Today''s sales projections'"),
        ("sample/john's.csv", "'sample/john''s.csv'"),
        (".*'awesome'.*[.]csv", "'.*''awesome''.*[.]csv'"),
    ],
)
def test_parameter_enclosure(param, expected):
    assert enclose_param(param) == expected


SUPPORTED_FORMAT = ("so", "dll", "exe", "sh")


class TestParseFilename:
    def test_error_parse_without_extension(self):
        with pytest.raises(ValueError, match="No file extension specified in filename"):
            assert parse_filename("Untitled File", SUPPORTED_FORMAT)

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
        assert parse_filename(filename, SUPPORTED_FORMAT) == (expected_format, None)

    @pytest.mark.parametrize("filename", ["New File.txt", "cats-memes.mp4"])
    def test_error_parse_first_level(self, filename):
        with pytest.raises(ValueError, match="Unsupported file format"):
            assert parse_filename(filename, SUPPORTED_FORMAT)

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
        assert parse_filename(filename, SUPPORTED_FORMAT) == expected

    @pytest.mark.parametrize("filename", ["example.so.tar.gz", "w.i.e.r.d"])
    def test_error_parse_second_level(self, filename):
        with pytest.raises(ValueError, match="Unsupported file format.*with compression extension."):
            assert parse_filename(filename, SUPPORTED_FORMAT)

    @pytest.mark.parametrize("filename", ["Untitled File", "New File.txt", "example.so.tar.gz"])
    @pytest.mark.parametrize("fallback", SUPPORTED_FORMAT)
    def test_fallback(self, filename, fallback):
        assert parse_filename(filename, SUPPORTED_FORMAT, fallback) == (fallback, None)

    @pytest.mark.parametrize("filename", ["Untitled File", "New File.txt", "example.so.tar.gz"])
    def test_wrong_fallback(self, filename):
        with pytest.raises(ValueError, match="Invalid fallback value"):
            assert parse_filename(filename, SUPPORTED_FORMAT, "mp4")
