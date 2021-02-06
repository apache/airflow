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

import unittest
from unittest import mock

from airflow.utils.file import correct_maybe_zipped, open_maybe_zipped


class TestCorrectMaybeZipped(unittest.TestCase):
    @mock.patch("zipfile.is_zipfile")
    def test_correct_maybe_zipped_normal_file(self, mocked_is_zipfile):
        path = '/path/to/some/file.txt'
        mocked_is_zipfile.return_value = False

        dag_folder = correct_maybe_zipped(path)

        assert dag_folder == path

    @mock.patch("zipfile.is_zipfile")
    def test_correct_maybe_zipped_normal_file_with_zip_in_name(self, mocked_is_zipfile):
        path = '/path/to/fakearchive.zip.other/file.txt'
        mocked_is_zipfile.return_value = False

        dag_folder = correct_maybe_zipped(path)

        assert dag_folder == path

    @mock.patch("zipfile.is_zipfile")
    def test_correct_maybe_zipped_archive(self, mocked_is_zipfile):
        path = '/path/to/archive.zip/deep/path/to/file.txt'
        mocked_is_zipfile.return_value = True

        dag_folder = correct_maybe_zipped(path)

        assert mocked_is_zipfile.call_count == 1
        (args, kwargs) = mocked_is_zipfile.call_args_list[0]
        assert '/path/to/archive.zip' == args[0]

        assert dag_folder == '/path/to/archive.zip'


class TestOpenMaybeZipped(unittest.TestCase):
    def test_open_maybe_zipped_normal_file(self):
        with mock.patch('builtins.open', mock.mock_open(read_data="data")) as mock_file:
            open_maybe_zipped('/path/to/some/file.txt')
            mock_file.assert_called_once_with('/path/to/some/file.txt', mode='r')

    def test_open_maybe_zipped_normal_file_with_zip_in_name(self):
        path = '/path/to/fakearchive.zip.other/file.txt'
        with mock.patch('builtins.open', mock.mock_open(read_data="data")) as mock_file:
            open_maybe_zipped(path)
            mock_file.assert_called_once_with(path, mode='r')

    @mock.patch("zipfile.is_zipfile")
    @mock.patch("zipfile.ZipFile")
    @mock.patch("io.TextIOWrapper")
    def test_open_maybe_zipped_archive(self, mocked_text_io_wrapper, mocked_zip_file, mocked_is_zipfile):
        mocked_is_zipfile.return_value = True
        open_return_value = mock.mock_open(read_data="data")
        instance = mocked_zip_file.return_value
        instance.open.return_value = open_return_value

        open_maybe_zipped('/path/to/archive.zip/deep/path/to/file.txt')

        mocked_text_io_wrapper.assert_called_once_with(open_return_value)
        mocked_is_zipfile.assert_called_once_with('/path/to/archive.zip')
        mocked_zip_file.assert_called_once_with('/path/to/archive.zip', mode='r')
        instance.open.assert_called_once_with('deep/path/to/file.txt')
