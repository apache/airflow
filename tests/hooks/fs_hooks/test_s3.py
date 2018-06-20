# -*- coding: utf-8 -*-
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
#

import io
import posixpath
import unittest

import mock

import boto3
from moto import mock_s3

from airflow.hooks.fs_hooks.s3 import S3FsHook, s3fs


class TestS3FsHook(unittest.TestCase):
    """Tests for the S3FsHook."""

    def setUp(self):
        self._mock_s3 = mock_s3()
        self._mock_s3.start()

        # Create bucket.
        conn = boto3.resource('s3')
        self._bucket = conn.create_bucket(Bucket='test_bucket')

        # Bootstrap some files.
        buffer = io.BytesIO(b'Hello world!\n')
        self._bucket.upload_fileobj(buffer, 'hello.txt')

        buffer = io.BytesIO(b'Hello world!\n')
        self._bucket.upload_fileobj(buffer, 'hello.csv')

        buffer = io.BytesIO(b'Nested\n')
        self._bucket.upload_fileobj(buffer, 'test/nested.txt')

    def tearDown(self):
        self._mock_s3.stop()

    @mock.patch.object(s3fs, 'S3FileSystem')
    @mock.patch.object(S3FsHook, 'get_connection')
    def test_get_conn(self, conn_mock, s3fs_mock):
        """Tests get_conn call without a connection."""

        with S3FsHook() as hook:
            hook.get_conn()

        conn_mock.assert_not_called()
        s3fs_mock.assert_called_once_with()

    @mock.patch.object(s3fs, 'S3FileSystem')
    @mock.patch.object(S3FsHook, 'get_connection')
    def test_get_conn_with_conn(self, conn_mock, s3fs_mock):
        """Tests get_conn call with a connection."""

        conn_mock.return_value = mock.Mock(
            login='s3_id',
            password='s3_access_key',
            extra_dejson={})

        with S3FsHook(conn_id='s3_default') as hook:
            hook.get_conn()

        s3fs_mock.assert_called_once_with(
            key='s3_id',
            secret='s3_access_key',
            s3_additional_kwargs={})

    @mock.patch.object(s3fs, 'S3FileSystem')
    @mock.patch.object(S3FsHook, 'get_connection')
    def test_get_conn_with_encr(self, conn_mock, s3fs_mock):
        """Tests get_conn call with an encrypted connection."""

        conn_mock.return_value = mock.Mock(
            login='s3_id',
            password='s3_access_key',
            extra_dejson={'encryption': 'AES256'})

        with S3FsHook(conn_id='s3_default') as hook:
            hook.get_conn()

        conn_mock.assert_called_once_with('s3_default')

        s3fs_mock.assert_called_once_with(
            key='s3_id',
            secret='s3_access_key',
            s3_additional_kwargs={'ServerSideEncryption': 'AES256'})

    def test_with(self):
        """Tests if context manager closes the connection."""

        with mock.patch.object(S3FsHook, 'disconnect') as mock_disconnect:
            with S3FsHook() as hook:
                pass

        mock_disconnect.assert_called_once()
        self.assertIsNone(hook._conn)

    def test_open(self):
        """Tests the open method."""

        with S3FsHook() as hook:
            # Try to write file.
            with hook.open('s3://test_bucket/new.txt', 'wb') as file_:
                file_.write(b'Hello world!')

            # Check file exists.
            self.assertTrue(hook.exists('s3://test_bucket/new.txt'))

            # Check reading file.
            with hook.open('s3://test_bucket/new.txt', 'rb') as file_:
                self.assertEqual(file_.read(), b'Hello world!')

    def test_exists(self):
        """Tests the exists method."""

        with S3FsHook() as hook:
            self.assertTrue(hook.exists('s3://test_bucket/hello.txt'))
            self.assertFalse(hook.exists('s3://test_bucket/random.txt'))

    def test_isdir(self):
        """Tests the isdir method."""

        with S3FsHook() as hook:
            self.assertTrue(hook.isdir('s3://test_bucket/test'))
            self.assertFalse(hook.isdir('s3://test_bucket/hello.csv'))

    def test_makedir(self):
        """Tests the makedirs method (effectively a no-op)."""

        with S3FsHook() as hook:
            hook.makedir('s3://test_bucket/test/nested')

    def test_makedirs(self):
        """Tests the makedirs method (effectively a no-op)."""

        with S3FsHook() as hook:
            hook.makedirs('s3://test_bucket/test/nested')

    def test_walk(self):
        """Tests the walk method."""

        expected = [('test_bucket', ['test'], ['hello.csv', 'hello.txt']),
                    ('test_bucket/test', [], ['nested.txt'])]

        with S3FsHook() as hook:
            result = list(hook.walk('s3://test_bucket'))

        for res_item, exp_item in zip(result, expected):
            self.assertEqual(res_item[0], exp_item[0])
            self.assertEqual(sorted(res_item[1]), sorted(exp_item[1]))
            self.assertEqual(sorted(res_item[2]), sorted(exp_item[2]))

    def test_glob(self):
        """Tests glob method."""

        with S3FsHook() as hook:
            self.assertEqual(
                hook.glob('s3://test_bucket/*.txt'),
                ['test_bucket/hello.txt'])

            self.assertEqual(
                hook.glob('s3://test_bucket/**/*.txt'),
                ['test_bucket/test/nested.txt'])

            self.assertEqual(hook.glob('s3://test_bucket/*.xml'), [])

    def test_rm(self):
        """Tetts rm method."""

        with S3FsHook() as hook:
            self.assertTrue(hook.exists('s3://test_bucket/hello.txt'))
            hook.rm('s3://test_bucket/hello.txt')
            self.assertFalse(hook.exists('s3://test_bucket/hello.txt'))

    def test_rmtree(self):
        """Tests the rmtree method."""

        with S3FsHook() as hook:
            self.assertTrue(hook.exists('s3://test_bucket/test/nested.txt'))
            hook.rmtree('s3://test_bucket/test')
            self.assertFalse(hook.exists('s3://test_bucket/test/nested.txt'))

    def test_join(self):
        """Tests the join method."""

        self.assertEqual(S3FsHook.join('test', 'example.csv'),
                         posixpath.join('test', 'example.csv'))

    def test_split(self):
        """Tests the split method."""

        file_path = posixpath.join('test', 'example.csv')
        self.assertEqual(S3FsHook.split(file_path),
                         posixpath.split(file_path))


if __name__ == '__main__':
    unittest.main()
