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
import os
from os import path
import unittest
import shutil
import tempfile

import boto3
import mock

from airflow.contrib.hooks.fs_hooks import (LocalHook as LocalFsHook, S3FsHook,
                                            Hdfs3Hook)

from moto import mock_s3


class TestLocalFsHook(unittest.TestCase):
    """Tests for the LocalFsHook."""

    def setUp(self):
        self._tmp_dir = tempfile.mkdtemp()

        # Bootstrap some files.
        with open(path.join(self._tmp_dir, 'hello.txt'), 'wb') as file_:
            file_.write(b'Hello world!\n')

        with open(path.join(self._tmp_dir, 'hello.csv'), 'wb') as file_:
            file_.write(b'Hello world!\n')

        nested_dir = path.join(self._tmp_dir, 'test')
        os.mkdir(nested_dir)

        with open(path.join(nested_dir, 'nested.txt'), 'wb') as file_:
            file_.write(b'Nested\n')

    def tearDown(self):
        shutil.rmtree(self._tmp_dir)

    def test_open(self):
        """Tests the open method."""

        with LocalFsHook() as hook:
            # Try to write file.
            new_path = path.join(self._tmp_dir, 'new.txt')
            self.assertFalse(hook.exists(new_path))

            with hook.open(new_path, 'wb') as file_:
                file_.write(b'Hello world!')

            # Check file exists.
            self.assertTrue(hook.exists(new_path))

            # Check reading file.
            with hook.open(new_path, 'rb') as file_:
                self.assertEqual(file_.read(), b'Hello world!')

    def test_exists(self):
        """Tests the exists method."""

        with LocalFsHook() as hook:
            self.assertTrue(
                hook.exists(path.join(self._tmp_dir, 'hello.txt')))
            self.assertFalse(
                hook.exists(path.join(self._tmp_dir, 'random.txt')))

    def test_makedir(self):
        """Tests the makedir method."""

        with LocalFsHook() as hook:
            # Test non-existing directory.
            dir_path = path.join(self._tmp_dir, 'new')
            self.assertFalse(hook.exists(dir_path))
            hook.makedir(dir_path)
            self.assertTrue(hook.exists(dir_path))

            # Test existing directory with exist_ok = True.
            dir_path = path.join(self._tmp_dir, 'test')
            self.assertTrue(hook.exists(dir_path))
            hook.makedir(dir_path, exist_ok=True)

            # Test existing directory with exist_ok = False.
            with self.assertRaises(IOError):
                hook.makedir(dir_path, exist_ok=False)

            # Test nested directory (should fail).
            dir_path = path.join(self._tmp_dir, 'new2', 'nested')
            with self.assertRaises(IOError):
                hook.makedir(dir_path)

    def test_makedirs(self):
        """Tests the makedirs method."""

        with LocalFsHook() as hook:
            # Test non-existing directory.
            dir_path = path.join(self._tmp_dir, 'new')
            self.assertFalse(hook.exists(dir_path))
            hook.makedirs(dir_path)
            self.assertTrue(hook.exists(dir_path))

            # Test existing directory with exist_ok = True.
            dir_path = path.join(self._tmp_dir, 'test')
            self.assertTrue(hook.exists(dir_path))
            hook.makedirs(dir_path, exist_ok=True)

            # Test existing directory with exist_ok = False.
            with self.assertRaises(IOError):
                hook.makedirs(dir_path, exist_ok=False)

            # Test nested directory (should fail).
            dir_path = path.join(self._tmp_dir, 'new2', 'nested')
            hook.makedirs(dir_path)
            self.assertTrue(hook.exists(dir_path))

    def test_walk(self):
        """Tests the walk method."""

        expected = [(self._tmp_dir, ['test'], ['hello.csv', 'hello.txt']),
                    (path.join(self._tmp_dir, 'test'), [], ['nested.txt'])]

        with LocalFsHook() as hook:
            result = list(hook.walk(self._tmp_dir))

        for res_item, exp_item in zip(result, expected):
            self.assertEqual(res_item[0], exp_item[0])
            self.assertEqual(sorted(res_item[1]), sorted(exp_item[1]))
            self.assertEqual(sorted(res_item[2]), sorted(exp_item[2]))

    def test_glob(self):
        """Tests glob method."""

        with LocalFsHook() as hook:
            self.assertEqual(
                hook.glob(path.join(self._tmp_dir, '*.txt')),
                [path.join(self._tmp_dir, 'hello.txt')])

            self.assertEqual(
                hook.glob(path.join(self._tmp_dir, '**/*.txt')),
                [path.join(self._tmp_dir, 'test', 'nested.txt')])

            self.assertEqual(hook.glob(path.join(self._tmp_dir, '*.xml')), [])

    def test_rm(self):
        """Tests rm method."""

        with LocalFsHook() as hook:
            file_path = path.join(self._tmp_dir, 'hello.txt')
            self.assertTrue(hook.exists(file_path))

            hook.rm(file_path)
            self.assertFalse(hook.exists(file_path))

    def test_rmtree(self):
        """Tests the rmtree method."""

        with LocalFsHook() as hook:
            dir_path = path.join(self._tmp_dir, 'test')
            self.assertTrue(hook.exists(dir_path))

            hook.rmtree(dir_path)
            self.assertFalse(hook.exists(dir_path))


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


class TestHdfs3Hook(unittest.TestCase):
    """
    Tests for the Hdfs3Hook class.

    Note that the HDFileSystem class is mocked in most of these tests
    to avoid the requirement of having a local HDFS instance for testing.
    """

    def setUp(self):
        self._mock_fs = mock.Mock()

        self._mocked_hook = Hdfs3Hook()
        self._mocked_hook._conn = self._mock_fs

    def test_open(self):
        """Tests the `open` method."""

        with self._mocked_hook as hook:
            hook.open('test.txt', mode='rb')

        self._mock_fs.open.assert_called_once_with('test.txt', mode='rb')

    def test_exists(self):
        """Tests the `exists` method."""

        with self._mocked_hook as hook:
            hook.exists('test.txt')

        self._mock_fs.exists.assert_called_once_with('test.txt')

    def test_makedir(self):
        """Tests the `makedir` method with a non-existing dir."""

        self._mock_fs.exists.return_value = False

        with self._mocked_hook as hook:
            hook.makedir('path/to/dir', mode=0o755)

        self._mock_fs.mkdir.assert_called_once_with('path/to/dir')
        self._mock_fs.chmod.assert_called_once_with('path/to/dir', mode=0o755)

    def test_makedir_existing(self):
        """Tests the `makedir` method with an existing dir
           and exist_ok = False.
        """

        self._mock_fs.exists.return_value = True

        with self._mocked_hook as hook:
            with self.assertRaises(IOError):
                hook.makedir('path/to/dir', mode=0o755, exist_ok=False)

    def test_makedir_existing_ok(self):
        """Tests the `makedir` method with an existing dir
           and exist_ok = True.
        """

        self._mock_fs.exists.return_value = True

        with self._mocked_hook as hook:
            hook.makedir('path/to/dir', mode=0o755, exist_ok=True)

        self._mock_fs.chmod.assert_not_called()

    def test_makedirs(self):
        """Tests the `makedirs` method with a non-existing dir."""

        self._mock_fs.exists.return_value = False

        with self._mocked_hook as hook:
            hook.makedirs('path/to/dir', mode=0o755)

        self._mock_fs.makedirs.assert_called_once_with(
            'path/to/dir', mode=0o755)

    def test_makedirs_existing(self):
        """Tests the `makedirs` method with an existing dir
           and exist_ok = False.
        """

        self._mock_fs.exists.return_value = True

        with self._mocked_hook as hook:
            with self.assertRaises(IOError):
                hook.makedirs('path/to/dir', mode=0o755, exist_ok=False)

        self._mock_fs.makedirs.assert_not_called()

    def test_makedirs_existing_ok(self):
        """Tests the `makedir` method with an existing dir
           and exist_ok = True.
        """

        self._mock_fs.exists.return_value = True

        with self._mocked_hook as hook:
            hook.makedirs('path/to/dir', mode=0o755, exist_ok=True)

    def test_glob(self):
        """Tests the `glob` method."""

        with self._mocked_hook as hook:
            hook.glob('*.txt')

        self._mock_fs.glob.assert_called_once_with('*.txt')

    def test_rm(self):
        """Tests the `rm` method."""

        with self._mocked_hook as hook:
            hook.rm('test_dir')

        self._mock_fs.rm.assert_called_once_with(
            'test_dir', recursive=False)

    def test_rmtree(self):
        """Tests the `rmtree` method."""

        with self._mocked_hook as hook:
            hook.rmtree('test_dir')

        self._mock_fs.rm.assert_called_once_with(
            'test_dir', recursive=True)


if __name__ == '__main__':
    unittest.main()
