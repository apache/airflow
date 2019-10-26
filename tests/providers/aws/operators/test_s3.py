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

import bz2
import errno
import filecmp
import io
import logging
import os
import shutil
import sys
import unittest
from collections import OrderedDict
from gzip import GzipFile
from itertools import product
from tempfile import NamedTemporaryFile, mkdtemp
from unittest import mock

import boto3
from boto3.session import Session
from moto import mock_s3

from airflow.configuration import conf
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.exceptions import AirflowException
from airflow.models import DAG, TaskInstance
from airflow.providers.aws.operators.s3 import (
    S3CopyObjectOperator, S3DeleteObjectsOperator, S3FileTransformOperator, S3ListOperator,
    S3ToGoogleCloudStorageOperator, S3ToHiveTransfer, S3ToRedshiftTransfer, S3ToSFTPOperator,
)
from airflow.utils import timezone
from airflow.utils.tests import assertEqualIgnoreMultipleSpaces
from airflow.utils.timezone import datetime


class TestS3CopyObjectOperator(unittest.TestCase):

    def setUp(self):
        self.source_bucket = "bucket1"
        self.source_key = "path1/data.txt"
        self.dest_bucket = "bucket2"
        self.dest_key = "path2/data_copy.txt"

    @mock_s3
    def test_s3_copy_object_arg_combination_1(self):
        conn = boto3.client('s3')
        conn.create_bucket(Bucket=self.source_bucket)
        conn.create_bucket(Bucket=self.dest_bucket)
        conn.upload_fileobj(Bucket=self.source_bucket,
                            Key=self.source_key,
                            Fileobj=io.BytesIO(b"input"))

        # there should be nothing found before S3CopyObjectOperator is executed
        self.assertFalse('Contents' in conn.list_objects(Bucket=self.dest_bucket,
                                                         Prefix=self.dest_key))

        t = S3CopyObjectOperator(task_id="test_task_s3_copy_object",
                                 source_bucket_key=self.source_key,
                                 source_bucket_name=self.source_bucket,
                                 dest_bucket_key=self.dest_key,
                                 dest_bucket_name=self.dest_bucket)
        t.execute(None)

        objects_in_dest_bucket = conn.list_objects(Bucket=self.dest_bucket,
                                                   Prefix=self.dest_key)
        # there should be object found, and there should only be one object found
        self.assertEqual(len(objects_in_dest_bucket['Contents']), 1)
        # the object found should be consistent with dest_key specified earlier
        self.assertEqual(objects_in_dest_bucket['Contents'][0]['Key'], self.dest_key)

    @mock_s3
    def test_s3_copy_object_arg_combination_2(self):
        conn = boto3.client('s3')
        conn.create_bucket(Bucket=self.source_bucket)
        conn.create_bucket(Bucket=self.dest_bucket)
        conn.upload_fileobj(Bucket=self.source_bucket,
                            Key=self.source_key,
                            Fileobj=io.BytesIO(b"input"))

        # there should be nothing found before S3CopyObjectOperator is executed
        self.assertFalse('Contents' in conn.list_objects(Bucket=self.dest_bucket,
                                                         Prefix=self.dest_key))

        source_key_s3_url = "s3://{}/{}".format(self.source_bucket, self.source_key)
        dest_key_s3_url = "s3://{}/{}".format(self.dest_bucket, self.dest_key)
        t = S3CopyObjectOperator(task_id="test_task_s3_copy_object",
                                 source_bucket_key=source_key_s3_url,
                                 dest_bucket_key=dest_key_s3_url)
        t.execute(None)

        objects_in_dest_bucket = conn.list_objects(Bucket=self.dest_bucket,
                                                   Prefix=self.dest_key)
        # there should be object found, and there should only be one object found
        self.assertEqual(len(objects_in_dest_bucket['Contents']), 1)
        # the object found should be consistent with dest_key specified earlier
        self.assertEqual(objects_in_dest_bucket['Contents'][0]['Key'], self.dest_key)


class TestS3DeleteObjectsOperator(unittest.TestCase):

    @mock_s3
    def test_s3_delete_single_object(self):
        bucket = "testbucket"
        key = "path/data.txt"

        conn = boto3.client('s3')
        conn.create_bucket(Bucket=bucket)
        conn.upload_fileobj(Bucket=bucket,
                            Key=key,
                            Fileobj=io.BytesIO(b"input"))

        # The object should be detected before the DELETE action is taken
        objects_in_dest_bucket = conn.list_objects(Bucket=bucket,
                                                   Prefix=key)
        self.assertEqual(len(objects_in_dest_bucket['Contents']), 1)
        self.assertEqual(objects_in_dest_bucket['Contents'][0]['Key'], key)

        t = S3DeleteObjectsOperator(task_id="test_task_s3_delete_single_object",
                                    bucket=bucket,
                                    keys=key)
        t.execute(None)

        # There should be no object found in the bucket created earlier
        self.assertFalse('Contents' in conn.list_objects(Bucket=bucket,
                                                         Prefix=key))

    @mock_s3
    def test_s3_delete_multiple_objects(self):
        bucket = "testbucket"
        key_pattern = "path/data"
        n_keys = 3
        keys = [key_pattern + str(i) for i in range(n_keys)]

        conn = boto3.client('s3')
        conn.create_bucket(Bucket=bucket)
        for k in keys:
            conn.upload_fileobj(Bucket=bucket,
                                Key=k,
                                Fileobj=io.BytesIO(b"input"))

        # The objects should be detected before the DELETE action is taken
        objects_in_dest_bucket = conn.list_objects(Bucket=bucket,
                                                   Prefix=key_pattern)
        self.assertEqual(len(objects_in_dest_bucket['Contents']), n_keys)
        self.assertEqual(sorted([x['Key'] for x in objects_in_dest_bucket['Contents']]),
                         sorted(keys))

        t = S3DeleteObjectsOperator(task_id="test_task_s3_delete_multiple_objects",
                                    bucket=bucket,
                                    keys=keys)
        t.execute(None)

        # There should be no object found in the bucket created earlier
        self.assertFalse('Contents' in conn.list_objects(Bucket=bucket,
                                                         Prefix=key_pattern))


class TestS3FileTransformOperator(unittest.TestCase):

    def setUp(self):
        self.tmp_dir = mkdtemp(prefix='test_tmpS3FileTransform_')
        self.transform_script = os.path.join(self.tmp_dir, "transform.py")
        os.mknod(self.transform_script)

    def tearDown(self):
        try:
            shutil.rmtree(self.tmp_dir)
        except OSError as e:
            # ENOENT - no such file or directory
            if e.errno != errno.ENOENT:
                raise e

    @mock.patch('subprocess.Popen')
    @mock.patch.object(S3FileTransformOperator, 'log')
    @mock_s3
    def test_execute_with_transform_script(self, mock_log, mock_Popen):
        process_output = [b"Foo", b"Bar", b"Baz"]

        process = mock_Popen.return_value
        process.stdout.readline.side_effect = process_output
        process.wait.return_value = None
        process.returncode = 0

        bucket = "bucket"
        input_key = "foo"
        output_key = "bar"
        bio = io.BytesIO(b"input")

        conn = boto3.client('s3')
        conn.create_bucket(Bucket=bucket)
        conn.upload_fileobj(Bucket=bucket, Key=input_key, Fileobj=bio)

        s3_url = "s3://{0}/{1}"
        t = S3FileTransformOperator(
            source_s3_key=s3_url.format(bucket, input_key),
            dest_s3_key=s3_url.format(bucket, output_key),
            transform_script=self.transform_script,
            replace=True,
            task_id="task_id")
        t.execute(None)

        mock_log.info.assert_has_calls([
            mock.call(line.decode(sys.getdefaultencoding())) for line in process_output
        ])

    @mock.patch('subprocess.Popen')
    @mock_s3
    def test_execute_with_failing_transform_script(self, mock_Popen):
        process = mock_Popen.return_value
        process.stdout.readline.side_effect = []
        process.wait.return_value = None
        process.returncode = 42

        bucket = "bucket"
        input_key = "foo"
        output_key = "bar"
        bio = io.BytesIO(b"input")

        conn = boto3.client('s3')
        conn.create_bucket(Bucket=bucket)
        conn.upload_fileobj(Bucket=bucket, Key=input_key, Fileobj=bio)

        s3_url = "s3://{0}/{1}"
        t = S3FileTransformOperator(
            source_s3_key=s3_url.format(bucket, input_key),
            dest_s3_key=s3_url.format(bucket, output_key),
            transform_script=self.transform_script,
            replace=True,
            task_id="task_id")

        with self.assertRaises(AirflowException) as e:
            t.execute(None)

        self.assertEqual('Transform script failed: 42', str(e.exception))

    @mock.patch('airflow.hooks.S3_hook.S3Hook.select_key', return_value="input")
    @mock_s3
    def test_execute_with_select_expression(self, mock_select_key):
        bucket = "bucket"
        input_key = "foo"
        output_key = "bar"
        bio = io.BytesIO(b"input")

        conn = boto3.client('s3')
        conn.create_bucket(Bucket=bucket)
        conn.upload_fileobj(Bucket=bucket, Key=input_key, Fileobj=bio)

        s3_url = "s3://{0}/{1}"
        select_expression = "SELECT * FROM S3Object s"
        t = S3FileTransformOperator(
            source_s3_key=s3_url.format(bucket, input_key),
            dest_s3_key=s3_url.format(bucket, output_key),
            select_expression=select_expression,
            replace=True,
            task_id="task_id")
        t.execute(None)

        mock_select_key.assert_called_once_with(
            key=s3_url.format(bucket, input_key),
            expression=select_expression
        )


class TestS3ListOperator(unittest.TestCase):
    TASK_ID = 'test-s3-list-operator'
    BUCKET = 'test-bucket'
    DELIMITER = '.csv'
    PREFIX = 'TEST'
    MOCK_FILES = ["TEST1.csv", "TEST2.csv", "TEST3.csv"]

    @mock.patch('airflow.providers.aws.operators.s3.S3Hook')
    def test_execute(self, mock_hook):

        mock_hook.return_value.list_keys.return_value = self.MOCK_FILES

        operator = S3ListOperator(
            task_id=self.TASK_ID, bucket=self.BUCKET, prefix=self.PREFIX, delimiter=self.DELIMITER)

        files = operator.execute(None)

        mock_hook.return_value.list_keys.assert_called_once_with(
            bucket_name=self.BUCKET, prefix=self.PREFIX, delimiter=self.DELIMITER)
        self.assertEqual(sorted(files), sorted(self.MOCK_FILES))


class TestS3ToGoogleCloudStorageOperator(unittest.TestCase):
    TASK_ID = 'test-s3-gcs-operator'
    S3_BUCKET = 'test-bucket'
    S3_PREFIX = 'TEST'
    S3_DELIMITER = '/'
    GCS_PATH_PREFIX = 'gs://gcs-bucket/data/'
    MOCK_FILES = ["TEST1.csv", "TEST2.csv", "TEST3.csv"]
    AWS_CONN_ID = 'aws_default'
    GCS_CONN_ID = 'google_cloud_default'

    def test_init(self):
        """Test S3ToGoogleCloudStorageOperator instance is properly initialized."""

        operator = S3ToGoogleCloudStorageOperator(
            task_id=self.TASK_ID,
            bucket=self.S3_BUCKET,
            prefix=self.S3_PREFIX,
            delimiter=self.S3_DELIMITER,
            gcp_conn_id=self.GCS_CONN_ID,
            dest_gcs=self.GCS_PATH_PREFIX)

        self.assertEqual(operator.task_id, self.TASK_ID)
        self.assertEqual(operator.bucket, self.S3_BUCKET)
        self.assertEqual(operator.prefix, self.S3_PREFIX)
        self.assertEqual(operator.delimiter, self.S3_DELIMITER)
        self.assertEqual(operator.gcp_conn_id, self.GCS_CONN_ID)
        self.assertEqual(operator.dest_gcs, self.GCS_PATH_PREFIX)

    @mock.patch('airflow.providers.aws.operators.s3.S3Hook')
    @mock.patch('airflow.providers.aws.operators.s3.GoogleCloudStorageHook')
    def test_execute(self, gcs_mock_hook, s3_mock_hook):
        """Test the execute function when the run is successful."""

        operator = S3ToGoogleCloudStorageOperator(
            task_id=self.TASK_ID,
            bucket=self.S3_BUCKET,
            prefix=self.S3_PREFIX,
            delimiter=self.S3_DELIMITER,
            dest_gcs_conn_id=self.GCS_CONN_ID,
            dest_gcs=self.GCS_PATH_PREFIX)

        s3_mock_hook.return_value.list_keys.return_value = self.MOCK_FILES

        uploaded_files = operator.execute(None)
        gcs_mock_hook.return_value.upload.assert_has_calls(
            [
                mock.call('gcs-bucket', 'data/TEST1.csv', mock.ANY, gzip=False),
                mock.call('gcs-bucket', 'data/TEST3.csv', mock.ANY, gzip=False),
                mock.call('gcs-bucket', 'data/TEST2.csv', mock.ANY, gzip=False)
            ], any_order=True
        )

        # Assert if S3Hook is called twice, once for listing and once within S3ToGoogleCloudStorageOperator
        assert s3_mock_hook.call_args_list == [
            mock.call(aws_conn_id=self.AWS_CONN_ID, verify=None),
            mock.call(aws_conn_id=self.AWS_CONN_ID, verify=None),
        ]

        gcs_mock_hook.assert_called_once_with(
            google_cloud_storage_conn_id=self.GCS_CONN_ID, delegate_to=None)

        # we expect MOCK_FILES to be uploaded
        self.assertEqual(sorted(self.MOCK_FILES), sorted(uploaded_files))

    @mock.patch('airflow.providers.aws.operators.s3.S3Hook')
    @mock.patch('airflow.providers.aws.operators.s3.GoogleCloudStorageHook')
    def test_execute_with_gzip(self, gcs_mock_hook, s3_mock_hook):
        """Test the execute function when the run is successful."""

        operator = S3ToGoogleCloudStorageOperator(
            task_id=self.TASK_ID,
            bucket=self.S3_BUCKET,
            prefix=self.S3_PREFIX,
            delimiter=self.S3_DELIMITER,
            dest_gcs_conn_id=self.GCS_CONN_ID,
            dest_gcs=self.GCS_PATH_PREFIX,
            gzip=True
        )

        s3_mock_hook.return_value.list_keys.return_value = self.MOCK_FILES

        operator.execute(None)
        gcs_mock_hook.return_value.upload.assert_has_calls(
            [
                mock.call('gcs-bucket', 'data/TEST2.csv', mock.ANY, gzip=True),
                mock.call('gcs-bucket', 'data/TEST1.csv', mock.ANY, gzip=True),
                mock.call('gcs-bucket', 'data/TEST3.csv', mock.ANY, gzip=True)
            ], any_order=True
        )


class TestS3ToHiveTransfer(unittest.TestCase):

    def setUp(self):
        self.fn = {}
        self.task_id = 'S3ToHiveTransferTest'
        self.s3_key = 'S32hive_test_file'
        self.field_dict = OrderedDict([('Sno', 'BIGINT'), ('Some,Text', 'STRING')])
        self.hive_table = 'S32hive_test_table'
        self.delimiter = '\t'
        self.create = True
        self.recreate = True
        self.partition = {'ds': 'STRING'}
        self.headers = True
        self.check_headers = True
        self.wildcard_match = False
        self.input_compressed = False
        self.kwargs = {'task_id': self.task_id,
                       's3_key': self.s3_key,
                       'field_dict': self.field_dict,
                       'hive_table': self.hive_table,
                       'delimiter': self.delimiter,
                       'create': self.create,
                       'recreate': self.recreate,
                       'partition': self.partition,
                       'headers': self.headers,
                       'check_headers': self.check_headers,
                       'wildcard_match': self.wildcard_match,
                       'input_compressed': self.input_compressed
                       }
        try:
            header = b"Sno\tSome,Text \n"
            line1 = b"1\tAirflow Test\n"
            line2 = b"2\tS32HiveTransfer\n"
            self.tmp_dir = mkdtemp(prefix='test_tmps32hive_')
            # create sample txt, gz and bz2 with and without headers
            with NamedTemporaryFile(mode='wb+',
                                    dir=self.tmp_dir,
                                    delete=False) as f_txt_h:
                self._set_fn(f_txt_h.name, '.txt', True)
                f_txt_h.writelines([header, line1, line2])
            fn_gz = self._get_fn('.txt', True) + ".gz"
            with GzipFile(filename=fn_gz, mode="wb") as f_gz_h:
                self._set_fn(fn_gz, '.gz', True)
                f_gz_h.writelines([header, line1, line2])
            fn_gz_upper = self._get_fn('.txt', True) + ".GZ"
            with GzipFile(filename=fn_gz_upper, mode="wb") as f_gz_upper_h:
                self._set_fn(fn_gz_upper, '.GZ', True)
                f_gz_upper_h.writelines([header, line1, line2])
            fn_bz2 = self._get_fn('.txt', True) + '.bz2'
            with bz2.BZ2File(filename=fn_bz2, mode="wb") as f_bz2_h:
                self._set_fn(fn_bz2, '.bz2', True)
                f_bz2_h.writelines([header, line1, line2])
            # create sample txt, bz and bz2 without header
            with NamedTemporaryFile(mode='wb+', dir=self.tmp_dir, delete=False) as f_txt_nh:
                self._set_fn(f_txt_nh.name, '.txt', False)
                f_txt_nh.writelines([line1, line2])
            fn_gz = self._get_fn('.txt', False) + ".gz"
            with GzipFile(filename=fn_gz, mode="wb") as f_gz_nh:
                self._set_fn(fn_gz, '.gz', False)
                f_gz_nh.writelines([line1, line2])
            fn_gz_upper = self._get_fn('.txt', False) + ".GZ"
            with GzipFile(filename=fn_gz_upper, mode="wb") as f_gz_upper_nh:
                self._set_fn(fn_gz_upper, '.GZ', False)
                f_gz_upper_nh.writelines([line1, line2])
            fn_bz2 = self._get_fn('.txt', False) + '.bz2'
            with bz2.BZ2File(filename=fn_bz2, mode="wb") as f_bz2_nh:
                self._set_fn(fn_bz2, '.bz2', False)
                f_bz2_nh.writelines([line1, line2])
        # Base Exception so it catches Keyboard Interrupt
        except BaseException as e:
            logging.error(e)
            self.tearDown()

    def tearDown(self):
        try:
            shutil.rmtree(self.tmp_dir)
        except OSError as e:
            # ENOENT - no such file or directory
            if e.errno != errno.ENOENT:
                raise e

    # Helper method to create a dictionary of file names and
    # file types (file extension and header)
    def _set_fn(self, fn, ext, header):
        key = self._get_key(ext, header)
        self.fn[key] = fn

    # Helper method to fetch a file of a
    # certain format (file extension and header)
    def _get_fn(self, ext, header):
        key = self._get_key(ext, header)
        return self.fn[key]

    @staticmethod
    def _get_key(ext, header):
        key = ext + "_" + ('h' if header else 'nh')
        return key

    @staticmethod
    def _check_file_equality(fn_1, fn_2, ext):
        # gz files contain mtime and filename in the header that
        # causes filecmp to return False even if contents are identical
        # Hence decompress to test for equality
        if ext.lower() == '.gz':
            with GzipFile(fn_1, 'rb') as f_1, NamedTemporaryFile(mode='wb') as f_txt_1:
                with GzipFile(fn_2, 'rb') as f_2, NamedTemporaryFile(mode='wb') as f_txt_2:
                    shutil.copyfileobj(f_1, f_txt_1)
                    shutil.copyfileobj(f_2, f_txt_2)
                    f_txt_1.flush()
                    f_txt_2.flush()
                    return filecmp.cmp(f_txt_1.name, f_txt_2.name, shallow=False)
        else:
            return filecmp.cmp(fn_1, fn_2, shallow=False)

    def test_bad_parameters(self):
        self.kwargs['check_headers'] = True
        self.kwargs['headers'] = False
        self.assertRaisesRegex(AirflowException, "To check_headers.*", S3ToHiveTransfer, **self.kwargs)

    def test__get_top_row_as_list(self):
        self.kwargs['delimiter'] = '\t'
        fn_txt = self._get_fn('.txt', True)
        header_list = S3ToHiveTransfer(**self.kwargs). \
            _get_top_row_as_list(fn_txt)
        self.assertEqual(header_list, ['Sno', 'Some,Text'],
                         msg="Top row from file doesnt matched expected value")

        self.kwargs['delimiter'] = ','
        header_list = S3ToHiveTransfer(**self.kwargs). \
            _get_top_row_as_list(fn_txt)
        self.assertEqual(header_list, ['Sno\tSome', 'Text'],
                         msg="Top row from file doesnt matched expected value")

    def test__match_headers(self):
        self.kwargs['field_dict'] = OrderedDict([('Sno', 'BIGINT'),
                                                 ('Some,Text', 'STRING')])
        self.assertTrue(S3ToHiveTransfer(**self.kwargs).
                        _match_headers(['Sno', 'Some,Text']),
                        msg="Header row doesnt match expected value")
        # Testing with different column order
        self.assertFalse(S3ToHiveTransfer(**self.kwargs).
                         _match_headers(['Some,Text', 'Sno']),
                         msg="Header row doesnt match expected value")
        # Testing with extra column in header
        self.assertFalse(S3ToHiveTransfer(**self.kwargs).
                         _match_headers(['Sno', 'Some,Text', 'ExtraColumn']),
                         msg="Header row doesnt match expected value")

    def test__delete_top_row_and_compress(self):
        s32hive = S3ToHiveTransfer(**self.kwargs)
        # Testing gz file type
        fn_txt = self._get_fn('.txt', True)
        gz_txt_nh = s32hive._delete_top_row_and_compress(fn_txt,
                                                         '.gz',
                                                         self.tmp_dir)
        fn_gz = self._get_fn('.gz', False)
        self.assertTrue(self._check_file_equality(gz_txt_nh, fn_gz, '.gz'),
                        msg="gz Compressed file not as expected")
        # Testing bz2 file type
        bz2_txt_nh = s32hive._delete_top_row_and_compress(fn_txt,
                                                          '.bz2',
                                                          self.tmp_dir)
        fn_bz2 = self._get_fn('.bz2', False)
        self.assertTrue(self._check_file_equality(bz2_txt_nh, fn_bz2, '.bz2'),
                        msg="bz2 Compressed file not as expected")

    @unittest.skipIf(mock is None, 'mock package not present')
    @unittest.skipIf(mock_s3 is None, 'moto package not present')
    @mock.patch('airflow.providers.aws.operators.s3.HiveCliHook')
    @mock_s3
    def test_execute(self, mock_hiveclihook):
        conn = boto3.client('s3')
        conn.create_bucket(Bucket='bucket')

        # Testing txt, zip, bz2 files with and without header row
        for (ext, has_header) in product(['.txt', '.gz', '.bz2', '.GZ'], [True, False]):
            self.kwargs['headers'] = has_header
            self.kwargs['check_headers'] = has_header
            logging.info("Testing %s format %s header", ext, 'with' if has_header else 'without')
            self.kwargs['input_compressed'] = ext.lower() != '.txt'
            self.kwargs['s3_key'] = 's3://bucket/' + self.s3_key + ext
            ip_fn = self._get_fn(ext, self.kwargs['headers'])
            op_fn = self._get_fn(ext, False)

            # Upload the file into the Mocked S3 bucket
            conn.upload_file(ip_fn, 'bucket', self.s3_key + ext)

            # file parameter to HiveCliHook.load_file is compared
            # against expected file output
            mock_hiveclihook().load_file.side_effect = \
                lambda *args, **kwargs: self.assertTrue(
                    self._check_file_equality(args[0], op_fn, ext),
                    msg='{0} output file not as expected'.format(ext))
            # Execute S3ToHiveTransfer
            s32hive = S3ToHiveTransfer(**self.kwargs)
            s32hive.execute(None)

    @unittest.skipIf(mock is None, 'mock package not present')
    @unittest.skipIf(mock_s3 is None, 'moto package not present')
    @mock.patch('airflow.providers.aws.operators.s3.HiveCliHook')
    @mock_s3
    def test_execute_with_select_expression(self, mock_hiveclihook):
        conn = boto3.client('s3')
        conn.create_bucket(Bucket='bucket')

        select_expression = "SELECT * FROM S3Object s"
        bucket = 'bucket'

        # Only testing S3ToHiveTransfer calls S3Hook.select_key with
        # the right parameters and its execute method succeeds here,
        # since Moto doesn't support select_object_content as of 1.3.2.
        for (ext, has_header) in product(['.txt', '.gz', '.GZ'], [True, False]):
            input_compressed = ext.lower() != '.txt'
            key = self.s3_key + ext

            self.kwargs['check_headers'] = False
            self.kwargs['headers'] = has_header
            self.kwargs['input_compressed'] = input_compressed
            self.kwargs['select_expression'] = select_expression
            self.kwargs['s3_key'] = 's3://{0}/{1}'.format(bucket, key)

            ip_fn = self._get_fn(ext, has_header)

            # Upload the file into the Mocked S3 bucket
            conn.upload_file(ip_fn, bucket, key)

            input_serialization = {
                'CSV': {'FieldDelimiter': self.delimiter}
            }
            if input_compressed:
                input_serialization['CompressionType'] = 'GZIP'
            if has_header:
                input_serialization['CSV']['FileHeaderInfo'] = 'USE'

            # Confirm that select_key was called with the right params
            with mock.patch('airflow.hooks.S3_hook.S3Hook.select_key',
                            return_value="") as mock_select_key:
                # Execute S3ToHiveTransfer
                s32hive = S3ToHiveTransfer(**self.kwargs)
                s32hive.execute(None)

                mock_select_key.assert_called_once_with(
                    bucket_name=bucket, key=key,
                    expression=select_expression,
                    input_serialization=input_serialization
                )


class TestS3ToRedshiftTransfer(unittest.TestCase):

    @mock.patch("boto3.session.Session")
    @mock.patch("airflow.hooks.postgres_hook.PostgresHook.run")
    def test_execute(self, mock_run, mock_Session):
        access_key = "aws_access_key_id"
        secret_key = "aws_secret_access_key"
        mock_Session.return_value = Session(access_key, secret_key)

        schema = "schema"
        table = "table"
        s3_bucket = "bucket"
        s3_key = "key"
        copy_options = ""

        t = S3ToRedshiftTransfer(
            schema=schema,
            table=table,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            copy_options=copy_options,
            redshift_conn_id="redshift_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
            dag=None)
        t.execute(None)

        copy_query = """
            COPY {schema}.{table}
            FROM 's3://{s3_bucket}/{s3_key}/{table}'
            with credentials
            'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
            {copy_options};
        """.format(schema=schema,
                   table=table,
                   s3_bucket=s3_bucket,
                   s3_key=s3_key,
                   access_key=access_key,
                   secret_key=secret_key,
                   copy_options=copy_options)

        assert mock_run.call_count == 1
        assertEqualIgnoreMultipleSpaces(self, mock_run.call_args[0][0], copy_query)


class TestS3ToSFTPOperator(unittest.TestCase):
    TASK_ID = 'test_s3_to_sftp'
    BUCKET = 'test-s3-bucket'
    S3_KEY = 'test/test_1_file.csv'
    SFTP_PATH = '/tmp/remote_path.txt'
    SFTP_CONN_ID = 'ssh_default'
    S3_CONN_ID = 'aws_default'
    LOCAL_FILE_PATH = '/tmp/test_s3_upload'
    SFTP_MOCK_FILE = 'test_sftp_file.csv'
    S3_MOCK_FILES = 'test_1_file.csv'
    TEST_DAG_ID = 'unit_tests'
    DEFAULT_DATE = datetime(2018, 1, 1)

    @mock_s3
    def setUp(self):
        # Reset
        from airflow.settings import Session
        session = Session()
        tis = session.query(TaskInstance).filter_by(dag_id=self.TEST_DAG_ID)
        tis.delete()
        session.commit()
        session.close()

        from airflow.contrib.hooks.ssh_hook import SSHHook
        from airflow.providers.aws.hooks.s3 import S3Hook

        hook = SSHHook(ssh_conn_id='ssh_default')
        s3_hook = S3Hook('aws_default')
        hook.no_host_key_check = True
        args = {
            'owner': 'airflow',
            'start_date': self.DEFAULT_DATE,
        }
        dag = DAG(self.TEST_DAG_ID + 'test_schedule_dag_once', default_args=args)
        dag.schedule_interval = '@once'

        self.hook = hook
        self.s3_hook = s3_hook

        self.ssh_client = self.hook.get_conn()
        self.sftp_client = self.ssh_client.open_sftp()

        self.dag = dag
        self.s3_bucket = self.BUCKET
        self.sftp_path = self.SFTP_PATH
        self.s3_key = self.S3_KEY

    @mock_s3
    def test_s3_to_sftp_operation(self):
        # Setting
        conf.set("core", "enable_xcom_pickling", "True")
        test_remote_file_content = \
            "This is remote file content \n which is also multiline " \
            "another line here \n this is last line. EOF"

        # Test for creation of s3 bucket
        conn = boto3.client('s3')
        conn.create_bucket(Bucket=self.s3_bucket)
        self.assertTrue(self.s3_hook.check_for_bucket(self.s3_bucket))

        with open(self.LOCAL_FILE_PATH, 'w') as file:
            file.write(test_remote_file_content)
        self.s3_hook.load_file(self.LOCAL_FILE_PATH, self.s3_key, bucket_name=self.BUCKET)

        # Check if object was created in s3
        objects_in_dest_bucket = conn.list_objects(Bucket=self.s3_bucket,
                                                   Prefix=self.s3_key)
        # there should be object found, and there should only be one object found
        self.assertEqual(len(objects_in_dest_bucket['Contents']), 1)

        # the object found should be consistent with dest_key specified earlier
        self.assertEqual(objects_in_dest_bucket['Contents'][0]['Key'], self.s3_key)

        # get remote file to local
        run_task = S3ToSFTPOperator(
            s3_bucket=self.BUCKET,
            s3_key=self.S3_KEY,
            sftp_path=self.SFTP_PATH,
            sftp_conn_id=self.SFTP_CONN_ID,
            s3_conn_id=self.S3_CONN_ID,
            task_id=self.TASK_ID,
            dag=self.dag
        )
        self.assertIsNotNone(run_task)

        run_task.execute(None)

        # Check that the file is created remotely
        check_file_task = SSHOperator(
            task_id="test_check_file",
            ssh_hook=self.hook,
            command="cat {0}".format(self.sftp_path),
            do_xcom_push=True,
            dag=self.dag
        )
        self.assertIsNotNone(check_file_task)
        ti3 = TaskInstance(task=check_file_task, execution_date=timezone.utcnow())
        ti3.run()
        self.assertEqual(
            ti3.xcom_pull(task_ids='test_check_file', key='return_value').strip(),
            test_remote_file_content.encode('utf-8'))

        # Clean up after finishing with test
        conn.delete_object(Bucket=self.s3_bucket, Key=self.s3_key)
        conn.delete_bucket(Bucket=self.s3_bucket)
        self.assertFalse((self.s3_hook.check_for_bucket(self.s3_bucket)))

    def delete_remote_resource(self):
        # check the remote file content
        remove_file_task = SSHOperator(
            task_id="test_check_file",
            ssh_hook=self.hook,
            command="rm {0}".format(self.sftp_path),
            do_xcom_push=True,
            dag=self.dag
        )
        self.assertIsNotNone(remove_file_task)
        ti3 = TaskInstance(task=remove_file_task, execution_date=timezone.utcnow())
        ti3.run()

    def tearDown(self):
        self.delete_remote_resource()
