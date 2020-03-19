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

import gzip as gz
import io
import mock
import tempfile
import unittest

from botocore.exceptions import NoCredentialsError

try:
    from airflow.hooks.S3_hook import S3Hook
except ImportError:
    S3Hook = None  # type: ignore

try:
    import boto3
    from moto import mock_s3
except ImportError:
    mock_s3 = None


@unittest.skipIf(S3Hook is None,
                 "Skipping test because S3Hook is not available")
@unittest.skipIf(mock_s3 is None,
                 "Skipping test because moto.mock_s3 is not available")
class TestS3Hook(unittest.TestCase):

    def setUp(self):
        self.s3_test_url = "s3://test/this/is/not/a-real-key.txt"

    def test_parse_s3_url(self):
        parsed = S3Hook.parse_s3_url(self.s3_test_url)
        self.assertEqual(parsed,
                         ("test", "this/is/not/a-real-key.txt"),
                         "Incorrect parsing of the s3 url")

    @mock_s3
    def test_check_for_bucket(self):
        hook = S3Hook(aws_conn_id=None)
        b = hook.get_bucket('bucket')
        b.create()

        self.assertTrue(hook.check_for_bucket('bucket'))
        self.assertFalse(hook.check_for_bucket('not-a-bucket'))

    def test_check_for_bucket_raises_error_with_invalid_conn_id(self):
        hook = S3Hook(aws_conn_id="does_not_exist")

        with self.assertRaises(NoCredentialsError):
            hook.check_for_bucket('bucket')

    @mock_s3
    def test_get_bucket(self):
        hook = S3Hook(aws_conn_id=None)
        b = hook.get_bucket('bucket')
        self.assertIsNotNone(b)

    @mock_s3
    def test_create_bucket_default_region(self):
        hook = S3Hook(aws_conn_id=None)
        hook.create_bucket(bucket_name='new_bucket')
        b = hook.get_bucket('new_bucket')
        self.assertIsNotNone(b)

    @mock_s3
    def test_create_bucket_us_standard_region(self):
        hook = S3Hook(aws_conn_id=None)
        hook.create_bucket(bucket_name='new_bucket', region_name='us-east-1')
        b = hook.get_bucket('new_bucket')
        self.assertIsNotNone(b)
        region = b.meta.client.get_bucket_location(Bucket=b.name).get('LocationConstraint', None)
        # https://github.com/spulec/moto/pull/1961
        # If location is "us-east-1", LocationConstraint should be None
        self.assertIsNone(region)

    @mock_s3
    def test_create_bucket_other_region(self):
        hook = S3Hook(aws_conn_id=None)
        hook.create_bucket(bucket_name='new_bucket', region_name='us-east-2')
        b = hook.get_bucket('new_bucket')
        self.assertIsNotNone(b)
        region = b.meta.client.get_bucket_location(Bucket=b.name).get('LocationConstraint', None)
        self.assertEqual(region, 'us-east-2')

    @mock_s3
    def test_check_for_prefix(self):
        hook = S3Hook(aws_conn_id=None)
        b = hook.get_bucket('bucket')
        b.create()
        b.put_object(Key='a', Body=b'a')
        b.put_object(Key='dir/b', Body=b'b')

        self.assertTrue(hook.check_for_prefix('bucket', prefix='dir/', delimiter='/'))
        self.assertFalse(hook.check_for_prefix('bucket', prefix='a', delimiter='/'))

    @mock_s3
    def test_list_prefixes(self):
        hook = S3Hook(aws_conn_id=None)
        b = hook.get_bucket('bucket')
        b.create()
        b.put_object(Key='a', Body=b'a')
        b.put_object(Key='dir/b', Body=b'b')

        self.assertIsNone(hook.list_prefixes('bucket', prefix='non-existent/'))
        self.assertListEqual(['dir/'], hook.list_prefixes('bucket', delimiter='/'))
        self.assertListEqual(['a'], hook.list_keys('bucket', delimiter='/'))
        self.assertListEqual(['dir/b'], hook.list_keys('bucket', prefix='dir/'))

    @mock_s3
    def test_list_prefixes_paged(self):
        hook = S3Hook(aws_conn_id=None)
        b = hook.get_bucket('bucket')
        b.create()

        # we dont need to test the paginator
        # that's covered by boto tests
        keys = ["%s/b" % i for i in range(2)]
        dirs = ["%s/" % i for i in range(2)]
        for key in keys:
            b.put_object(Key=key, Body=b'a')

        self.assertListEqual(sorted(dirs),
                             sorted(hook.list_prefixes('bucket', delimiter='/',
                                                       page_size=1)))

    @mock_s3
    def test_list_keys(self):
        hook = S3Hook(aws_conn_id=None)
        b = hook.get_bucket('bucket')
        b.create()
        b.put_object(Key='a', Body=b'a')
        b.put_object(Key='dir/b', Body=b'b')

        self.assertIsNone(hook.list_keys('bucket', prefix='non-existent/'))
        self.assertListEqual(['a', 'dir/b'], hook.list_keys('bucket'))
        self.assertListEqual(['a'], hook.list_keys('bucket', delimiter='/'))
        self.assertListEqual(['dir/b'], hook.list_keys('bucket', prefix='dir/'))

    @mock_s3
    def test_list_keys_paged(self):
        hook = S3Hook(aws_conn_id=None)
        b = hook.get_bucket('bucket')
        b.create()

        keys = [str(i) for i in range(2)]
        for key in keys:
            b.put_object(Key=key, Body=b'a')

        self.assertListEqual(sorted(keys),
                             sorted(hook.list_keys('bucket', delimiter='/',
                                                   page_size=1)))

    @mock_s3
    def test_check_for_key(self):
        hook = S3Hook(aws_conn_id=None)
        b = hook.get_bucket('bucket')
        b.create()
        b.put_object(Key='a', Body=b'a')

        self.assertTrue(hook.check_for_key('a', 'bucket'))
        self.assertTrue(hook.check_for_key('s3://bucket//a'))
        self.assertFalse(hook.check_for_key('b', 'bucket'))
        self.assertFalse(hook.check_for_key('s3://bucket//b'))

    def test_check_for_key_raises_error_with_invalid_conn_id(self):
        hook = S3Hook(aws_conn_id="does_not_exist")

        with self.assertRaises(NoCredentialsError):
            hook.check_for_key('a', 'bucket')

    @mock_s3
    def test_get_key(self):
        hook = S3Hook(aws_conn_id=None)
        b = hook.get_bucket('bucket')
        b.create()
        b.put_object(Key='a', Body=b'a')

        self.assertEqual(hook.get_key('a', 'bucket').key, 'a')
        self.assertEqual(hook.get_key('s3://bucket/a').key, 'a')

    @mock_s3
    def test_read_key(self):
        hook = S3Hook(aws_conn_id=None)
        conn = hook.get_conn()
        # We need to create the bucket since this is all in Moto's 'virtual'
        # AWS account
        conn.create_bucket(Bucket='mybucket')
        conn.put_object(Bucket='mybucket', Key='my_key', Body=b'Cont\xC3\xA9nt')

        self.assertEqual(hook.read_key('my_key', 'mybucket'), u'Contént')

    # As of 1.3.2, Moto doesn't support select_object_content yet.
    @mock.patch('airflow.contrib.hooks.aws_hook.AwsHook.get_client_type')
    def test_select_key(self, mock_get_client_type):
        mock_get_client_type.return_value.select_object_content.return_value = \
            {'Payload': [{'Records': {'Payload': b'Cont\xC3\xA9nt'}}]}
        hook = S3Hook(aws_conn_id=None)
        self.assertEqual(hook.select_key('my_key', 'mybucket'), u'Contént')

    @mock_s3
    def test_check_for_wildcard_key(self):
        hook = S3Hook(aws_conn_id=None)
        b = hook.get_bucket('bucket')
        b.create()
        b.put_object(Key='abc', Body=b'a')
        b.put_object(Key='a/b', Body=b'a')

        self.assertTrue(hook.check_for_wildcard_key('a*', 'bucket'))
        self.assertTrue(hook.check_for_wildcard_key('s3://bucket//a*'))
        self.assertTrue(hook.check_for_wildcard_key('abc', 'bucket'))
        self.assertTrue(hook.check_for_wildcard_key('s3://bucket//abc'))
        self.assertFalse(hook.check_for_wildcard_key('a', 'bucket'))
        self.assertFalse(hook.check_for_wildcard_key('s3://bucket//a'))
        self.assertFalse(hook.check_for_wildcard_key('b', 'bucket'))
        self.assertFalse(hook.check_for_wildcard_key('s3://bucket//b'))

    @mock_s3
    def test_get_wildcard_key(self):
        hook = S3Hook(aws_conn_id=None)
        b = hook.get_bucket('bucket')
        b.create()
        b.put_object(Key='abc', Body=b'a')
        b.put_object(Key='a/b', Body=b'a')

        # The boto3 Class API is _odd_, and we can't do an isinstance check as
        # each instance is a different class, so lets just check one property
        # on S3.Object. Not great but...
        self.assertEqual(hook.get_wildcard_key('a*', 'bucket').key, 'a/b')
        self.assertEqual(hook.get_wildcard_key('s3://bucket/a*').key, 'a/b')
        self.assertEqual(hook.get_wildcard_key('a*', 'bucket', delimiter='/').key, 'abc')
        self.assertEqual(hook.get_wildcard_key('s3://bucket/a*', delimiter='/').key, 'abc')
        self.assertEqual(hook.get_wildcard_key('abc', 'bucket', delimiter='/').key, 'abc')
        self.assertEqual(hook.get_wildcard_key('s3://bucket/abc', delimiter='/').key, 'abc')

        self.assertIsNone(hook.get_wildcard_key('a', 'bucket'))
        self.assertIsNone(hook.get_wildcard_key('s3://bucket/a'))
        self.assertIsNone(hook.get_wildcard_key('b', 'bucket'))
        self.assertIsNone(hook.get_wildcard_key('s3://bucket/b'))

    @mock_s3
    def test_load_string(self):
        hook = S3Hook(aws_conn_id=None)
        conn = hook.get_conn()
        # We need to create the bucket since this is all in Moto's 'virtual'
        # AWS account
        conn.create_bucket(Bucket="mybucket")

        hook.load_string(u"Contént", "my_key", "mybucket")
        resource = boto3.resource('s3').Object('mybucket', 'my_key')  # pylint: disable=no-member
        body = resource.get()['Body'].read()
        self.assertEqual(body, b'Cont\xC3\xA9nt')

    @mock_s3
    def test_load_string_acl(self):
        hook = S3Hook()
        conn = hook.get_conn()
        # We need to create the bucket since this is all in Moto's 'virtual'
        # AWS account
        conn.create_bucket(Bucket="mybucket")
        hook.load_string(u"Contént", "my_key", "mybucket",
                         acl_policy='public-read')
        response = boto3.client('s3').get_object_acl(Bucket="mybucket",
                                                     Key="my_key", RequestPayer='requester')
        assert ((response['Grants'][1]['Permission'] == 'READ') and
                (response['Grants'][0]['Permission'] == 'FULL_CONTROL'))

    @mock_s3
    def test_load_bytes(self):
        hook = S3Hook(aws_conn_id=None)
        conn = hook.get_conn()
        # We need to create the bucket since this is all in Moto's 'virtual'
        # AWS account
        conn.create_bucket(Bucket="mybucket")

        hook.load_bytes(b"Content", "my_key", "mybucket")
        resource = boto3.resource('s3').Object('mybucket', 'my_key')  # pylint: disable=no-member
        body = resource.get()['Body'].read()

        self.assertEqual(body, b'Content')

    @mock_s3
    def test_load_fileobj(self):
        hook = S3Hook(aws_conn_id=None)
        conn = hook.get_conn()
        # We need to create the bucket since this is all in Moto's 'virtual'
        # AWS account
        conn.create_bucket(Bucket="mybucket")
        with tempfile.TemporaryFile() as temp_file:
            temp_file.write(b"Content")
            temp_file.seek(0)
            hook.load_file_obj(temp_file, "my_key", "mybucket")
            resource = boto3.resource('s3').Object('mybucket', 'my_key')  # pylint: disable=no-member
            body = resource.get()['Body'].read()

            self.assertEqual(body, b'Content')

    @mock_s3
    def test_load_file_gzip(self):
        hook = S3Hook()
        conn = hook.get_conn()
        # We need to create the bucket since this is all in Moto's 'virtual'
        # AWS account
        conn.create_bucket(Bucket="mybucket")
        with tempfile.NamedTemporaryFile() as temp_file:
            temp_file.write(b"Content")
            temp_file.seek(0)
            hook.load_file(temp_file, "my_key", 'mybucket', gzip=True)
            resource = boto3.resource('s3').Object('mybucket', 'my_key')  # pylint: disable=no-member
            with gz.GzipFile(fileobj=io.BytesIO(resource.get()['Body'].read())) as gzfile:
                assert gzfile.read() == b'Content'

    @mock_s3
    def test_load_file_acl(self):
        hook = S3Hook()
        conn = hook.get_conn()
        # We need to create the bucket since this is all in Moto's 'virtual'
        # AWS account
        conn.create_bucket(Bucket="mybucket")
        with tempfile.NamedTemporaryFile() as temp_file:
            temp_file.write(b"Content")
            temp_file.seek(0)
            hook.load_file(temp_file, "my_key", 'mybucket', gzip=True,
                           acl_policy='public-read')
            response = boto3.client('s3').get_object_acl(Bucket='mybucket',
                                                         Key="my_key",
                                                         RequestPayer='requester')  # pylint: disable=no-member # noqa: E501 # pylint: disable=C0301
            assert ((response['Grants'][1]['Permission'] == 'READ') and
                    (response['Grants'][0]['Permission'] == 'FULL_CONTROL'))

    @mock_s3
    def test_copy_object_acl(self):
        hook = S3Hook()
        conn = hook.get_conn()
        # We need to create the bucket since this is all in Moto's 'virtual'
        # AWS account
        conn.create_bucket(Bucket="mybucket")
        with tempfile.NamedTemporaryFile() as temp_file:
            temp_file.write(b"Content")
            temp_file.seek(0)
            hook.load_file_obj(temp_file, "my_key", 'mybucket')
            hook.copy_object("my_key", "my_key", 'mybucket', 'mybucket')
            response = boto3.client('s3').get_object_acl(Bucket='mybucket',
                                                         Key="my_key",
                                                         RequestPayer='requester')  # pylint: disable=no-member # noqa: E501 # pylint: disable=C0301
            assert ((response['Grants'][0]['Permission'] == 'FULL_CONTROL') and
                    (len(response['Grants']) == 1))


if __name__ == '__main__':
    unittest.main()
