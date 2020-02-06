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

import pytest

from airflow import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

try:
    from moto import mock_s3
except ImportError:
    mock_s3 = None


@pytest.mark.skipif(mock_s3 is None, reason='moto package not present')
class TestAwsS3Hook:

    @mock_s3
    def test_get_conn(self):
        hook = S3Hook()
        assert hook.get_conn() is not None

    def test_delete_objects_key_does_not_exist(self, s3_bucket):
        hook = S3Hook()
        with pytest.raises(AirflowException) as err:
            hook.delete_objects(bucket=s3_bucket, keys=['key-1'])

        assert isinstance(err.value, AirflowException)
        assert str(err.value) == "Errors when deleting: ['key-1']"

    def test_delete_objects_one_key(self, mocked_s3_res, s3_bucket):
        key = 'key-1'
        mocked_s3_res.Object(s3_bucket, key).put(Body=b'Data')
        hook = S3Hook()
        hook.delete_objects(bucket=s3_bucket, keys=[key])
        assert [o.key for o in mocked_s3_res.Bucket(s3_bucket).objects.all()] == []

    def test_delete_objects_many_keys(self, mocked_s3_res, s3_bucket):
        num_keys_to_remove = 1001
        keys = []
        for index in range(num_keys_to_remove):
            key = 'key-{}'.format(index)
            mocked_s3_res.Object(s3_bucket, key).put(Body=b'Data')
            keys.append(key)

        assert sum(1 for _ in mocked_s3_res.Bucket(s3_bucket).objects.all()) == num_keys_to_remove
        hook = S3Hook()
        hook.delete_objects(bucket=s3_bucket, keys=keys)
        assert [o.key for o in mocked_s3_res.Bucket(s3_bucket).objects.all()] == []
