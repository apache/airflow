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

from unittest import mock
from unittest.mock import call

import pytest

from airflow.providers.amazon.aws.sensors.s3 import S3PrefixSensor


def test_deprecation_warnings_generated():
    with pytest.warns(expected_warning=DeprecationWarning):
        S3PrefixSensor(task_id='s3_prefix', bucket_name='bucket', prefix='prefix')


@mock.patch('airflow.providers.amazon.aws.sensors.s3.S3Hook.head_object')
def test_poke(mock_head_object):
    op = S3PrefixSensor(task_id='s3_prefix', bucket_name='bucket', prefix='prefix')

    mock_head_object.return_value = None
    assert not op.poke({})
    mock_head_object.assert_called_once_with('prefix/', 'bucket')

    mock_head_object.return_value = {'ContentLength': 0}
    assert op.poke({})


@mock.patch('airflow.providers.amazon.aws.sensors.s3.S3Hook.head_object')
def test_poke_should_check_multiple_prefixes(mock_head_object):
    op = S3PrefixSensor(task_id='s3_prefix', bucket_name='bucket', prefix=['prefix1', 'prefix2/'])

    mock_head_object.return_value = None
    assert not op.poke({}), "poke returns false when the prefixes do not exist"

    mock_head_object.assert_has_calls(
        calls=[
            call('prefix1/', 'bucket'),
        ]
    )

    mock_head_object.side_effect = [{'ContentLength': 0}, None]
    assert not op.poke({}), "poke returns false when only some of the prefixes exist"

    mock_head_object.side_effect = [{'ContentLength': 0}, {'ContentLength': 0}]
    assert op.poke({}), "poke returns true when both prefixes exist"

    mock_head_object.assert_has_calls(
        calls=[
            call('prefix1/', 'bucket'),
            call('prefix2/', 'bucket'),
        ]
    )
