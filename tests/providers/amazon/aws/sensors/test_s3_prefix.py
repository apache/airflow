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

from airflow.providers.amazon.aws.sensors.s3_prefix import S3PrefixSensor


@mock.patch('airflow.providers.amazon.aws.sensors.s3_prefix.S3Hook')
def test_poke(mock_hook):
    op = S3PrefixSensor(task_id='s3_prefix', bucket_name='bucket', prefix='prefix')

    mock_hook.return_value.check_for_prefix.return_value = False
    assert not op.poke({})
    mock_hook.return_value.check_for_prefix.assert_called_once_with(
        prefix='prefix', delimiter='/', bucket_name='bucket'
    )

    mock_hook.return_value.check_for_prefix.return_value = True
    assert op.poke({})


@mock.patch('airflow.providers.amazon.aws.sensors.s3_prefix.S3Hook')
def test_poke_should_check_multiple_prefixes(mock_hook):
    op = S3PrefixSensor(task_id='s3_prefix', bucket_name='bucket', prefix=['prefix1', 'prefix2'])

    mock_hook.return_value.check_for_prefix.return_value = False
    assert not op.poke({}), "poke returns false when the prefixes do not exist"

    mock_hook.return_value.check_for_prefix.assert_has_calls(
        calls=[
            call(prefix='prefix1', delimiter='/', bucket_name='bucket'),
        ]
    )

    mock_hook.return_value.check_for_prefix.side_effect = [True, False]
    assert not op.poke({}), "poke returns false when only some of the prefixes exist"

    mock_hook.return_value.check_for_prefix.side_effect = [True, True]
    assert op.poke({}), "poke returns true when both prefixes exist"

    mock_hook.return_value.check_for_prefix.assert_has_calls(
        calls=[
            call(prefix='prefix1', delimiter='/', bucket_name='bucket'),
            call(prefix='prefix2', delimiter='/', bucket_name='bucket'),
        ]
    )
