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

from airflow.contrib.hooks.aws_glue_catalog_hook import AwsGlueCatalogHook

try:
    from moto import mock_glue
except ImportError:
    mock_glue = None

try:
    from unittest import mock
except ImportError:
    import mock


@unittest.skipIf(mock_glue is None,
                 "Skipping test because moto.mock_glue is not available")
class TestAwsGlueCatalogHook(unittest.TestCase):

    @mock_glue
    def test_get_conn_returns_a_boto3_connection(self):
        hook = AwsGlueCatalogHook(region_name="us-east-1")
        self.assertIsNotNone(hook.get_conn())

    @mock_glue
    def test_conn_id(self):
        hook = AwsGlueCatalogHook(aws_conn_id='my_aws_conn_id', region_name="us-east-1")
        self.assertEqual(hook.aws_conn_id, 'my_aws_conn_id')

    @mock_glue
    def test_region(self):
        hook = AwsGlueCatalogHook(region_name="us-west-2")
        self.assertEqual(hook.region_name, 'us-west-2')

    @mock_glue
    @mock.patch.object(AwsGlueCatalogHook, 'get_conn')
    def test_get_partitions_empty(self, mock_get_conn):
        response = set()
        mock_get_conn.get_paginator.paginate.return_value = response
        hook = AwsGlueCatalogHook(region_name="us-east-1")

        self.assertEqual(hook.get_partitions('db', 'tbl'), set())

    @mock_glue
    @mock.patch.object(AwsGlueCatalogHook, 'get_conn')
    def test_get_partitions(self, mock_get_conn):
        response = [{
            'Partitions': [{
                'Values': ['2015-01-01']
            }]
        }]
        mock_paginator = mock.Mock()
        mock_paginator.paginate.return_value = response
        mock_conn = mock.Mock()
        mock_conn.get_paginator.return_value = mock_paginator
        mock_get_conn.return_value = mock_conn
        hook = AwsGlueCatalogHook(region_name="us-east-1")
        result = hook.get_partitions('db',
                                     'tbl',
                                     expression='foo=bar',
                                     page_size=2,
                                     max_items=3)

        self.assertEqual(result, set([('2015-01-01',)]))
        mock_conn.get_paginator.assert_called_once_with('get_partitions')
        mock_paginator.paginate.assert_called_once_with(DatabaseName='db',
                                                        TableName='tbl',
                                                        Expression='foo=bar',
                                                        PaginationConfig={
                                                            'PageSize': 2,
                                                            'MaxItems': 3})

    @mock_glue
    @mock.patch.object(AwsGlueCatalogHook, 'get_partitions')
    def test_check_for_partition(self, mock_get_partitions):
        mock_get_partitions.return_value = set([('2018-01-01',)])
        hook = AwsGlueCatalogHook(region_name="us-east-1")

        self.assertTrue(hook.check_for_partition('db', 'tbl', 'expr'))
        mock_get_partitions.assert_called_once_with('db', 'tbl', 'expr', max_items=1)

    @mock_glue
    @mock.patch.object(AwsGlueCatalogHook, 'get_partitions')
    def test_check_for_partition_false(self, mock_get_partitions):
        mock_get_partitions.return_value = set()
        hook = AwsGlueCatalogHook(region_name="us-east-1")

        self.assertFalse(hook.check_for_partition('db', 'tbl', 'expr'))


if __name__ == '__main__':
    unittest.main()
