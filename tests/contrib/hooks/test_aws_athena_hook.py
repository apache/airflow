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

import copy
import unittest

import boto3

try:
    from airflow.contrib.hooks.aws_athena_hook import AWSAthenaHelpers
except ImportError:
    AWSAthenaHelpers = None

try:
    from moto import mock_glue
except ImportError:
    mock_glue = None

try:
    from moto import mock_s3
except ImportError:
    mock_s3 = None

REGION_NAME = 'us-east-1'
BUCKET_NAME = 'mybucket'

TABLE_INPUT_TEMPLATE = {
    "Name": "",
    "StorageDescriptor": {
        "Columns": [
            {
                "Name": "foo",
                "Type": "string",
                "Comment": "string"
            },
            {
                "Name": "bar",
                "Type": "string",
                "Comment": "string"
            }
        ],
        "Location": "",
    }
}


@unittest.skipIf(AWSAthenaHelpers is None,
                 "Skipping test because AWSAthenaHelpers is not available")
@unittest.skipIf(mock_glue is None,
                 "Skipping test because moto.mock_glue is not available")
@unittest.skipIf(mock_s3 is None,
                 "Skipping test because moto.mock_s3 is not available")
class TestAWSAthenaHelpers(unittest.TestCase):

    @mock_s3
    @mock_glue
    def setUp(self):
        self.s3_location = 's3://bucket/mytable'
        self.database_name = 'mydb'
        self.table_name = 'mytable'
        self.helpers = AWSAthenaHelpers(aws_conn_id=None, region_name=REGION_NAME)
        self.s3_hook = self.helpers.get_s3_hook()
        self.glue_hook = self.helpers.get_glue_hook()
        self.glue_client = boto3.client('glue', REGION_NAME)

    def test_get_conn_returns_a_connection(self):
        conn = self.helpers.get_conn()
        self.assertIsNotNone(conn)

    def test_get_s3_hook_returns_a_connection(self):
        s3_hook = self.helpers.get_s3_hook()
        self.assertIsNotNone(s3_hook)

    def test_get_glue_hook_returns_a_connection(self):
        glue_hook = self.helpers.get_glue_hook()
        self.assertIsNotNone(glue_hook)

    def test_aws_conn_id(self):
        self.assertEqual(self.helpers.aws_conn_id, None)

    def test_aws_region_name(self):
        self.assertEqual(self.helpers.region_name, REGION_NAME)

    @mock_s3
    @mock_glue
    def test_run_insert_into_table_append_success(self):
        tables = {
            'src_table': {
                'db': 'src_db',
                'table': 'src_table',
                'partition_keys': [],
                'file': 'src_data.txt'
            },
            'dest_table': {
                'db': 'dst_db',
                'table': 'dst_table',
                'partition_keys': [],
                'file': 'dst_data.txt'
            }
        }

        glue_client = boto3.client('glue', REGION_NAME)
        s3_hook = self.helpers.get_s3_hook()

        for table in tables:
            self._create_table_with_data(
                glue_client,
                s3_hook,
                database_name=tables[table]['db'],
                table_name=tables[table]['table'],
                filename=tables[table]['file']
            )

        self.helpers.run_insert_into_table(src_db='src_db', src_table='src_table',
                                           dst_db='dst_db', dst_table='dst_table',
                                           mode='append')

        self.assertListEqual(sorted(['src_db/', 'dst_db/']),
                             sorted(self.s3_hook.list_prefixes(BUCKET_NAME, delimiter='/')))
        self.assertListEqual(['src_db/src_table/src_data.txt'],
                             self.s3_hook.list_keys(BUCKET_NAME, prefix='src_db/src_table'))
        self.assertListEqual(sorted(['dst_db/dst_table/src_data.txt', 'dst_db/dst_table/dst_data.txt']),
                             sorted(self.s3_hook.list_keys(BUCKET_NAME, prefix='dst_db/dst_table')))

    @mock_s3
    @mock_glue
    def test_run_insert_into_table_overwrite_success(self):
        tables = {
            'src_table': {
                'db': 'src_db',
                'table': 'src_table',
                'partition_keys': [],
                'file': 'src_data.txt'
            },
            'dest_table': {
                'db': 'dst_db',
                'table': 'dst_table',
                'partition_keys': [],
                'file': 'dst_data.txt'
            }
        }

        glue_client = boto3.client('glue', REGION_NAME)
        s3_hook = self.helpers.get_s3_hook()

        for table in tables:
            self._create_table_with_data(
                glue_client,
                s3_hook,
                database_name=tables[table]['db'],
                table_name=tables[table]['table'],
                filename=tables[table]['file']
            )

        self.helpers.run_insert_into_table(src_db='src_db', src_table='src_table',
                                           dst_db='dst_db', dst_table='dst_table',
                                           mode='overwrite')

        self.assertListEqual(sorted(['src_db/', 'dst_db/']),
                             sorted(self.s3_hook.list_prefixes(BUCKET_NAME, delimiter='/')))
        self.assertListEqual(['src_db/src_table/src_data.txt'],
                             self.s3_hook.list_keys(BUCKET_NAME, prefix='src_db/src_table'))
        self.assertListEqual(sorted(['dst_db/dst_table/src_data.txt']),
                             sorted(self.s3_hook.list_keys(BUCKET_NAME, prefix='dst_db/dst_table')))

    @mock_s3
    @mock_glue
    def test_run_insert_into_table_not_empty_failed(self):
        tables = {
            'src_table': {
                'db': 'src_db',
                'table': 'src_table',
                'partition_keys': [],
                'file': 'src_data.txt'
            },
            'dest_table': {
                'db': 'dst_db',
                'table': 'dst_table',
                'partition_keys': [],
                'file': 'dst_data.txt'
            }
        }

        glue_client = boto3.client('glue', REGION_NAME)
        s3_hook = self.helpers.get_s3_hook()

        for table in tables:
            self._create_table_with_data(
                glue_client,
                s3_hook,
                database_name=tables[table]['db'],
                table_name=tables[table]['table'],
                filename=tables[table]['file']
            )

        with self.assertRaises(Exception):
            self.helpers.run_insert_into_table(src_db='src_db', src_table='src_table',
                                               dst_db='dst_db', dst_table='dst_table',
                                               mode='error')

    @mock_s3
    @mock_glue
    def test_run_insert_into_partitioned_table_success(self):
        tables = {
            'src_table': {
                'db': 'src_db',
                'table': 'src_table',
                'partition_keys': [{
                    'Name': 'dt',
                    'Type': 'string',
                    'Comment': 'string'
                }],
                'partition_values': ['2019-05-28', '2019-09-18'],
                'file': 'src_data.txt'
            },
            'dest_table': {
                'db': 'dst_db',
                'table': 'dst_table',
                'partition_keys': [{
                    'Name': 'dt',
                    'Type': 'string',
                    'Comment': 'string'
                }],
                'partition_values': ['2019-05-28', '2019-09-18'],
                'file': 'dst_data.txt'
            }
        }

        glue_client = boto3.client('glue', REGION_NAME)
        s3_hook = self.helpers.get_s3_hook()

        for table in tables:
            self._create_table_with_data(
                glue_client,
                s3_hook,
                tables[table]['db'],
                tables[table]['table'],
                tables[table]['partition_keys'],
                tables[table]['partition_values'],
                tables[table]['file']
            )

        self.assertListEqual(sorted(['dst_db/dst_table/dt=2019-05-28/dst_data.txt',
                                    'dst_db/dst_table/dt=2019-09-18/dst_data.txt']),
                             sorted(self.s3_hook.list_keys(BUCKET_NAME, prefix='dst_db/dst_table')))

        self.assertListEqual(sorted(['src_db/src_table/dt=2019-05-28/src_data.txt',
                                    'src_db/src_table/dt=2019-09-18/src_data.txt']),
                             sorted(self.s3_hook.list_keys(BUCKET_NAME, prefix='src_db/src_table')))

        self.helpers.run_insert_into_table(src_db='src_db', src_table='src_table',
                                           dst_db='dst_db', dst_table='dst_table',
                                           mode='append')

        self.assertListEqual(sorted(['dst_db/dst_table/dt=2019-05-28/src_data.txt',
                                     'dst_db/dst_table/dt=2019-09-18/src_data.txt',
                                     'dst_db/dst_table/dt=2019-05-28/dst_data.txt',
                                     'dst_db/dst_table/dt=2019-09-18/dst_data.txt']),
                             sorted(self.s3_hook.list_keys(BUCKET_NAME, prefix='dst_db/dst_table')))

    @staticmethod
    def _create_table_input(database_name, table_name, columns=[], partition_keys=[]):
        table_input = copy.deepcopy(TABLE_INPUT_TEMPLATE)
        table_input['Name'] = table_name
        table_input['PartitionKeys'] = partition_keys
        table_input['StorageDescriptor']['Columns'] = columns
        table_input['StorageDescriptor']['Location'] = 's3://{}/{}/{}'.format(
            BUCKET_NAME,
            database_name,
            table_name
        )
        return table_input

    def _create_table_with_data(self, glue_client, s3_hook, database_name, table_name,
                                partition_keys=[], partition_values=[], filename=None, **kwargs):

        table_input = self._create_table_input(database_name=database_name,
                                               table_name=table_name,
                                               partition_keys=partition_keys, **kwargs)

        glue_client.create_database(
            DatabaseInput={
                'Name': database_name
            }
        )
        glue_client.create_table(
            DatabaseName=database_name,
            TableInput=table_input
        )

        if partition_keys:
            s3_hook.create_bucket(bucket_name=BUCKET_NAME)
            partition_key_names = [key['Name'] for key in partition_keys]
            partition_paths = self._generate_partition_path(partition_key_names, partition_values)

            for partition_path in partition_paths:
                s3_hook.load_string(u'test_string',
                                    key='{}/{}/{}/{}'.format(database_name, table_name,
                                                             partition_path, filename),
                                    bucket_name=BUCKET_NAME)

        else:
            s3_hook.create_bucket(bucket_name=BUCKET_NAME)
            s3_hook.load_string(u'test_string',
                                key='{}/{}/{}'.format(database_name, table_name, filename),
                                bucket_name=BUCKET_NAME)

    @staticmethod
    def _generate_partition_path(partitioned_by, partition_values):
        """
        generate the s3 path from partition key and ots value.
        :param partitioned_by: the list of partition keys
        :type: partitioned_by: list
        :param partition_values: the list of partition values
        :type partition_values: the list of tuple. For example:
            ``[('2018-01-01','1'), ('2018-01-01','2')]``
        :return:
        """
        partition_paths = []

        for value in partition_values:
            if len(partitioned_by) != len([value]):
                raise ValueError('lens of partition_key_name should be equal to partition_value.')

            partition_path = []
            for k, v in enumerate([value]):
                partition_path.append('{}={}'.format(partitioned_by[k], v))
            partition_paths.append('/'.join(partition_path))

        return partition_paths
