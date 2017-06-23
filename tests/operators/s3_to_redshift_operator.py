# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
try:
    from unittest.mock import MagicMock, patch
except ImportError:
    try:
        from mock import MagicMock, patch
    except ImportError:
        patch = None

from airflow.exceptions import AirflowException
from airflow.operators.s3_to_redshift_operator import S3ToRedshiftCopy

class S3ToRedshiftCopyTest(unittest.TestCase):
    def setUp(self):
        self.logging_patcher = patch(
            'airflow.operators.s3_to_redshift_operator.logging')
        self.logging_patcher.start()
        self.pg_hook_patcher = patch(
            'airflow.operators.s3_to_redshift_operator.PostgresHook')
        self.mock_pg_hook = self.pg_hook_patcher.start()
        self.s3_hook_patcher = patch(
            'airflow.operators.s3_to_redshift_operator.S3Hook')
        self.mock_s3_hook = self.s3_hook_patcher.start()

        self.mock_s3 = MagicMock()
        self.mock_s3_hook.return_value = self.mock_s3
        self.mock_s3.get_credentials.return_value = 'key', 'secret'
        self.mock_s3.check_for_key.return_value = True

    def tearDown(self):
        self.logging_patcher.stop()
        self.pg_hook_patcher.stop()
        self.s3_hook_patcher.stop()

    @unittest.skipIf(patch is None, 'mock package not present')
    def test_copy_from_s3(self):
        operator = S3ToRedshiftCopy(
            s3_bucket='bucket', s3_key='key', redshift_conn_id='conn_id',
            schema='schema', table='table', copy_options={'OPTION': 'VALUE'},
            task_id='unittest')
        operator.execute(None)

        expected_sql = ("COPY schema.table FROM 's3://bucket/key' CREDENTIALS "
                        "'aws_access_key_id=key;aws_secret_access_key=secret' "
                        "OPTION VALUE")
        self.mock_pg_hook.return_value.run.assert_called_once_with(
            expected_sql, True)

    @unittest.skipIf(patch is None, 'mock package not present')
    def test_copy_from_s3__option_flag(self):
        operator = S3ToRedshiftCopy(
            s3_bucket='bucket', s3_key='key', redshift_conn_id='conn_id',
            schema='schema', table='table',
            copy_options={'FLAG': None}, task_id='unittest')
        operator.execute(None)

        expected_sql = ("COPY schema.table FROM 's3://bucket/key' CREDENTIALS "
                        "'aws_access_key_id=key;aws_secret_access_key=secret' "
                        "FLAG")
        self.mock_pg_hook.return_value.run.assert_called_once_with(
            expected_sql, True)

    @unittest.skipIf(patch is None, 'mock package not present')
    def test_copy_from_s3__provided_col_names(self):
        operator = S3ToRedshiftCopy(
            s3_bucket='bucket', s3_key='key', redshift_conn_id='conn_id',
            schema='schema', table='table',
            column_names=['a', 'b', 'c'], copy_options={'OPTION': 'VALUE'},
            task_id='unittest')
        operator.execute(None)

        expected_sql = ("COPY schema.table (a,b,c) FROM 's3://bucket/key' "
                        "CREDENTIALS 'aws_access_key_id=key;"
                        "aws_secret_access_key=secret' OPTION VALUE")
        self.mock_pg_hook.return_value.run.assert_called_once_with(
            expected_sql, True)

    @unittest.skipIf(patch is None, 'mock package not present')
    @patch('airflow.operators.s3_to_redshift_operator.StringIO')
    def test_copy_from_s3__inferred_col_names(self, mock_stringio):
        mock_stringio.return_value.readline.return_value = 'a,b,c'

        operator = S3ToRedshiftCopy(
            s3_bucket='bucket', s3_key='key', redshift_conn_id='conn_id',
            schema='schema', table='table',
            infer_column_names=True, task_id='unittest')
        operator.execute(None)

        expected_sql = ("COPY schema.table (a,b,c) FROM 's3://bucket/key' "
                        "CREDENTIALS 'aws_access_key_id=key;"
                        "aws_secret_access_key=secret' "
                        "IGNOREHEADER 1")
        self.mock_pg_hook.return_value.run.assert_called_once_with(
            expected_sql, True)

    @unittest.skipIf(patch is None, 'mock package not present')
    def test_copy_from_s3__infer_and_provided_col_names(self):
        with self.assertRaises(AirflowException):
            S3ToRedshiftCopy(
                s3_bucket='bucket', s3_key='key', redshift_conn_id='conn_id',
                schema='schema', table='table',
                column_names=['a', 'b', 'c'], infer_column_names=True,
                task_id='unittest')


if __name__ == '__main__':
    unittest.main()
