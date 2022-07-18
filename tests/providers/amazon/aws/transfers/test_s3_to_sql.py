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
from unittest.mock import patch

import pytest
from sqlalchemy import or_

from airflow import configuration, models
from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator
from airflow.utils import db
from airflow.utils.session import create_session


class TestS3ToSqlTransfer(unittest.TestCase):
    def setUp(self):
        configuration.conf.load_test_config()

        db.merge_conn(
            models.Connection(
                conn_id='s3_test',
                conn_type='s3',
                schema='test',
                extra='{"aws_access_key_id": "aws_access_key_id", "aws_secret_access_key":'
                ' "aws_secret_access_key"}',
            )
        )

        db.merge_conn(
            models.Connection(
                conn_id='mysql_test',
                conn_type='mysql',
                host='some.host.com',
                schema='test_db',
                login='user',
                password='password',
            )
        )

        self.s3_to_sql_transfer_kwargs = {
            's3_key': 'test/s3_to_mysql_test.csv',
            'destination_table': 'mysql_table',
            'file_format': 'csv',
            'file_options': {},
            'source_conn_id': 's3_test',
            'destination_conn_id': 'mysql_test',
            'preoperator': None,
            'task_id': 'task_id',
            'insert_args': None,
            'dag': None,
        }

    @patch('airflow.providers.amazon.aws.transfers.s3_to_sql.S3Hook.download_file')
    @patch('airflow.providers.amazon.aws.transfers.s3_to_sql.pandas.read_csv')
    @patch('airflow.providers.amazon.aws.transfers.s3_to_sql.BaseHook.get_hook')
    @patch('airflow.providers.amazon.aws.transfers.s3_to_sql.os.remove')
    def test_execute(self, mock_remove, mock_get_hook, mock_read_file, mock_download_file):
        S3ToSqlOperator(**self.s3_to_sql_transfer_kwargs).execute({})

        mock_download_file.assert_called_once_with(key=self.s3_to_sql_transfer_kwargs['s3_key'])
        mock_read_file.assert_called_once_with(
            mock_download_file.return_value, **self.s3_to_sql_transfer_kwargs['file_options']
        )
        mock_get_hook.assert_called_once_with(self.s3_to_sql_transfer_kwargs['destination_conn_id'])
        mock_remove.assert_called_once_with(mock_download_file.return_value)

    @patch('airflow.providers.amazon.aws.transfers.s3_to_sql.S3Hook.download_file')
    @patch('airflow.providers.amazon.aws.transfers.s3_to_sql.pandas.read_csv')
    @patch('airflow.providers.amazon.aws.transfers.s3_to_sql.BaseHook.get_hook')
    @patch('airflow.providers.amazon.aws.transfers.s3_to_sql.os.remove')
    def test_execute_exception(self, mock_remove, mock_get_hook, mock_read_file, mock_download_file):
        mock_read_file.side_effect = Exception
        with pytest.raises(Exception):
            S3ToSqlOperator(**self.s3_to_sql_transfer_kwargs).execute({})

        mock_download_file.assert_called_once_with(key=self.s3_to_sql_transfer_kwargs['s3_key'])
        mock_read_file.assert_called_once_with(
            mock_download_file.return_value, **self.s3_to_sql_transfer_kwargs['file_options']
        )
        mock_get_hook.assert_called_once_with(self.s3_to_sql_transfer_kwargs['destination_conn_id'])
        mock_remove.assert_called_once_with(mock_download_file.return_value)

    def tearDown(self):
        with create_session() as session:
            (
                session.query(models.Connection)
                .filter(
                    or_(models.Connection.conn_id == 's3_test', models.Connection.conn_id == 'mysql_test')
                )
                .delete()
            )
