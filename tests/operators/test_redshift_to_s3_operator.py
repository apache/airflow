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

import mock
import re
import unittest

from boto3.session import Session
from airflow.operators.redshift_to_s3_operator import RedshiftToS3Transfer


class TestRedshiftToS3Transfer(unittest.TestCase):

    @mock.patch("boto3.session.Session")
    @mock.patch("airflow.hooks.postgres_hook.PostgresHook.run")
    @mock.patch("airflow.hooks.postgres_hook.PostgresHook.get_conn")
    def test_execute(self, mock_get_conn, mock_run, mock_Session):
        column_name = "col"
        cur = mock.MagicMock()
        cur.fetchall.return_value = [(column_name, )]
        mock_get_conn.return_value.cursor.return_value = cur

        access_key = "aws_access_key_id"
        secret_key = "aws_secret_access_key"
        mock_Session.return_value = Session(access_key, secret_key)

        schema = "schema"
        table = "table"
        s3_bucket = "bucket"
        s3_key = "key"
        unload_options = ""

        t = RedshiftToS3Transfer(
            schema=schema,
            table=table,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            unload_options=unload_options,
            redshift_conn_id="redshift_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
            dag=None)
        t.execute(None)

        columns_query = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = '{0}'
            AND   table_name = '{1}'
            ORDER BY ordinal_position
            """.format(schema, table)

        unload_query = """
            UNLOAD ('SELECT \\'{0}\\'
                     UNION ALL
                     SELECT CAST({0} AS text) AS {0}
                     FROM {1}.{2}
                     ORDER BY 1 DESC')
            TO 's3://{3}/{4}/{2}_'
            with credentials
            'aws_access_key_id={5};aws_secret_access_key={6}'
            {7};
            """.format(column_name, schema, table,
                       s3_bucket, s3_key, access_key,
                       secret_key, unload_options)

        def _trim(s):
            return re.sub("\s+", " ", s.strip())

        self.assertEqual(_trim(cur.execute.call_args[0][0]),
                         _trim(columns_query))
        cur.execute.assert_called_once()

        self.assertEqual(_trim(mock_run.call_args[0][0]),
                         _trim(unload_query))
        mock_run.assert_called_once()
