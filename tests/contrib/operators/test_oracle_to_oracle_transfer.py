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

import unittest
from airflow.contrib.operators.oracle_to_oracle_transfer \
    import OracleToOracleTransfer
from tests.compat import MagicMock


class OracleToOracleTransferTest(unittest.TestCase):

    @staticmethod
    def test_execute():
        oracle_destination_conn_id = 'oracle_destination_conn_id'
        destination_table = 'destination_table'
        oracle_source_conn_id = 'oracle_source_conn_id'
        source_sql = "select sysdate from dual where trunc(sysdate) = :p_data"
        source_sql_params = {':p_data': "2018-01-01"}
        rows_chunk = 5000
        cursor_description = [
            ('id', "<class 'cx_Oracle.NUMBER'>", 39, None, 38, 0, 0),
            ('description', "<class 'cx_Oracle.STRING'>", 60, 240, None, None, 1)
        ]
        cursor_rows = [[1, 'description 1'], [2, 'description 2']]

        mock_dest_hook = MagicMock()
        mock_src_hook = MagicMock()
        mock_src_conn = mock_src_hook.get_conn.return_value.__enter__.return_value
        mock_cursor = mock_src_conn.cursor.return_value
        mock_cursor.description.__iter__.return_value = cursor_description
        mock_cursor.fetchmany.side_effect = [cursor_rows, []]

        op = OracleToOracleTransfer(
            task_id='copy_data',
            oracle_destination_conn_id=oracle_destination_conn_id,
            destination_table=destination_table,
            oracle_source_conn_id=oracle_source_conn_id,
            source_sql=source_sql,
            source_sql_params=source_sql_params,
            rows_chunk=rows_chunk)

        op._execute(mock_src_hook, mock_dest_hook, None)

        assert mock_src_hook.get_conn.called
        assert mock_src_conn.cursor.called
        mock_cursor.execute.assert_called_with(source_sql, source_sql_params)
        mock_cursor.fetchmany.assert_called_with(rows_chunk)
        mock_dest_hook.bulk_insert_rows.assert_called_once_with(
            destination_table,
            cursor_rows,
            commit_every=rows_chunk,
            target_fields=['id', 'description'])


if __name__ == '__main__':
    unittest.main()
