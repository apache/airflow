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
from unittest.mock import Mock, patch

from airflow.providers.salesforce.operators.salesforce_bulk import (
    SalesforceBulkInsertOperator,
    SalesforceBulkUpdateOperator,
    SalesforceBulkUpsertOperator,
    SalesforceBulkDeleteOperator
)


class TestSalesforceBulkInsertOperator(unittest.TestCase):
    """
    Test class for SalesforceBulkInsertOperator
    """

    @patch('airflow.providers.salesforce.operators.salesforce_apex_rest.SalesforceHook.get_conn')
    def test_execute_salesforce_bulk_insert(self, mock_get_conn):
        """
        Test execute bulk insert
        """

        object_name = 'Account'
        payload = [
            {'Name': 'account1'},
            {'Name': 'account2'},
        ]
        batch_size = 10000
        use_serial = True

        mock_get_conn.return_value.bulk.__getattr__(object_name).insert = Mock()
        operator = SalesforceBulkInsertOperator(
            task_id='bulk_insert',
            object_name=object_name,
            payload=payload,
            batch_size=batch_size,
            use_serial=use_serial,
        )

        operator.execute(context={})

        mock_get_conn.return_value.bulk.__getattr__(object_name).insert.assert_called_once_with(
            data=payload,
            batch_size=batch_size,
            use_serial=use_serial,
        )


class TestSalesforceBulkUpdateOperator(unittest.TestCase):
    """
    Test class for SalesforceBulkUpdateOperator
    """

    @patch('airflow.providers.salesforce.operators.salesforce_apex_rest.SalesforceHook.get_conn')
    def test_execute_salesforce_bulk_update(self, mock_get_conn):
        """
        Test execute bulk update
        """

        object_name = 'Account'
        payload = [
            {'Id': '000000000000000AAA', 'Name': 'account1'},
            {'Id': '000000000000000BBB', 'Name': 'account2'},
        ]
        batch_size = 10000
        use_serial = True

        mock_get_conn.return_value.bulk.__getattr__(object_name).update = Mock()
        operator = SalesforceBulkUpdateOperator(
            task_id='bulk_update',
            object_name=object_name,
            payload=payload,
            batch_size=batch_size,
            use_serial=use_serial,
        )

        operator.execute(context={})

        mock_get_conn.return_value.bulk.__getattr__(object_name).update.assert_called_once_with(
            data=payload,
            batch_size=batch_size,
            use_serial=use_serial,
        )


class TestSalesforceBulkUpsertOperator(unittest.TestCase):
    """
    Test class for SalesforceBulkUpsertOperator
    """

    @patch('airflow.providers.salesforce.operators.salesforce_apex_rest.SalesforceHook.get_conn')
    def test_execute_salesforce_bulk_upsert(self, mock_get_conn):
        """
        Test execute bulk upsert
        """

        object_name = 'Account'
        payload = [
            {'Id': '000000000000000AAA', 'Name': 'account1'},
            {'Name': 'account2'},
        ]
        external_id_field = 'Id'
        batch_size = 10000
        use_serial = True

        mock_get_conn.return_value.bulk.__getattr__(object_name).upsert = Mock()
        operator = SalesforceBulkUpsertOperator(
            task_id='bulk_upsert',
            object_name=object_name,
            payload=payload,
            external_id_field=external_id_field,
            batch_size=batch_size,
            use_serial=use_serial,
        )

        operator.execute(context={})

        mock_get_conn.return_value.bulk.__getattr__(object_name).upsert.assert_called_once_with(
            data=payload,
            external_id_field=external_id_field,
            batch_size=batch_size,
            use_serial=use_serial,
        )


class TestSalesforceBulkDeleteOperator(unittest.TestCase):
    """
    Test class for SalesforceBulkDeleteOperator
    """

    @patch('airflow.providers.salesforce.operators.salesforce_apex_rest.SalesforceHook.get_conn')
    def test_execute_salesforce_bulk_insert(self, mock_get_conn):
        """
        Test execute bulk delete
        """

        object_name = 'Account'
        payload = [
            {'Id': '000000000000000AAA'},
            {'Id': '000000000000000BBB'},
        ]
        batch_size = 10000
        use_serial = True

        mock_get_conn.return_value.bulk.__getattr__(object_name).delete = Mock()
        operator = SalesforceBulkDeleteOperator(
            task_id='bulk_delete',
            object_name=object_name,
            payload=payload,
            batch_size=batch_size,
            use_serial=use_serial,
        )

        operator.execute(context={})

        mock_get_conn.return_value.bulk.__getattr__(object_name).delete.assert_called_once_with(
            data=payload,
            batch_size=batch_size,
            use_serial=use_serial,
        )
