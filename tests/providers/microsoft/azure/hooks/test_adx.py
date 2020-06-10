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

import json
import unittest
from unittest import mock

from azure.kusto.data.request import ClientRequestProperties, KustoClient, KustoConnectionStringBuilder

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.microsoft.azure.hooks.adx import AzureDataExplorerHook
from airflow.utils import db
from airflow.utils.session import create_session

ADX_TEST_CONN_ID = 'adx_test_connection_id'


class TestAzureDataExplorerHook(unittest.TestCase):
    def tearDown(self):
        super().tearDown()
        with create_session() as session:
            session.query(Connection).filter(
                Connection.conn_id == ADX_TEST_CONN_ID).delete()

    def test_conn_missing_method(self):
        db.merge_conn(
            Connection(conn_id=ADX_TEST_CONN_ID,
                       conn_type='azure_data_explorer',
                       login='client_id',
                       password='client secret',
                       host='https://help.kusto.windows.net',
                       extra=json.dumps({})))
        with self.assertRaises(AirflowException) as e:
            AzureDataExplorerHook(azure_data_explorer_conn_id=ADX_TEST_CONN_ID)
            self.assertIn('missing required parameter: `auth_method`',
                          str(e.exception))

    def test_conn_unknown_method(self):
        db.merge_conn(
            Connection(conn_id=ADX_TEST_CONN_ID,
                       conn_type='azure_data_explorer',
                       login='client_id',
                       password='client secret',
                       host='https://help.kusto.windows.net',
                       extra=json.dumps({'auth_method': 'AAD_OTHER'})))
        with self.assertRaises(AirflowException) as e:
            AzureDataExplorerHook(azure_data_explorer_conn_id=ADX_TEST_CONN_ID)
        self.assertIn('Unknown authentication method: AAD_OTHER',
                      str(e.exception))

    def test_conn_missing_cluster(self):
        db.merge_conn(
            Connection(conn_id=ADX_TEST_CONN_ID,
                       conn_type='azure_data_explorer',
                       login='client_id',
                       password='client secret',
                       extra=json.dumps({})))
        with self.assertRaises(AirflowException) as e:
            AzureDataExplorerHook(azure_data_explorer_conn_id=ADX_TEST_CONN_ID)
        self.assertIn('Host connection option is required', str(e.exception))

    @mock.patch.object(KustoClient, '__init__')
    def test_conn_method_aad_creds(self, mock_init):
        mock_init.return_value = None
        db.merge_conn(
            Connection(conn_id=ADX_TEST_CONN_ID,
                       conn_type='azure_data_explorer',
                       login='client_id',
                       password='client secret',
                       host='https://help.kusto.windows.net',
                       extra=json.dumps({
                           'tenant': 'tenant',
                           'auth_method': 'AAD_CREDS'
                       })))
        AzureDataExplorerHook(azure_data_explorer_conn_id=ADX_TEST_CONN_ID)
        assert mock_init.called_with(
            KustoConnectionStringBuilder.with_aad_user_password_authentication(
                'https://help.kusto.windows.net', 'client_id', 'client secret',
                'tenant'))

    @mock.patch.object(KustoClient, '__init__')
    def test_conn_method_aad_app(self, mock_init):
        mock_init.return_value = None
        db.merge_conn(
            Connection(conn_id=ADX_TEST_CONN_ID,
                       conn_type='azure_data_explorer',
                       login='app_id',
                       password='app key',
                       host='https://help.kusto.windows.net',
                       extra=json.dumps({
                           'tenant': 'tenant',
                           'auth_method': 'AAD_APP'
                       })))
        AzureDataExplorerHook(azure_data_explorer_conn_id=ADX_TEST_CONN_ID)
        assert mock_init.called_with(
            KustoConnectionStringBuilder.
            with_aad_application_key_authentication(
                'https://help.kusto.windows.net', 'app_id', 'app key',
                'tenant'))

    @mock.patch.object(KustoClient, '__init__')
    def test_conn_method_aad_app_cert(self, mock_init):
        mock_init.return_value = None
        db.merge_conn(
            Connection(conn_id=ADX_TEST_CONN_ID,
                       conn_type='azure_data_explorer',
                       login='client_id',
                       host='https://help.kusto.windows.net',
                       extra=json.dumps({
                           'tenant': 'tenant',
                           'auth_method': 'AAD_APP_CERT',
                           'certificate': 'PEM',
                           'thumbprint': 'thumbprint'
                       })))
        AzureDataExplorerHook(azure_data_explorer_conn_id=ADX_TEST_CONN_ID)
        assert mock_init.called_with(
            KustoConnectionStringBuilder.
            with_aad_application_certificate_authentication(
                'https://help.kusto.windows.net', 'client_id', 'PEM',
                'thumbprint', 'tenant'))

    @mock.patch.object(KustoClient, '__init__')
    def test_conn_method_aad_device(self, mock_init):
        mock_init.return_value = None
        db.merge_conn(
            Connection(conn_id=ADX_TEST_CONN_ID,
                       conn_type='azure_data_explorer',
                       host='https://help.kusto.windows.net',
                       extra=json.dumps({'auth_method': 'AAD_DEVICE'})))
        AzureDataExplorerHook(azure_data_explorer_conn_id=ADX_TEST_CONN_ID)
        assert mock_init.called_with(
            KustoConnectionStringBuilder.with_aad_device_authentication(
                'https://help.kusto.windows.net'))

    @mock.patch.object(KustoClient, 'execute')
    def test_run_query(self, mock_execute):
        mock_execute.return_value = None
        db.merge_conn(
            Connection(conn_id=ADX_TEST_CONN_ID,
                       conn_type='azure_data_explorer',
                       host='https://help.kusto.windows.net',
                       extra=json.dumps({'auth_method': 'AAD_DEVICE'})))
        hook = AzureDataExplorerHook(
            azure_data_explorer_conn_id=ADX_TEST_CONN_ID)
        hook.run_query('Database',
                       'Logs | schema',
                       options={'option1': 'option_value'})
        properties = ClientRequestProperties()
        properties.set_option('option1', 'option_value')
        assert mock_execute.called_with('Database',
                                        'Logs | schema',
                                        properties=properties)
