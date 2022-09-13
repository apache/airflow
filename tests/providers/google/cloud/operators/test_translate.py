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
from __future__ import annotations

import unittest
from unittest import mock

from airflow.providers.google.cloud.operators.translate import CloudTranslateTextOperator

GCP_CONN_ID = 'google_cloud_default'
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]


class TestCloudTranslate(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.translate.CloudTranslateHook')
    def test_minimal_green_path(self, mock_hook):
        mock_hook.return_value.translate.return_value = [
            {
                'translatedText': 'Yellowing self Gęśle',
                'detectedSourceLanguage': 'pl',
                'model': 'base',
                'input': 'zażółć gęślą jaźń',
            }
        ]
        op = CloudTranslateTextOperator(
            values=['zażółć gęślą jaźń'],
            target_language='en',
            format_='text',
            source_language=None,
            model='base',
            gcp_conn_id=GCP_CONN_ID,
            task_id='id',
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        context = mock.MagicMock()
        return_value = op.execute(context=context)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.translate.assert_called_once_with(
            values=['zażółć gęślą jaźń'],
            target_language='en',
            format_='text',
            source_language=None,
            model='base',
        )
        assert [
            {
                'translatedText': 'Yellowing self Gęśle',
                'detectedSourceLanguage': 'pl',
                'model': 'base',
                'input': 'zażółć gęślą jaźń',
            }
        ] == return_value
