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

from unittest import TestCase, mock

from moto import mock_ssm

from airflow.providers.amazon.aws.secrets.ssm import AwsSsmSecretsBackend
from airflow.secrets import get_connections


class TestSsmSecrets(TestCase):
    @mock.patch("airflow.providers.amazon.aws.secrets.ssm.AwsSsmSecretsBackend.get_conn_uri")
    def test_aws_ssm_get_connections(self, mock_get_uri):
        mock_get_uri.return_value = "scheme://user:pass@host:100"
        conn_list = AwsSsmSecretsBackend().get_connections("fake_conn")
        conn = conn_list[0]
        assert conn.host == 'host'

    @mock.patch.dict('os.environ', {
        'AIRFLOW_CONN_TEST_MYSQL': 'mysql://airflow:airflow@host:5432/airflow',
    })
    @mock_ssm
    def test_get_conn_uri_non_existent_key(self):
        """
        Test that if the key with connection ID is not present in SSM,
        AwsSsmSecretsBackend.get_connections should return None and fallback to the
        environment variable if it is set
        """
        conn_id = "test_mysql"
        test_client = AwsSsmSecretsBackend()

        self.assertIsNone(test_client.get_conn_uri(conn_id=conn_id))
        self.assertEqual([], test_client.get_connections(conn_id=conn_id))

        # Fallback to connection defined in Environment Variable
        self.assertEqual(
            "mysql://airflow:airflow@host:5432/airflow",
            get_connections(conn_id="test_mysql")[0].get_uri())
