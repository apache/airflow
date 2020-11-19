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

from airflow.providers_manager import ProvidersManager

ALL_PROVIDERS = [
    'apache-airflow-providers-amazon',
    'apache-airflow-providers-apache-cassandra',
    'apache-airflow-providers-apache-druid',
    'apache-airflow-providers-apache-hdfs',
    'apache-airflow-providers-apache-hive',
    'apache-airflow-providers-apache-kylin',
    'apache-airflow-providers-apache-livy',
    'apache-airflow-providers-apache-pig',
    'apache-airflow-providers-apache-pinot',
    'apache-airflow-providers-apache-spark',
    'apache-airflow-providers-apache-sqoop',
    'apache-airflow-providers-celery',
    'apache-airflow-providers-cloudant',
    'apache-airflow-providers-cncf-kubernetes',
    'apache-airflow-providers-databricks',
    'apache-airflow-providers-datadog',
    'apache-airflow-providers-dingding',
    'apache-airflow-providers-discord',
    'apache-airflow-providers-docker',
    'apache-airflow-providers-elasticsearch',
    'apache-airflow-providers-exasol',
    'apache-airflow-providers-facebook',
    'apache-airflow-providers-ftp',
    'apache-airflow-providers-google',
    'apache-airflow-providers-grpc',
    'apache-airflow-providers-hashicorp',
    'apache-airflow-providers-http',
    'apache-airflow-providers-imap',
    'apache-airflow-providers-jdbc',
    'apache-airflow-providers-jenkins',
    'apache-airflow-providers-jira',
    'apache-airflow-providers-microsoft-azure',
    'apache-airflow-providers-microsoft-mssql',
    'apache-airflow-providers-microsoft-winrm',
    'apache-airflow-providers-mongo',
    'apache-airflow-providers-mysql',
    'apache-airflow-providers-odbc',
    'apache-airflow-providers-openfaas',
    'apache-airflow-providers-opsgenie',
    'apache-airflow-providers-oracle',
    'apache-airflow-providers-pagerduty',
    'apache-airflow-providers-papermill',
    'apache-airflow-providers-plexus',
    'apache-airflow-providers-postgres',
    'apache-airflow-providers-presto',
    'apache-airflow-providers-qubole',
    'apache-airflow-providers-redis',
    'apache-airflow-providers-salesforce',
    'apache-airflow-providers-samba',
    'apache-airflow-providers-segment',
    'apache-airflow-providers-sendgrid',
    'apache-airflow-providers-sftp',
    'apache-airflow-providers-singularity',
    'apache-airflow-providers-slack',
    'apache-airflow-providers-snowflake',
    'apache-airflow-providers-sqlite',
    'apache-airflow-providers-ssh',
    'apache-airflow-providers-vertica',
    'apache-airflow-providers-yandex',
    'apache-airflow-providers-zendesk',
]


class TestProviderManager(unittest.TestCase):
    def test_providers_are_loaded(self):
        provider_manager = ProvidersManager()
        provider_list = list(provider_manager.providers.keys())
        provider_list.sort()
        self.assertEqual(ALL_PROVIDERS, provider_list)
