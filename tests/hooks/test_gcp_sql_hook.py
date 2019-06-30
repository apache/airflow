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

from tests.compat import mock
import json
import unittest

from airflow.contrib.hooks.gcp_sql_hook import CloudSqlDatabaseHook
from airflow.models.connection import Connection


class TestCloudSqlDatabaseHook(unittest.TestCase):

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlDatabaseHook.get_connection')
    def setUp(self, m):
        super(TestCloudSqlDatabaseHook, self).setUp()

        self.sql_connection = Connection(
            conn_id='my_gcp_sql_connection',
            conn_type='gcpcloudsql',
            login='login',
            password='password',
            host='host',
            schema='schema',
            extra='{"database_type":"postgres", "location":"my_location", '
                  '"instance":"my_instance", "use_proxy": true, '
                  '"project_id":"my_project"}'
        )
        self.connection = Connection(
            conn_id='my_gcp_connection',
            conn_type='google_cloud_platform',
        )
        scopes = [
            "https://www.googleapis.com/auth/pubsub",
            "https://www.googleapis.com/auth/datastore",
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/devstorage.read_write",
            "https://www.googleapis.com/auth/logging.write",
            "https://www.googleapis.com/auth/cloud-platform",
        ]
        conn_extra = {
            "extra__google_cloud_platform__scope": ",".join(scopes),
            "extra__google_cloud_platform__project": "your-gcp-project",
            "extra__google_cloud_platform__key_path":
                '/var/local/google_cloud_default.json'
        }
        conn_extra_json = json.dumps(conn_extra)
        self.connection.set_extra(conn_extra_json)

        m.side_effect = [self.sql_connection, self.connection]
        self.db_hook = CloudSqlDatabaseHook(
            gcp_cloudsql_conn_id='my_gcp_sql_connection',
            gcp_conn_id='my_gcp_connection'
        )

    def test_get_sqlproxy_runner(self):
        self.db_hook._generate_connection_uri()
        sqlproxy_runner = self.db_hook.get_sqlproxy_runner()
        self.assertEqual(sqlproxy_runner.gcp_conn_id, self.connection.conn_id)
        project = self.sql_connection.extra_dejson['project_id']
        location = self.sql_connection.extra_dejson['location']
        instance = self.sql_connection.extra_dejson['instance']
        instance_spec = "{project}:{location}:{instance}".format(
            project=project, location=location, instance=instance
        )
        self.assertEqual(sqlproxy_runner.instance_specification, instance_spec)
