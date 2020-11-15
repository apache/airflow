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
import json
import os

import pytest

from airflow.models import Connection
from airflow.utils import db
from tests.test_utils import AIRFLOW_MAIN_FOLDER
from tests.test_utils.system_tests_class import SystemTest

CREDENTIALS_DIR = os.environ.get('CREDENTIALS_DIR', '/files/airflow-breeze-config/keys')
SNOWFLAKE_KEY = 'snowflake.json'
SNOWFLAKE_CREDENTIALS_PATH = os.path.join(CREDENTIALS_DIR, SNOWFLAKE_KEY)
SNOWFLAKE_DAG_FOLDER = os.path.join(AIRFLOW_MAIN_FOLDER, 'airflow', 'providers', 'snowflake', 'example_dags')


@pytest.mark.credential_file(SNOWFLAKE_KEY)
@pytest.mark.system('snowflake')
class SnowflakeExampleDagsSystemTest(SystemTest):
    def setUp(self):
        super().setUp()

        if os.environ.get('RUN_AIRFLOW_1_10') == 'true':
            with open(SNOWFLAKE_CREDENTIALS_PATH) as f:
                # Example:
                # {
                #     "account": "foo",
                #     "region": "us-west-2",
                #     "user": "airflow",
                #     "password": "secret",
                #     "warehouse": "shared",
                #     "database": "test",
                #     "schema": "public",
                #     "role": "airflow"
                # }
                credentials = json.load(f)

            extra = {
                'account': credentials['account'],
                'region': credentials['region'],
                'role': credentials['role'],
                'warehouse': credentials['warehouse'],
                'database': credentials['database'],
            }
            conn = Connection(
                conn_id='snowflake_conn_id',
                login=credentials['user'],
                password=credentials['password'],
                schema=credentials['schema'],
                conn_type='snowflake',
                extra=json.dumps(extra),
            )
            db.merge_conn(conn)

    def test_dag_example(self):
        self.run_dag('example_snowflake', SNOWFLAKE_DAG_FOLDER)
