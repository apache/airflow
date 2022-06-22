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
import os

import pytest

from airflow.models import Connection
from airflow.utils import db
from airflow.utils.session import create_session
from tests.test_utils import AIRFLOW_MAIN_FOLDER
from tests.test_utils.amazon_system_helpers import AWS_DAG_FOLDER, AmazonSystemTest
from tests.test_utils.terraform import Terraform


@pytest.mark.backend("mysql", "postgres")
class TestS3ToRedshiftExampleDags(AmazonSystemTest, Terraform):
    TERRAFORM_DIR = os.path.join(
        AIRFLOW_MAIN_FOLDER, "tests", "providers", "amazon", "aws", "infrastructure", "example_s3_to_redshift"
    )

    def setUp(self) -> None:
        super().setUp()
        host, port = self.get_tf_output("redshift_endpoint").split(':')
        schema = self.get_tf_output("redshift_database_name")
        login = self.get_tf_output("redshift_master_username")
        password = self.get_tf_output("redshift_master_password")
        db.merge_conn(Connection("redshift_default", "postgres", host, login, password, schema, port))

    def test_run_example_dag_s3_to_redshift(self):
        self.run_dag('example_s3_to_redshift', AWS_DAG_FOLDER)

    def tearDown(self) -> None:
        super().tearDown()
        with create_session() as session:
            session.query(Connection).filter(Connection.conn_id == "redshift_default").delete()
