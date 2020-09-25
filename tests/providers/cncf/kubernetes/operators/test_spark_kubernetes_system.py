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
import subprocess

import pytest

from airflow.models import Connection
from airflow.utils import db
from tests.test_utils import AIRFLOW_MAIN_FOLDER
from tests.test_utils.system_tests_class import SystemTest

KUBERNETES_DAG_FOLDER = os.path.join(
    AIRFLOW_MAIN_FOLDER, "airflow", "providers", "cncf", "kubernetes", "example_dags"
)

SPARK_OPERATOR_VERSION = "v1beta2-1.1.1-2.4.5"

MANIFEST_BASE_URL = (
    f'https://raw.githubusercontent.com/GoogleCloudPlatform/spark-on-k8s-operator/'
    f'{SPARK_OPERATOR_VERSION}/manifest/'
)

SPARK_OPERATOR_MANIFESTS = [
    f"{MANIFEST_BASE_URL}crds/sparkoperator.k8s.io_sparkapplications.yaml",
    f"{MANIFEST_BASE_URL}crds/sparkoperator.k8s.io_scheduledsparkapplications.yaml",
    f"{MANIFEST_BASE_URL}spark-operator-rbac.yaml",
    f"{MANIFEST_BASE_URL}spark-operator.yaml",
    f"{MANIFEST_BASE_URL}spark-rbac.yaml",
]


def kubectl_apply_list(manifests):
    for manifest in manifests:
        command = ['kubectl', 'apply', '-f', manifest]
        subprocess.run(command, check=True)


def kubectl_delete_list(manifests):
    for manifest in manifests:
        command = ['kubectl', 'delete', '--ignore-not-found', '-f', manifest]
        subprocess.run(command, check=True)


@pytest.mark.system("cncf.kubernetes")
class SparkKubernetesExampleDagsSystemTest(SystemTest):
    def setUp(self):
        super().setUp()
        kubectl_apply_list(SPARK_OPERATOR_MANIFESTS)
        if os.environ.get("RUN_AIRFLOW_1_10") == "true":
            db.merge_conn(Connection(conn_id='kubernetes_default', conn_type='kubernetes'))

    def tearDown(self):
        super().tearDown()
        kubectl_delete_list(SPARK_OPERATOR_MANIFESTS)

    def test_run_example_dag_spark_pi(self):
        self.run_dag('spark_pi', KUBERNETES_DAG_FOLDER)
