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
import json
import os
import subprocess
import unittest

from airflow import models, settings, configuration, AirflowException
from airflow.utils.timezone import datetime

DEFAULT_DATE = datetime(2015, 1, 1)

KEYPATH_EXTRA = 'extra__google_cloud_platform__key_path'
KEYFILE_DICT_EXTRA = 'extra__google_cloud_platform__keyfile_dict'
SCOPE_EXTRA = 'extra__google_cloud_platform__scope'
PROJECT_EXTRA = 'extra__google_cloud_platform__project'

AIRFLOW_MAIN_FOLDER = os.path.realpath(os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    os.pardir, os.pardir, os.pardir))

CONTRIB_OPERATORS_EXAMPLES_DAG_FOLDER = os.path.join(
    AIRFLOW_MAIN_FOLDER, "airflow", "contrib", "example_dags")

OPERATORS_EXAMPLES_DAG_FOLDER = os.path.join(
    AIRFLOW_MAIN_FOLDER, "airflow", "example_dags")

TESTS_DAG_FOLDER = os.path.join(
    AIRFLOW_MAIN_FOLDER, "tests", "dags")

GCP_FOLDER_ENVIRONMENT_VARIABLE = "GCP_SERVICE_ACCOUNT_KEY_FOLDER"

GCP_COMPUTE_KEY = 'gcp_compute.json'
GCP_FUNCTION_KEY = 'gcp_function.json'
GCP_CLOUDSQL_KEY = 'gcp_cloudsql.json'
GCP_BIGTABLE_KEY = 'gcp_bigtable.json'
GCP_SPANNER_KEY = 'gcp_spanner.json'
GCP_GCS_KEY = 'gcp_gcs.json'

SKIP_TEST_WARNING = """
The test is only run when there is GCP connection available! "
Set GCP_SERVICE_ACCOUNT_KEY_FOLDER environment variable if "
you want to run them".
"""


class BaseGcpIntegrationTestCase(unittest.TestCase):
    def __init__(self,
                 method_name,
                 dag_id,
                 gcp_key,
                 dag_name=None,
                 example_dags_folder=CONTRIB_OPERATORS_EXAMPLES_DAG_FOLDER,
                 project_extra=None):
        super(BaseGcpIntegrationTestCase, self).__init__(method_name)
        self.dag_id = dag_id
        self.dag_name = self.dag_id + '.py' if not dag_name else dag_name
        self.gcp_key = gcp_key
        self.example_dags_folder = example_dags_folder
        self.project_extra = project_extra
        self.full_key_path = None

    def _gcp_authenticate(self):
        key_dir_path = os.environ['GCP_SERVICE_ACCOUNT_KEY_FOLDER']
        self.full_key_path = os.path.join(key_dir_path, self.gcp_key)

        if not os.path.isfile(self.full_key_path):
            raise Exception("The key {} could not be found. Please copy it to the "
                            "{} folder.".format(self.gcp_key, key_dir_path))
        print("Setting the GCP key to {}".format(self.full_key_path))
        # Checking if we can authenticate using service account credentials provided
        retcode = subprocess.call(['gcloud', 'auth', 'activate-service-account',
                                   '--key-file={}'.format(self.full_key_path)])
        if retcode != 0:
            raise AirflowException("The gcloud auth method was not successful!")
        self.update_connection_with_key_path()
        # Now we revoke all authentication here because we want to make sure
        # that all works fine with the credentials retrieved from the gcp_connection
        subprocess.call(['gcloud', 'auth', 'revoke'])

    def update_connection_with_key_path(self):
        session = settings.Session()
        try:
            conn = session.query(models.Connection).filter(
                models.Connection.conn_id == 'google_cloud_default')[0]
            extras = conn.extra_dejson
            extras[KEYPATH_EXTRA] = self.full_key_path
            if extras.get(KEYFILE_DICT_EXTRA):
                del extras[KEYFILE_DICT_EXTRA]
            extras[SCOPE_EXTRA] = 'https://www.googleapis.com/auth/cloud-platform'
            extras[PROJECT_EXTRA] = self.project_extra
            conn.extra = json.dumps(extras)
            session.commit()
        except BaseException as e:
            print('Airflow DB Session error:' + str(e.message))
            session.rollback()
            raise
        finally:
            session.close()

    def update_connection_with_dictionary(self):
        session = settings.Session()
        try:
            conn = session.query(models.Connection).filter(
                models.Connection.conn_id == 'google_cloud_default')[0]
            extras = conn.extra_dejson
            with open(self.full_key_path, "r") as f:
                content = json.load(f)
            extras[KEYFILE_DICT_EXTRA] = json.dumps(content)
            if extras.get(KEYPATH_EXTRA):
                del extras[KEYPATH_EXTRA]
            extras[SCOPE_EXTRA] = 'https://www.googleapis.com/auth/cloud-platform'
            extras[PROJECT_EXTRA] = self.project_extra
            conn.extra = json.dumps(extras)
            session.commit()
        except BaseException as e:
            print('Airflow DB Session error:' + str(e.message))
            session.rollback()
            raise
        finally:
            session.close()

    def _symlink_dag(self):
        target_path = os.path.join(TESTS_DAG_FOLDER, self.dag_name)
        if os.path.exists(target_path):
            os.remove(target_path)
        os.symlink(
            os.path.join(self.example_dags_folder, self.dag_name),
            os.path.join(target_path))

    def _rm_symlink_dag(self):
        os.remove(os.path.join(TESTS_DAG_FOLDER, self.dag_name))

    def _run_dag(self):
        dag_bag = models.DagBag(dag_folder=TESTS_DAG_FOLDER, include_examples=False)
        self.args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        dag = dag_bag.get_dag(self.dag_id)
        dag.clear(reset_dag_runs=True)
        dag.run(ignore_first_depends_on_past=True, verbose=True)

    def setUp(self):
        configuration.conf.load_test_config()
        self._gcp_authenticate()
        self._symlink_dag()

    def tearDown(self):
        self._rm_symlink_dag()

    @staticmethod
    def skip_check(key):
        if GCP_FOLDER_ENVIRONMENT_VARIABLE not in os.environ:
            return True
        key_folder = os.environ[GCP_FOLDER_ENVIRONMENT_VARIABLE]
        if not os.path.isdir(key_folder):
            return True
        key_path = os.path.join(key_folder, key)
        if not os.path.isfile(key_path):
            return True
        return False
