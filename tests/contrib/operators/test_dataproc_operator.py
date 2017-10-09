# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import datetime
import re
import unittest

from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator
from airflow.contrib.operators.dataproc_operator import DataprocClusterDeleteOperator
from airflow.contrib.operators.dataproc_operator import DataProcHadoopOperator
from airflow.contrib.operators.dataproc_operator import DataProcHiveOperator
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator
from airflow.contrib.operators.dataproc_operator import DataProcSparkOperator
from airflow.version import version

from copy import deepcopy

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

from mock import Mock
from mock import patch

TASK_ID = 'test-dataproc-operator'
CLUSTER_NAME = 'test-cluster-name'
PROJECT_ID = 'test-project-id'
NUM_WORKERS = 123
ZONE = 'us-central1-a'
STORAGE_BUCKET = 'gs://airflow-test-bucket/'
IMAGE_VERSION = '1.1'
MASTER_MACHINE_TYPE = 'n1-standard-2'
MASTER_DISK_SIZE = 100
WORKER_MACHINE_TYPE = 'n1-standard-2'
WORKER_DISK_SIZE = 100
NUM_PREEMPTIBLE_WORKERS = 2
LABEL1 = {}
LABEL2 = {'application':'test', 'year': 2017}
SERVICE_ACCOUNT_SCOPES = [
    'https://www.googleapis.com/auth/bigquery',
    'https://www.googleapis.com/auth/bigtable.data'
]
DEFAULT_DATE = datetime.datetime(2017, 6, 6)
REGION = 'test-region'
MAIN_URI = 'test-uri'

class DataprocClusterCreateOperatorTest(unittest.TestCase):
    # Unit test for the DataprocClusterCreateOperator
    def setUp(self):
        # instantiate two different test cases with different labels.
        self.labels = [LABEL1, LABEL2]
        self.dataproc_operators = []
        self.mock_conn = Mock()
        for labels in self.labels:
             self.dataproc_operators.append(
                DataprocClusterCreateOperator(
                    task_id=TASK_ID,
                    cluster_name=CLUSTER_NAME,
                    project_id=PROJECT_ID,
                    num_workers=NUM_WORKERS,
                    zone=ZONE,
                    storage_bucket=STORAGE_BUCKET,
                    image_version=IMAGE_VERSION,
                    master_machine_type=MASTER_MACHINE_TYPE,
                    master_disk_size=MASTER_DISK_SIZE,
                    worker_machine_type=WORKER_MACHINE_TYPE,
                    worker_disk_size=WORKER_DISK_SIZE,
                    num_preemptible_workers=NUM_PREEMPTIBLE_WORKERS,
                    labels = deepcopy(labels),
                    service_account_scopes = SERVICE_ACCOUNT_SCOPES
                )
             )
        self.dag = DAG(
            'test_dag',
            default_args={
                'owner': 'airflow',
                'start_date': DEFAULT_DATE,
                'end_date': DEFAULT_DATE,
            },
            schedule_interval='@daily')

    def test_init(self):
        """Test DataProcClusterOperator instance is properly initialized."""
        for suffix, dataproc_operator in enumerate(self.dataproc_operators):
            self.assertEqual(dataproc_operator.cluster_name, CLUSTER_NAME)
            self.assertEqual(dataproc_operator.project_id, PROJECT_ID)
            self.assertEqual(dataproc_operator.num_workers, NUM_WORKERS)
            self.assertEqual(dataproc_operator.zone, ZONE)
            self.assertEqual(dataproc_operator.storage_bucket, STORAGE_BUCKET)
            self.assertEqual(dataproc_operator.image_version, IMAGE_VERSION)
            self.assertEqual(dataproc_operator.master_machine_type, MASTER_MACHINE_TYPE)
            self.assertEqual(dataproc_operator.master_disk_size, MASTER_DISK_SIZE)
            self.assertEqual(dataproc_operator.worker_machine_type, WORKER_MACHINE_TYPE)
            self.assertEqual(dataproc_operator.worker_disk_size, WORKER_DISK_SIZE)
            self.assertEqual(dataproc_operator.num_preemptible_workers, NUM_PREEMPTIBLE_WORKERS)
            self.assertEqual(dataproc_operator.labels, self.labels[suffix])
            self.assertEqual(dataproc_operator.service_account_scopes, SERVICE_ACCOUNT_SCOPES)

    def test_build_cluster_data(self):
        for suffix, dataproc_operator in enumerate(self.dataproc_operators):
            cluster_data = dataproc_operator._build_cluster_data()
            self.assertEqual(cluster_data['clusterName'], CLUSTER_NAME)
            self.assertEqual(cluster_data['projectId'], PROJECT_ID)
            self.assertEqual(cluster_data['config']['softwareConfig'], {'imageVersion': IMAGE_VERSION})
            self.assertEqual(cluster_data['config']['configBucket'], STORAGE_BUCKET)
            self.assertEqual(cluster_data['config']['workerConfig']['numInstances'], NUM_WORKERS)
            self.assertEqual(cluster_data['config']['secondaryWorkerConfig']['numInstances'],
                             NUM_PREEMPTIBLE_WORKERS)
            self.assertEqual(cluster_data['config']['gceClusterConfig']['serviceAccountScopes'],
                SERVICE_ACCOUNT_SCOPES)
            # test whether the default airflow-version label has been properly
            # set to the dataproc operator.
            merged_labels = {}
            merged_labels.update(self.labels[suffix])
            merged_labels.update({'airflow-version': 'v' + version.replace('.', '-').replace('+','-')})
            self.assertTrue(re.match(r'[a-z]([-a-z0-9]*[a-z0-9])?',
                                     cluster_data['labels']['airflow-version']))
            self.assertEqual(cluster_data['labels'], merged_labels)

    def test_cluster_name_log_no_sub(self):
        with patch('airflow.contrib.operators.dataproc_operator.DataProcHook') as mock_hook:
            mock_hook.return_value.get_conn = self.mock_conn
            dataproc_task = DataprocClusterCreateOperator(
                task_id=TASK_ID,
                cluster_name=CLUSTER_NAME,
                project_id=PROJECT_ID,
                num_workers=NUM_WORKERS,
                zone=ZONE,
                dag=self.dag
            )
            with patch.object(dataproc_task.log, 'info') as mock_info:
                with self.assertRaises(TypeError) as _:
                    dataproc_task.execute(None)
                mock_info.assert_called_with('Creating cluster: %s', CLUSTER_NAME)

    def test_cluster_name_log_sub(self):
        with patch('airflow.contrib.operators.dataproc_operator.DataProcHook') as mock_hook:
            mock_hook.return_value.get_conn = self.mock_conn
            dataproc_task = DataprocClusterCreateOperator(
                task_id=TASK_ID,
                cluster_name='smoke-cluster-{{ ts_nodash }}',
                project_id=PROJECT_ID,
                num_workers=NUM_WORKERS,
                zone=ZONE,
                dag=self.dag
            )
            with patch.object(dataproc_task.log, 'info') as mock_info:
                context = { 'ts_nodash' : 'testnodash'}

                rendered = dataproc_task.render_template('cluster_name', getattr(dataproc_task,'cluster_name'), context)
                setattr(dataproc_task, 'cluster_name', rendered)
                with self.assertRaises(TypeError) as _:
                    dataproc_task.execute(None)
                mock_info.assert_called_with('Creating cluster: %s', u'smoke-cluster-testnodash')

class DataprocClusterDeleteOperatorTest(unittest.TestCase):
    # Unit test for the DataprocClusterDeleteOperator
    def setUp(self):
        self.mock_execute = Mock()
        self.mock_execute.execute = Mock(return_value={'done' : True})
        self.mock_get = Mock()
        self.mock_get.get = Mock(return_value=self.mock_execute)
        self.mock_operations = Mock()
        self.mock_operations.get = Mock(return_value=self.mock_get)
        self.mock_regions = Mock()
        self.mock_regions.operations = Mock(return_value=self.mock_operations)
        self.mock_projects=Mock()
        self.mock_projects.regions = Mock(return_value=self.mock_regions)
        self.mock_conn = Mock()
        self.mock_conn.projects = Mock(return_value=self.mock_projects)
        self.dag = DAG(
            'test_dag',
            default_args={
                'owner': 'airflow',
                'start_date': DEFAULT_DATE,
                'end_date': DEFAULT_DATE,
            },
            schedule_interval='@daily')

    def test_cluster_name_log_no_sub(self):
        with patch('airflow.contrib.hooks.gcp_dataproc_hook.DataProcHook') as mock_hook:
            mock_hook.return_value.get_conn = self.mock_conn
            dataproc_task = DataprocClusterDeleteOperator(
                task_id=TASK_ID,
                cluster_name=CLUSTER_NAME,
                project_id=PROJECT_ID,
                dag=self.dag
            )
            with patch.object(dataproc_task.log, 'info') as mock_info:
                with self.assertRaises(TypeError) as _:
                    dataproc_task.execute(None)
                mock_info.assert_called_with('Deleting cluster: %s', CLUSTER_NAME)

    def test_cluster_name_log_sub(self):
        with patch('airflow.contrib.operators.dataproc_operator.DataProcHook') as mock_hook:
            mock_hook.return_value.get_conn = self.mock_conn
            dataproc_task = DataprocClusterDeleteOperator(
                task_id=TASK_ID,
                cluster_name='smoke-cluster-{{ ts_nodash }}',
                project_id=PROJECT_ID,
                dag=self.dag
            )

            with patch.object(dataproc_task.log, 'info') as mock_info:
                context = { 'ts_nodash' : 'testnodash'}

                rendered = dataproc_task.render_template('cluster_name', getattr(dataproc_task,'cluster_name'), context)
                setattr(dataproc_task, 'cluster_name', rendered)
                with self.assertRaises(TypeError) as _:
                    dataproc_task.execute(None)
                mock_info.assert_called_with('Deleting cluster: %s', u'smoke-cluster-testnodash')

class DataProcHadoopOperatorTest(unittest.TestCase):
    # Unit test for the DataProcHadoopOperator
    def test_hook_correct_region(self):
       with patch('airflow.contrib.operators.dataproc_operator.DataProcHook') as mock_hook:
            dataproc_task = DataProcHadoopOperator(
                task_id=TASK_ID,
                region=REGION
            )

            dataproc_task.execute(None)
            mock_hook.return_value.submit.assert_called_once_with(mock.ANY, mock.ANY, REGION)

class DataProcHiveOperatorTest(unittest.TestCase):
    # Unit test for the DataProcHiveOperator
    def test_hook_correct_region(self):
       with patch('airflow.contrib.operators.dataproc_operator.DataProcHook') as mock_hook:
            dataproc_task = DataProcHiveOperator(
                task_id=TASK_ID,
                region=REGION
            )

            dataproc_task.execute(None)
            mock_hook.return_value.submit.assert_called_once_with(mock.ANY, mock.ANY, REGION)

class DataProcPySparkOperatorTest(unittest.TestCase):
    # Unit test for the DataProcPySparkOperator
    def test_hook_correct_region(self):
       with patch('airflow.contrib.operators.dataproc_operator.DataProcHook') as mock_hook:
            dataproc_task = DataProcPySparkOperator(
                task_id=TASK_ID,
                main=MAIN_URI,
                region=REGION
            )

            dataproc_task.execute(None)
            mock_hook.return_value.submit.assert_called_once_with(mock.ANY, mock.ANY, REGION)

class DataProcSparkOperatorTest(unittest.TestCase):
    # Unit test for the DataProcSparkOperator
    def test_hook_correct_region(self):
       with patch('airflow.contrib.operators.dataproc_operator.DataProcHook') as mock_hook:
            dataproc_task = DataProcSparkOperator(
                task_id=TASK_ID,
                region=REGION
            )

            dataproc_task.execute(None)
            mock_hook.return_value.submit.assert_called_once_with(mock.ANY, mock.ANY, REGION)
