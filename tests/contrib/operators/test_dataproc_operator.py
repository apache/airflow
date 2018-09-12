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

import datetime
import re
import unittest

from airflow import DAG, AirflowException
from airflow.contrib.operators.dataproc_operator import \
    DataprocClusterCreateOperator, \
    DataprocClusterDeleteOperator, \
    DataProcHadoopOperator, \
    DataProcHiveOperator, \
    DataProcPySparkOperator, \
    DataProcSparkOperator, \
    DataprocWorkflowTemplateInstantiateInlineOperator, \
    DataprocWorkflowTemplateInstantiateOperator, \
    DataprocClusterScaleOperator
from airflow.version import version

from copy import deepcopy

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

from mock import MagicMock, Mock
from mock import patch

TASK_ID = 'test-dataproc-operator'
CLUSTER_NAME = 'test-cluster-name'
PROJECT_ID = 'test-project-id'
NUM_WORKERS = 123
ZONE = 'us-central1-a'
NETWORK_URI = '/projects/project_id/regions/global/net'
SUBNETWORK_URI = '/projects/project_id/regions/global/subnet'
INTERNAL_IP_ONLY = True
TAGS = ['tag1', 'tag2']
STORAGE_BUCKET = 'gs://airflow-test-bucket/'
IMAGE_VERSION = '1.1'
CUSTOM_IMAGE = 'test-custom-image'
MASTER_MACHINE_TYPE = 'n1-standard-2'
MASTER_DISK_SIZE = 100
MASTER_DISK_TYPE = 'pd-standard'
WORKER_MACHINE_TYPE = 'n1-standard-2'
WORKER_DISK_SIZE = 100
WORKER_DISK_TYPE = 'pd-standard'
NUM_PREEMPTIBLE_WORKERS = 2
GET_INIT_ACTION_TIMEOUT = "600s"  # 10m
LABEL1 = {}
LABEL2 = {'application': 'test', 'year': 2017}
SERVICE_ACCOUNT_SCOPES = [
    'https://www.googleapis.com/auth/bigquery',
    'https://www.googleapis.com/auth/bigtable.data'
]
IDLE_DELETE_TTL = 321
AUTO_DELETE_TIME = datetime.datetime(2017, 6, 7)
AUTO_DELETE_TTL = 654
DEFAULT_DATE = datetime.datetime(2017, 6, 6)
REGION = 'test-region'
MAIN_URI = 'test-uri'
TEMPLATE_ID = 'template-id'

HOOK = 'airflow.contrib.operators.dataproc_operator.DataProcHook'
DATAPROC_JOB_ID = 'dataproc_job_id'
DATAPROC_JOB_TO_SUBMIT = {
    'job': {
        'reference': {
            'projectId': PROJECT_ID,
            'jobId': DATAPROC_JOB_ID,
        },
        'placement': {
            'clusterName': CLUSTER_NAME
        }
    }
}


def _assert_dataproc_job_id(mock_hook, dataproc_task):
    hook = mock_hook.return_value
    job = MagicMock()
    job.build.return_value = DATAPROC_JOB_TO_SUBMIT
    hook.create_job_template.return_value = job
    dataproc_task.execute(None)
    assert dataproc_task.dataproc_job_id == DATAPROC_JOB_ID


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
                    network_uri=NETWORK_URI,
                    subnetwork_uri=SUBNETWORK_URI,
                    internal_ip_only=INTERNAL_IP_ONLY,
                    tags=TAGS,
                    storage_bucket=STORAGE_BUCKET,
                    image_version=IMAGE_VERSION,
                    master_machine_type=MASTER_MACHINE_TYPE,
                    master_disk_type=MASTER_DISK_TYPE,
                    master_disk_size=MASTER_DISK_SIZE,
                    worker_machine_type=WORKER_MACHINE_TYPE,
                    worker_disk_type=WORKER_DISK_TYPE,
                    worker_disk_size=WORKER_DISK_SIZE,
                    num_preemptible_workers=NUM_PREEMPTIBLE_WORKERS,
                    labels=deepcopy(labels),
                    service_account_scopes=SERVICE_ACCOUNT_SCOPES,
                    idle_delete_ttl=IDLE_DELETE_TTL,
                    auto_delete_time=AUTO_DELETE_TIME,
                    auto_delete_ttl=AUTO_DELETE_TTL
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
            self.assertEqual(dataproc_operator.network_uri, NETWORK_URI)
            self.assertEqual(dataproc_operator.subnetwork_uri, SUBNETWORK_URI)
            self.assertEqual(dataproc_operator.tags, TAGS)
            self.assertEqual(dataproc_operator.storage_bucket, STORAGE_BUCKET)
            self.assertEqual(dataproc_operator.image_version, IMAGE_VERSION)
            self.assertEqual(dataproc_operator.master_machine_type, MASTER_MACHINE_TYPE)
            self.assertEqual(dataproc_operator.master_disk_size, MASTER_DISK_SIZE)
            self.assertEqual(dataproc_operator.master_disk_type, MASTER_DISK_TYPE)
            self.assertEqual(dataproc_operator.worker_machine_type, WORKER_MACHINE_TYPE)
            self.assertEqual(dataproc_operator.worker_disk_size, WORKER_DISK_SIZE)
            self.assertEqual(dataproc_operator.worker_disk_type, WORKER_DISK_TYPE)
            self.assertEqual(dataproc_operator.num_preemptible_workers,
                             NUM_PREEMPTIBLE_WORKERS)
            self.assertEqual(dataproc_operator.labels, self.labels[suffix])
            self.assertEqual(dataproc_operator.service_account_scopes,
                             SERVICE_ACCOUNT_SCOPES)
            self.assertEqual(dataproc_operator.idle_delete_ttl, IDLE_DELETE_TTL)
            self.assertEqual(dataproc_operator.auto_delete_time, AUTO_DELETE_TIME)
            self.assertEqual(dataproc_operator.auto_delete_ttl, AUTO_DELETE_TTL)

    def test_get_init_action_timeout(self):
        for suffix, dataproc_operator in enumerate(self.dataproc_operators):
            timeout = dataproc_operator._get_init_action_timeout()
            self.assertEqual(timeout, "600s")

    def test_build_cluster_data(self):
        for suffix, dataproc_operator in enumerate(self.dataproc_operators):
            cluster_data = dataproc_operator._build_cluster_data()
            self.assertEqual(cluster_data['clusterName'], CLUSTER_NAME)
            self.assertEqual(cluster_data['projectId'], PROJECT_ID)
            self.assertEqual(cluster_data['config']['softwareConfig'],
                             {'imageVersion': IMAGE_VERSION})
            self.assertEqual(cluster_data['config']['configBucket'], STORAGE_BUCKET)
            self.assertEqual(cluster_data['config']['workerConfig']['numInstances'],
                             NUM_WORKERS)
            self.assertEqual(
                cluster_data['config']['secondaryWorkerConfig']['numInstances'],
                NUM_PREEMPTIBLE_WORKERS)
            self.assertEqual(
                cluster_data['config']['gceClusterConfig']['serviceAccountScopes'],
                SERVICE_ACCOUNT_SCOPES)
            self.assertEqual(cluster_data['config']['gceClusterConfig']['internalIpOnly'],
                             INTERNAL_IP_ONLY)
            self.assertEqual(cluster_data['config']['gceClusterConfig']['subnetworkUri'],
                             SUBNETWORK_URI)
            self.assertEqual(cluster_data['config']['gceClusterConfig']['networkUri'],
                             NETWORK_URI)
            self.assertEqual(cluster_data['config']['gceClusterConfig']['tags'],
                             TAGS)
            self.assertEqual(cluster_data['config']['lifecycleConfig']['idleDeleteTtl'],
                             "321s")
            self.assertEqual(cluster_data['config']['lifecycleConfig']['autoDeleteTime'],
                             "2017-06-07T00:00:00.000000Z")
            # test whether the default airflow-version label has been properly
            # set to the dataproc operator.
            merged_labels = {}
            merged_labels.update(self.labels[suffix])
            merged_labels.update({'airflow-version': 'v' + version.replace('.', '-').replace('+','-')})
            self.assertTrue(re.match(r'[a-z]([-a-z0-9]*[a-z0-9])?',
                                     cluster_data['labels']['airflow-version']))
            self.assertEqual(cluster_data['labels'], merged_labels)

    def test_build_cluster_data_with_autoDeleteTime(self):
        dataproc_operator = DataprocClusterCreateOperator(
            task_id=TASK_ID,
            cluster_name=CLUSTER_NAME,
            project_id=PROJECT_ID,
            num_workers=NUM_WORKERS,
            zone=ZONE,
            dag=self.dag,
            auto_delete_time=AUTO_DELETE_TIME,
        )
        cluster_data = dataproc_operator._build_cluster_data()
        self.assertEqual(cluster_data['config']['lifecycleConfig']['autoDeleteTime'],
                         "2017-06-07T00:00:00.000000Z")

    def test_build_cluster_data_with_autoDeleteTtl(self):
        dataproc_operator = DataprocClusterCreateOperator(
            task_id=TASK_ID,
            cluster_name=CLUSTER_NAME,
            project_id=PROJECT_ID,
            num_workers=NUM_WORKERS,
            zone=ZONE,
            dag=self.dag,
            auto_delete_ttl=AUTO_DELETE_TTL,
        )
        cluster_data = dataproc_operator._build_cluster_data()
        self.assertEqual(cluster_data['config']['lifecycleConfig']['autoDeleteTtl'],
                         "654s")

    def test_build_cluster_data_with_autoDeleteTime_and_autoDeleteTtl(self):
        dataproc_operator = DataprocClusterCreateOperator(
            task_id=TASK_ID,
            cluster_name=CLUSTER_NAME,
            project_id=PROJECT_ID,
            num_workers=NUM_WORKERS,
            zone=ZONE,
            dag=self.dag,
            auto_delete_time=AUTO_DELETE_TIME,
            auto_delete_ttl=AUTO_DELETE_TTL,
        )
        cluster_data = dataproc_operator._build_cluster_data()
        if 'autoDeleteTtl' in cluster_data['config']['lifecycleConfig']:
            self.fail("If 'auto_delete_time' and 'auto_delete_ttl' is set, " +
                      "only `auto_delete_time` is used")
        self.assertEqual(cluster_data['config']['lifecycleConfig']['autoDeleteTime'],
                         "2017-06-07T00:00:00.000000Z")

    def test_init_with_image_version_and_custom_image_both_set(self):
        with self.assertRaises(AssertionError):
            DataprocClusterCreateOperator(
                task_id=TASK_ID,
                cluster_name=CLUSTER_NAME,
                project_id=PROJECT_ID,
                num_workers=NUM_WORKERS,
                zone=ZONE,
                dag=self.dag,
                image_version=IMAGE_VERSION,
                custom_image=CUSTOM_IMAGE
            )

    def test_init_with_custom_image(self):
        dataproc_operator = DataprocClusterCreateOperator(
            task_id=TASK_ID,
            cluster_name=CLUSTER_NAME,
            project_id=PROJECT_ID,
            num_workers=NUM_WORKERS,
            zone=ZONE,
            dag=self.dag,
            custom_image=CUSTOM_IMAGE
        )

        cluster_data = dataproc_operator._build_cluster_data()
        expected_custom_image_url = \
            'https://www.googleapis.com/compute/beta/projects/' \
            '{}/global/images/{}'.format(PROJECT_ID, CUSTOM_IMAGE)
        self.assertEqual(cluster_data['config']['masterConfig']['imageUri'],
                         expected_custom_image_url)
        self.assertEqual(cluster_data['config']['workerConfig']['imageUri'],
                         expected_custom_image_url)

    def test_cluster_name_log_no_sub(self):
        with patch('airflow.contrib.operators.dataproc_operator.DataProcHook') \
                as mock_hook:
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
        with patch('airflow.contrib.operators.dataproc_operator.DataProcHook') \
                as mock_hook:
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
                context = {'ts_nodash': 'testnodash'}

                rendered = dataproc_task.render_template(
                    'cluster_name',
                    getattr(dataproc_task, 'cluster_name'), context)
                setattr(dataproc_task, 'cluster_name', rendered)
                with self.assertRaises(TypeError):
                    dataproc_task.execute(None)
                mock_info.assert_called_with('Creating cluster: %s',
                                             u'smoke-cluster-testnodash')

    def test_build_cluster_data_internal_ip_only_without_subnetwork(self):

        def create_cluster_with_invalid_internal_ip_only_setup():

            # Given
            create_cluster = DataprocClusterCreateOperator(
                task_id=TASK_ID,
                cluster_name=CLUSTER_NAME,
                project_id=PROJECT_ID,
                num_workers=NUM_WORKERS,
                zone=ZONE,
                dag=self.dag,
                internal_ip_only=True)

            # When
            create_cluster._build_cluster_data()

        # Then
        with self.assertRaises(AirflowException) as cm:
            create_cluster_with_invalid_internal_ip_only_setup()

        self.assertEqual(str(cm.exception),
                         "Set internal_ip_only to true only when"
                         " you pass a subnetwork_uri.")


class DataprocClusterScaleOperatorTest(unittest.TestCase):
    # Unit test for the DataprocClusterScaleOperator
    def setUp(self):
        self.mock_execute = Mock()
        self.mock_execute.execute = Mock(return_value={'done': True})
        self.mock_get = Mock()
        self.mock_get.get = Mock(return_value=self.mock_execute)
        self.mock_operations = Mock()
        self.mock_operations.get = Mock(return_value=self.mock_get)
        self.mock_regions = Mock()
        self.mock_regions.operations = Mock(return_value=self.mock_operations)
        self.mock_projects = Mock()
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
            dataproc_task = DataprocClusterScaleOperator(
                task_id=TASK_ID,
                cluster_name=CLUSTER_NAME,
                project_id=PROJECT_ID,
                num_workers=NUM_WORKERS,
                num_preemptible_workers=NUM_PREEMPTIBLE_WORKERS,
                dag=self.dag
            )
            with patch.object(dataproc_task.log, 'info') as mock_info:
                with self.assertRaises(TypeError):
                    dataproc_task.execute(None)
                mock_info.assert_called_with('Scaling cluster: %s', CLUSTER_NAME)

    def test_cluster_name_log_sub(self):
        with patch('airflow.contrib.operators.dataproc_operator.DataProcHook') \
                as mock_hook:
            mock_hook.return_value.get_conn = self.mock_conn
            dataproc_task = DataprocClusterScaleOperator(
                task_id=TASK_ID,
                cluster_name='smoke-cluster-{{ ts_nodash }}',
                project_id=PROJECT_ID,
                num_workers=NUM_WORKERS,
                num_preemptible_workers=NUM_PREEMPTIBLE_WORKERS,
                dag=self.dag
            )

            with patch.object(dataproc_task.log, 'info') as mock_info:
                context = {'ts_nodash': 'testnodash'}

                rendered = dataproc_task.render_template(
                    'cluster_name',
                    getattr(dataproc_task, 'cluster_name'), context)
                setattr(dataproc_task, 'cluster_name', rendered)
                with self.assertRaises(TypeError):
                    dataproc_task.execute(None)
                mock_info.assert_called_with('Scaling cluster: %s',
                                             u'smoke-cluster-testnodash')


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
                context = {'ts_nodash': 'testnodash'}

                rendered = dataproc_task.render_template(
                    'cluster_name',
                    getattr(dataproc_task, 'cluster_name'), context)
                setattr(dataproc_task, 'cluster_name', rendered)
                with self.assertRaises(TypeError):
                    dataproc_task.execute(None)
                mock_info.assert_called_with('Deleting cluster: %s',
                                             u'smoke-cluster-testnodash')


class DataProcHadoopOperatorTest(unittest.TestCase):
    # Unit test for the DataProcHadoopOperator
    @staticmethod
    def test_hook_correct_region():
        with patch(HOOK) as mock_hook:
            dataproc_task = DataProcHadoopOperator(
                task_id=TASK_ID,
                region=REGION
            )

            dataproc_task.execute(None)
            mock_hook.return_value.submit.assert_called_once_with(mock.ANY, mock.ANY,
                                                                  REGION)

    @staticmethod
    def test_dataproc_job_id_is_set():
        with patch(HOOK) as mock_hook:
            dataproc_task = DataProcHadoopOperator(
                task_id=TASK_ID
            )

            _assert_dataproc_job_id(mock_hook, dataproc_task)


class DataProcHiveOperatorTest(unittest.TestCase):
    # Unit test for the DataProcHiveOperator
    @staticmethod
    def test_hook_correct_region():
        with patch(HOOK) as mock_hook:
            dataproc_task = DataProcHiveOperator(
                task_id=TASK_ID,
                region=REGION
            )

            dataproc_task.execute(None)
            mock_hook.return_value.submit.assert_called_once_with(mock.ANY, mock.ANY,
                                                                  REGION)

    @staticmethod
    def test_dataproc_job_id_is_set():
        with patch(HOOK) as mock_hook:
            dataproc_task = DataProcHiveOperator(
                task_id=TASK_ID
            )

            _assert_dataproc_job_id(mock_hook, dataproc_task)


class DataProcPySparkOperatorTest(unittest.TestCase):
    # Unit test for the DataProcPySparkOperator
    @staticmethod
    def test_hook_correct_region():
        with patch(HOOK) as mock_hook:
            dataproc_task = DataProcPySparkOperator(
                task_id=TASK_ID,
                main=MAIN_URI,
                region=REGION
            )

            dataproc_task.execute(None)
            mock_hook.return_value.submit.assert_called_once_with(mock.ANY, mock.ANY,
                                                                  REGION)

    @staticmethod
    def test_dataproc_job_id_is_set():
        with patch(HOOK) as mock_hook:
            dataproc_task = DataProcPySparkOperator(
                task_id=TASK_ID,
                main=MAIN_URI
            )

            _assert_dataproc_job_id(mock_hook, dataproc_task)


class DataProcSparkOperatorTest(unittest.TestCase):
    # Unit test for the DataProcSparkOperator
    @staticmethod
    def test_hook_correct_region():
        with patch(HOOK) as mock_hook:
            dataproc_task = DataProcSparkOperator(
                task_id=TASK_ID,
                region=REGION
            )

            dataproc_task.execute(None)
            mock_hook.return_value.submit.assert_called_once_with(mock.ANY, mock.ANY,
                                                                  REGION)

    @staticmethod
    def test_dataproc_job_id_is_set():
        with patch(HOOK) as mock_hook:
            dataproc_task = DataProcSparkOperator(
                task_id=TASK_ID
            )

            _assert_dataproc_job_id(mock_hook, dataproc_task)


class DataprocWorkflowTemplateInstantiateOperatorTest(unittest.TestCase):
    def setUp(self):
        # Setup service.projects().regions().workflowTemplates().instantiate().execute()
        self.operation = {'name': 'operation', 'done': True}
        self.mock_execute = Mock()
        self.mock_execute.execute.return_value = self.operation
        self.mock_workflows = Mock()
        self.mock_workflows.instantiate.return_value = self.mock_execute
        self.mock_regions = Mock()
        self.mock_regions.workflowTemplates.return_value = self.mock_workflows
        self.mock_projects = Mock()
        self.mock_projects.regions.return_value = self.mock_regions
        self.mock_conn = Mock()
        self.mock_conn.projects.return_value = self.mock_projects
        self.dag = DAG(
            'test_dag',
            default_args={
                'owner': 'airflow',
                'start_date': DEFAULT_DATE,
                'end_date': DEFAULT_DATE,
            },
            schedule_interval='@daily')

    def test_workflow(self):
        with patch(HOOK) as MockHook:
            hook = MockHook()
            hook.get_conn.return_value = self.mock_conn
            hook.wait.return_value = None

            dataproc_task = DataprocWorkflowTemplateInstantiateOperator(
                task_id=TASK_ID,
                project_id=PROJECT_ID,
                region=REGION,
                template_id=TEMPLATE_ID,
                dag=self.dag
            )

            dataproc_task.execute(None)
            template_name = (
                'projects/test-project-id/regions/test-region/'
                'workflowTemplates/template-id')
            self.mock_workflows.instantiate.assert_called_once_with(
                name=template_name,
                body=mock.ANY)
            hook.wait.assert_called_once_with(self.operation)


class DataprocWorkflowTemplateInstantiateInlineOperatorTest(unittest.TestCase):
    def setUp(self):
        # Setup service.projects().regions().workflowTemplates().instantiateInline()
        #              .execute()
        self.operation = {'name': 'operation', 'done': True}
        self.mock_execute = Mock()
        self.mock_execute.execute.return_value = self.operation
        self.mock_workflows = Mock()
        self.mock_workflows.instantiateInline.return_value = self.mock_execute
        self.mock_regions = Mock()
        self.mock_regions.workflowTemplates.return_value = self.mock_workflows
        self.mock_projects = Mock()
        self.mock_projects.regions.return_value = self.mock_regions
        self.mock_conn = Mock()
        self.mock_conn.projects.return_value = self.mock_projects
        self.dag = DAG(
            'test_dag',
            default_args={
                'owner': 'airflow',
                'start_date': DEFAULT_DATE,
                'end_date': DEFAULT_DATE,
            },
            schedule_interval='@daily')

    def test_iniline_workflow(self):
        with patch(HOOK) as MockHook:
            hook = MockHook()
            hook.get_conn.return_value = self.mock_conn
            hook.wait.return_value = None

            template = {
                "placement": {
                    "managed_cluster": {
                        "cluster_name": CLUSTER_NAME,
                        "config": {
                            "gce_cluster_config": {
                                "zone_uri": ZONE,
                            }
                        }
                    }
                },
                "jobs": [
                    {
                        "step_id": "say-hello",
                        "pig_job": {
                            "query": "sh echo hello"
                        }
                    }],
            }

            dataproc_task = DataprocWorkflowTemplateInstantiateInlineOperator(
                task_id=TASK_ID,
                project_id=PROJECT_ID,
                region=REGION,
                template=template,
                dag=self.dag
            )

            dataproc_task.execute(None)
            self.mock_workflows.instantiateInline.assert_called_once_with(
                parent='projects/test-project-id/regions/test-region',
                instanceId=mock.ANY,
                body=template)
            hook.wait.assert_called_once_with(self.operation)
