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

import unittest

from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator


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


class DataprocClusterCreateOperatorTest(unittest.TestCase):

    def setUp(self):
        self.dataproc = DataprocClusterCreateOperator(
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
            num_preemptible_workers=NUM_PREEMPTIBLE_WORKERS)

    def test_init(self):
        """Test DataFlowPythonOperator instance is properly initialized."""
        self.assertEqual(self.dataproc.cluster_name, CLUSTER_NAME)
        self.assertEqual(self.dataproc.project_id, PROJECT_ID)
        self.assertEqual(self.dataproc.num_workers, NUM_WORKERS)
        self.assertEqual(self.dataproc.zone, ZONE)
        self.assertEqual(self.dataproc.storage_bucket, STORAGE_BUCKET)
        self.assertEqual(self.dataproc.image_version, IMAGE_VERSION)
        self.assertEqual(self.dataproc.master_machine_type, MASTER_MACHINE_TYPE)
        self.assertEqual(self.dataproc.master_disk_size, MASTER_DISK_SIZE)
        self.assertEqual(self.dataproc.worker_machine_type, WORKER_MACHINE_TYPE)
        self.assertEqual(self.dataproc.worker_disk_size, WORKER_DISK_SIZE)
        self.assertEqual(self.dataproc.num_preemptible_workers, NUM_PREEMPTIBLE_WORKERS)

    def test_build_cluster_data(self):
        cluster_data = self.dataproc._build_cluster_data()
        self.assertEqual(cluster_data['clusterName'], CLUSTER_NAME)
        self.assertEqual(cluster_data['projectId'], PROJECT_ID)
        self.assertEqual(cluster_data['config']['softwareConfig'], {'imageVersion': IMAGE_VERSION})
        self.assertEqual(cluster_data['config']['configBucket'], STORAGE_BUCKET)
        self.assertEqual(cluster_data['config']['workerConfig']['numInstances'], NUM_WORKERS)
        self.assertEqual(cluster_data['config']['secondaryWorkerConfig']['numInstances'],
                         NUM_PREEMPTIBLE_WORKERS)
