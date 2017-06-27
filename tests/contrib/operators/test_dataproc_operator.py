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
from airflow.version import version

from copy import deepcopy

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

class DataprocClusterCreateOperatorTest(unittest.TestCase):
    # Unitest for the DataprocClusterCreateOperator
    def setUp(self):
        # instantiate two different test cases with different labels 
        self.labels = [LABEL1, LABEL2]
        self.dataproc_operators = []
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
                    labels = deepcopy(labels)
                )
             )

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
            # test whether the default airflow_version label has been properly set to the dataproc operator 
            merged_labels = {}
            merged_labels.update(self.labels[suffix])
            merged_labels.update({'airflow_version': version})
            self.assertEqual(cluster_data['labels'], merged_labels)
