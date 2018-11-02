# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# 'License'); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# 'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Example Airflow DAG that creates and performs following operations on Cloud Bigtable:
- creates an Instance
- creates a Table
- updates Cluster
- waits for Table replication completeness
- deletes the Table
- deletes the Instance

This DAG relies on the following environment variables
* GCP_PROJECT_ID - Google Cloud Platform project
* CBT_INSTANCE_ID - desired ID of a Cloud Bigtable instance
* CBT_INSTANCE_DISPLAY_NAME - desired human-readable display name of the Instance
* CBT_INSTANCE_TYPE - type of the Instance, e.g. 1 for DEVELOPMENT
    See https://googleapis.github.io/google-cloud-python/latest/bigtable/instance.html#google.cloud.bigtable.instance.Instance
* CBT_INSTANCE_LABELS - labels to add for the Instance
* CBT_CLUSTER_ID - desired ID of the main Cluster created for the Instance
* CBT_CLUSTER_ZONE - zone in which main Cluster will be created. e.g. europe-west1-b
    See available zones: https://cloud.google.com/bigtable/docs/locations
* CBT_CLUSTER_NODES - initial amount of nodes of the Cluster
* CBT_CLUSTER_NODES_UPDATED - amount of nodes for BigtableClusterUpdateOperator
* CBT_CLUSTER_STORAGE_TYPE - storage for the Cluster, e.g. 1 for SSD
    See https://googleapis.github.io/google-cloud-python/latest/bigtable/instance.html#google.cloud.bigtable.instance.Instance.cluster # noqa: E501
* CBT_TABLE_ID - desired ID of the Table
* CBT_POKE_INTERVAL - number of seconds between every attempt of Sensor check

"""

import datetime
import json

from os import getenv

import airflow
from airflow import models
from airflow.contrib.operators.gcp_bigtable_operator import BigtableInstanceCreateOperator, \
    BigtableInstanceDeleteOperator, BigtableClusterUpdateOperator, BigtableTableCreateOperator, \
    BigtableTableWaitForReplicationSensor, BigtableTableDeleteOperator

# [START howto_operator_gcp_bigtable_args]
GCP_PROJECT_ID = getenv('GCP_PROJECT_ID', 'example-project')
CBT_INSTANCE_ID = getenv('CBT_INSTANCE_ID', 'some-instance-id')
CBT_INSTANCE_DISPLAY_NAME = getenv('CBT_INSTANCE_DISPLAY_NAME', 'Human-readable name')
CBT_INSTANCE_TYPE = getenv('CBT_INSTANCE_TYPE', '2')
CBT_INSTANCE_LABELS = getenv('CBT_INSTANCE_LABELS', '{}')
CBT_CLUSTER_ID = getenv('CBT_CLUSTER_ID', 'some-cluster-id')
CBT_CLUSTER_ZONE = getenv('CBT_CLUSTER_ZONE', 'europe-west1-b')
CBT_CLUSTER_NODES = getenv('CBT_CLUSTER_NODES', '3')
CBT_CLUSTER_NODES_UPDATED = getenv('CBT_CLUSTER_NODES_UPDATED', '5')
CBT_CLUSTER_STORAGE_TYPE = getenv('CBT_CLUSTER_STORAGE_TYPE', '2')
CBT_TABLE_ID = getenv('CBT_TABLE_ID', 'some-table-id')
CBT_POKE_INTERVAL = getenv('CBT_POKE_INTERVAL', '60')
# [END howto_operator_gcp_bigtable_args]

default_args = {
    'start_date': airflow.utils.dates.days_ago(1)
}

with models.DAG(
    'example_gcp_bigtable_operators',
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1)
) as dag:
    # [START howto_operator_gcp_bigtable_instance_create]
    create_instance_task = BigtableInstanceCreateOperator(
        project_id=GCP_PROJECT_ID,
        instance_id=CBT_INSTANCE_ID,
        main_cluster_id=CBT_CLUSTER_ID,
        main_cluster_zone=CBT_CLUSTER_ZONE,
        instance_display_name=CBT_INSTANCE_DISPLAY_NAME,
        instance_type=int(CBT_INSTANCE_TYPE),
        instance_labels=json.loads(CBT_INSTANCE_LABELS),
        cluster_nodes=int(CBT_CLUSTER_NODES),
        cluster_storage_type=int(CBT_CLUSTER_STORAGE_TYPE),
        task_id='create_instance',
    )
    # [END howto_operator_gcp_bigtable_instance_create]

    # [START howto_operator_gcp_bigtable_cluster_update]
    cluster_update_task = BigtableClusterUpdateOperator(
        project_id=GCP_PROJECT_ID,
        instance_id=CBT_INSTANCE_ID,
        cluster_id=CBT_CLUSTER_ID,
        nodes=int(CBT_CLUSTER_NODES_UPDATED),
        task_id='update_cluster',
    )
    # [END howto_operator_gcp_bigtable_cluster_update]

    # [START howto_operator_gcp_bigtable_instance_delete]
    delete_instance_task = BigtableInstanceDeleteOperator(
        project_id=GCP_PROJECT_ID,
        instance_id=CBT_INSTANCE_ID,
        task_id='delete_instance',
    )
    # [END howto_operator_gcp_bigtable_instance_delete]

    # [START howto_operator_gcp_bigtable_table_create]
    create_table_task = BigtableTableCreateOperator(
        project_id=GCP_PROJECT_ID,
        instance_id=CBT_INSTANCE_ID,
        table_id=CBT_TABLE_ID,
        task_id='create_table',
    )
    # [END howto_operator_gcp_bigtable_table_create]

    # [START howto_operator_gcp_bigtable_table_wait_for_replication]
    wait_for_table_replication_task = BigtableTableWaitForReplicationSensor(
        project_id=GCP_PROJECT_ID,
        instance_id=CBT_INSTANCE_ID,
        table_id=CBT_TABLE_ID,
        poke_interval=int(CBT_POKE_INTERVAL),
        task_id='wait_for_table_replication',
    )
    # [END howto_operator_gcp_bigtable_table_wait_for_replication]

    # [START howto_operator_gcp_bigtable_table_delete]
    delete_table_task = BigtableTableDeleteOperator(
        project_id=GCP_PROJECT_ID,
        instance_id=CBT_INSTANCE_ID,
        table_id=CBT_TABLE_ID,
        task_id='delete_table',
    )
    # [END howto_operator_gcp_bigtable_table_delete]

    wait_for_table_replication_task >> delete_table_task
    create_instance_task \
        >> create_table_task \
        >> cluster_update_task \
        >> delete_table_task \
        >> delete_instance_task
