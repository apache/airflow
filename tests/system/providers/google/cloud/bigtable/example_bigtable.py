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
"""
Example Airflow DAG that creates and performs following operations on Cloud Bigtable:
- creates an Instance
- creates a Table
- updates Cluster
- waits for Table replication completeness
- deletes the Table
- deletes the Instance

This DAG relies on the following environment variables:

* GCP_PROJECT_ID - Google Cloud project
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
    See https://googleapis.github.io/google-cloud-python/latest/bigtable/instance.html#google.cloud.bigtable.instance.Instance.cluster
* CBT_TABLE_ID - desired ID of the Table
* CBT_POKE_INTERVAL - number of seconds between every attempt of Sensor check
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow import models
from airflow.decorators import task_group
from airflow.providers.google.cloud.operators.bigtable import (
    BigtableCreateInstanceOperator,
    BigtableCreateTableOperator,
    BigtableDeleteInstanceOperator,
    BigtableDeleteTableOperator,
    BigtableUpdateClusterOperator,
    BigtableUpdateInstanceOperator,
)
from airflow.providers.google.cloud.sensors.bigtable import BigtableTableReplicationCompletedSensor
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT")
DAG_ID = "bigtable"


CBT_INSTANCE_ID = f"bigtable-instance-id-{ENV_ID}"
CBT_INSTANCE_DISPLAY_NAME = "Instance-name"
CBT_INSTANCE_DISPLAY_NAME_UPDATED = f"{CBT_INSTANCE_DISPLAY_NAME} - updated"
CBT_INSTANCE_TYPE = 2
CBT_INSTANCE_TYPE_PROD = 1
CBT_INSTANCE_LABELS = {}
CBT_INSTANCE_LABELS_UPDATED = {"env": "prod"}
CBT_CLUSTER_ID = f"bigtable-cluster-id-{ENV_ID}"
CBT_CLUSTER_ZONE = "europe-west1-b"
CBT_CLUSTER_NODES = 3
CBT_CLUSTER_NODES_UPDATED = 5
CBT_CLUSTER_STORAGE_TYPE = 2
CBT_TABLE_ID = f"bigtable-table-id{ENV_ID}"
CBT_POKE_INTERVAL = 60


with models.DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["bigtable", "example"],
) as dag:
    # [START howto_operator_gcp_bigtable_instance_create]
    create_instance_task = BigtableCreateInstanceOperator(
        project_id=PROJECT_ID,
        instance_id=CBT_INSTANCE_ID,
        main_cluster_id=CBT_CLUSTER_ID,
        main_cluster_zone=CBT_CLUSTER_ZONE,
        instance_display_name=CBT_INSTANCE_DISPLAY_NAME,
        instance_type=CBT_INSTANCE_TYPE,
        instance_labels=CBT_INSTANCE_LABELS,
        cluster_nodes=None,
        cluster_storage_type=CBT_CLUSTER_STORAGE_TYPE,
        task_id="create_instance_task",
    )
    create_instance_task2 = BigtableCreateInstanceOperator(
        instance_id=CBT_INSTANCE_ID,
        main_cluster_id=CBT_CLUSTER_ID,
        main_cluster_zone=CBT_CLUSTER_ZONE,
        instance_display_name=CBT_INSTANCE_DISPLAY_NAME,
        instance_type=CBT_INSTANCE_TYPE,
        instance_labels=CBT_INSTANCE_LABELS,
        cluster_nodes=CBT_CLUSTER_NODES,
        cluster_storage_type=CBT_CLUSTER_STORAGE_TYPE,
        task_id="create_instance_task2",
    )
    # [END howto_operator_gcp_bigtable_instance_create]

    @task_group()
    def create_tables():
        # [START howto_operator_gcp_bigtable_table_create]
        create_table_task = BigtableCreateTableOperator(
            project_id=PROJECT_ID,
            instance_id=CBT_INSTANCE_ID,
            table_id=CBT_TABLE_ID,
            task_id="create_table",
        )
        create_table_task2 = BigtableCreateTableOperator(
            instance_id=CBT_INSTANCE_ID,
            table_id=CBT_TABLE_ID,
            task_id="create_table_task2",
        )
        # [END howto_operator_gcp_bigtable_table_create]
        create_table_task >> create_table_task2

    @task_group()
    def update_clusters_and_instance():
        # [START howto_operator_gcp_bigtable_cluster_update]
        cluster_update_task = BigtableUpdateClusterOperator(
            project_id=PROJECT_ID,
            instance_id=CBT_INSTANCE_ID,
            cluster_id=CBT_CLUSTER_ID,
            nodes=CBT_CLUSTER_NODES_UPDATED,
            task_id="update_cluster_task",
        )
        cluster_update_task2 = BigtableUpdateClusterOperator(
            instance_id=CBT_INSTANCE_ID,
            cluster_id=CBT_CLUSTER_ID,
            nodes=CBT_CLUSTER_NODES_UPDATED,
            task_id="update_cluster_task2",
        )
        # [END howto_operator_gcp_bigtable_cluster_update]

        # [START howto_operator_gcp_bigtable_instance_update]
        update_instance_task = BigtableUpdateInstanceOperator(
            instance_id=CBT_INSTANCE_ID,
            instance_display_name=CBT_INSTANCE_DISPLAY_NAME_UPDATED,
            instance_type=CBT_INSTANCE_TYPE_PROD,
            instance_labels=CBT_INSTANCE_LABELS_UPDATED,
            task_id="update_instance_task",
        )
        # [END howto_operator_gcp_bigtable_instance_update]

        [cluster_update_task, cluster_update_task2] >> update_instance_task

    # [START howto_operator_gcp_bigtable_table_wait_for_replication]
    wait_for_table_replication_task = BigtableTableReplicationCompletedSensor(
        instance_id=CBT_INSTANCE_ID,
        table_id=CBT_TABLE_ID,
        poke_interval=CBT_POKE_INTERVAL,
        timeout=180,
        task_id="wait_for_table_replication_task2",
    )
    # [END howto_operator_gcp_bigtable_table_wait_for_replication]

    # [START howto_operator_gcp_bigtable_table_delete]
    delete_table_task = BigtableDeleteTableOperator(
        project_id=PROJECT_ID,
        instance_id=CBT_INSTANCE_ID,
        table_id=CBT_TABLE_ID,
        task_id="delete_table_task",
    )
    delete_table_task2 = BigtableDeleteTableOperator(
        instance_id=CBT_INSTANCE_ID,
        table_id=CBT_TABLE_ID,
        task_id="delete_table_task2",
    )
    # [END howto_operator_gcp_bigtable_table_delete]
    delete_table_task.trigger_rule = TriggerRule.ALL_DONE
    delete_table_task2.trigger_rule = TriggerRule.ALL_DONE

    # [START howto_operator_gcp_bigtable_instance_delete]
    delete_instance_task = BigtableDeleteInstanceOperator(
        project_id=PROJECT_ID,
        instance_id=CBT_INSTANCE_ID,
        task_id="delete_instance_task",
    )
    delete_instance_task2 = BigtableDeleteInstanceOperator(
        instance_id=CBT_INSTANCE_ID,
        task_id="delete_instance_task2",
    )
    # [END howto_operator_gcp_bigtable_instance_delete]
    delete_instance_task.trigger_rule = TriggerRule.ALL_DONE
    delete_instance_task2.trigger_rule = TriggerRule.ALL_DONE

    (
        [create_instance_task, create_instance_task2]
        >> create_tables()
        >> wait_for_table_replication_task
        >> update_clusters_and_instance()
        >> delete_table_task
        >> delete_table_task2
        >> [delete_instance_task, delete_instance_task2]
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
