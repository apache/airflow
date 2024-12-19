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
Example Airflow DAG for Google AlloyDB operators.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.alloy_db import (
    AlloyDBCreateClusterOperator,
    AlloyDBDeleteClusterOperator,
    AlloyDBUpdateClusterOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "alloy_db"
GCP_PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")

GCP_LOCATION = "europe-north1"
GCP_LOCATION_SECONDARY = "europe-west1"
GCP_NETWORK = "default"
CLUSTER_USER = "postgres-test"
CLUSTER_USER_PASSWORD = "postgres-test-pa$$w0rd"
CLUSTER_ID = f"cluster-{DAG_ID}-{ENV_ID}".replace("_", "-")
SECONDARY_CLUSTER_ID = f"cluster-secondary-{DAG_ID}-{ENV_ID}".replace("_", "-")
CLUSTER = {
    "network": f"projects/{GCP_PROJECT_ID}/global/networks/{GCP_NETWORK}",
    "initial_user": {
        "user": CLUSTER_USER,
        "password": CLUSTER_USER_PASSWORD,
    },
}
CLUSTER_UPDATE = {
    "automated_backup_policy": {
        "enabled": True,
    }
}
CLUSTER_UPDATE_MASK = {"paths": ["automated_backup_policy.enabled"]}
SECONDARY_CLUSTER = {
    "network": f"projects/{GCP_PROJECT_ID}/global/networks/{GCP_NETWORK}",
    "secondary_config": {
        "primary_cluster_name": f"projects/{GCP_PROJECT_ID}/locations/{GCP_LOCATION}/clusters/{CLUSTER_ID}",
    },
}

with DAG(
    DAG_ID,
    schedule="@once",  # Override to match your needs
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "alloy-db"],
) as dag:
    # [START howto_operator_alloy_db_create_cluster]
    create_cluster = AlloyDBCreateClusterOperator(
        task_id="create_cluster",
        cluster_id=CLUSTER_ID,
        cluster_configuration=CLUSTER,
        is_secondary=False,
        location=GCP_LOCATION,
        project_id=GCP_PROJECT_ID,
    )
    # [END howto_operator_alloy_db_create_cluster]

    # [START howto_operator_alloy_db_update_cluster]
    update_cluster = AlloyDBUpdateClusterOperator(
        task_id="update_cluster",
        cluster_id=CLUSTER_ID,
        cluster_configuration=CLUSTER_UPDATE,
        update_mask=CLUSTER_UPDATE_MASK,
        location=GCP_LOCATION,
        project_id=GCP_PROJECT_ID,
    )
    # [END howto_operator_alloy_db_update_cluster]

    create_secondary_cluster = AlloyDBCreateClusterOperator(
        task_id="create_secondary_cluster",
        cluster_id=SECONDARY_CLUSTER_ID,
        cluster_configuration=SECONDARY_CLUSTER,
        is_secondary=True,
        location=GCP_LOCATION_SECONDARY,
        project_id=GCP_PROJECT_ID,
    )

    delete_secondary_cluster = AlloyDBDeleteClusterOperator(
        task_id="delete_secondary_cluster",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION_SECONDARY,
        cluster_id=SECONDARY_CLUSTER_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # [START howto_operator_alloy_db_delete_cluster]
    delete_cluster = AlloyDBDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        cluster_id=CLUSTER_ID,
    )
    # [END howto_operator_alloy_db_delete_cluster]

    delete_cluster.trigger_rule = TriggerRule.ALL_DONE

    create_cluster >> update_cluster >> create_secondary_cluster >> delete_secondary_cluster >> delete_cluster

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
