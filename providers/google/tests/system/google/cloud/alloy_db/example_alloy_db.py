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
    AlloyDBCreateBackupOperator,
    AlloyDBCreateClusterOperator,
    AlloyDBCreateInstanceOperator,
    AlloyDBCreateUserOperator,
    AlloyDBDeleteBackupOperator,
    AlloyDBDeleteClusterOperator,
    AlloyDBDeleteInstanceOperator,
    AlloyDBDeleteUserOperator,
    AlloyDBUpdateBackupOperator,
    AlloyDBUpdateClusterOperator,
    AlloyDBUpdateInstanceOperator,
    AlloyDBUpdateUserOperator,
)

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "alloy_db"
GCP_PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")

GCP_LOCATION = "europe-north1"
GCP_LOCATION_SECONDARY = "europe-west1"
GCP_LOCATION_BACKUP = "europe-west2"
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
INSTANCE_ID = f"instance-{DAG_ID}-{ENV_ID}".replace("_", "-")
INSTANCE = {
    "instance_type": "PRIMARY",
}
INSTANCE_UPDATE = {"labels": {"label_test": "test_value"}}
INSTANCE_UPDATE_MASK = {"paths": ["labels"]}
SECONDARY_INSTANCE = {
    "instance_type": "SECONDARY",
}
SECONDARY_INSTANCE_ID = f"instance-secondary-{DAG_ID}-{ENV_ID}".replace("_", "-")
USER_ID = "test-user"
USER = {
    "password": "Test-Pa$$w0rd",
    "user_type": "ALLOYDB_BUILT_IN",
}
USER_UPDATE = {
    "database_roles": [
        "alloydbsuperuser",
    ]
}
USER_UPDATE_MASK = {
    "paths": ["database_roles"],
}
BACKUP_ID = "test-backup"
BACKUP = {
    "cluster_name": f"projects/{GCP_PROJECT_ID}/locations/{GCP_LOCATION}/clusters/{CLUSTER_ID}",
    "type": "ON_DEMAND",
}
BACKUP_UPDATE = {"labels": {"label_test": "test_value"}}
BACKUP_UPDATE_MASK = {
    "paths": ["labels"],
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

    # [START howto_operator_alloy_db_create_instance]
    create_instance = AlloyDBCreateInstanceOperator(
        task_id="create_instance",
        cluster_id=CLUSTER_ID,
        instance_id=INSTANCE_ID,
        instance_configuration=INSTANCE,
        is_secondary=False,
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
    )
    # [END howto_operator_alloy_db_create_instance]

    # [START howto_operator_alloy_db_create_backup]
    create_backup = AlloyDBCreateBackupOperator(
        task_id="create_backup",
        backup_id=BACKUP_ID,
        backup_configuration=BACKUP,
        location=GCP_LOCATION_BACKUP,
        project_id=GCP_PROJECT_ID,
    )
    # [END howto_operator_alloy_db_create_backup]

    # [START howto_operator_alloy_db_update_backup]
    update_backup = AlloyDBUpdateBackupOperator(
        task_id="update_backup",
        backup_id=BACKUP_ID,
        backup_configuration=BACKUP_UPDATE,
        update_mask=BACKUP_UPDATE_MASK,
        location=GCP_LOCATION_BACKUP,
        project_id=GCP_PROJECT_ID,
    )
    # [END howto_operator_alloy_db_update_backup]

    # [START howto_operator_alloy_db_update_instance]
    update_instance = AlloyDBUpdateInstanceOperator(
        task_id="update_instance",
        cluster_id=CLUSTER_ID,
        instance_id=INSTANCE_ID,
        instance_configuration=INSTANCE_UPDATE,
        update_mask=INSTANCE_UPDATE_MASK,
        location=GCP_LOCATION,
        project_id=GCP_PROJECT_ID,
    )
    # [END howto_operator_alloy_db_update_instance]

    create_secondary_cluster = AlloyDBCreateClusterOperator(
        task_id="create_secondary_cluster",
        cluster_id=SECONDARY_CLUSTER_ID,
        cluster_configuration=SECONDARY_CLUSTER,
        is_secondary=True,
        location=GCP_LOCATION_SECONDARY,
        project_id=GCP_PROJECT_ID,
    )

    create_secondary_instance = AlloyDBCreateInstanceOperator(
        task_id="create_secondary_instance",
        cluster_id=SECONDARY_CLUSTER_ID,
        instance_id=SECONDARY_INSTANCE_ID,
        instance_configuration=SECONDARY_INSTANCE,
        is_secondary=True,
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION_SECONDARY,
    )

    # [START howto_operator_alloy_db_create_user]
    creat_user = AlloyDBCreateUserOperator(
        task_id="create_user",
        user_id=USER_ID,
        user_configuration=USER,
        cluster_id=CLUSTER_ID,
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
    )
    # [END howto_operator_alloy_db_create_user]

    # [START howto_operator_alloy_db_update_user]
    update_user = AlloyDBUpdateUserOperator(
        task_id="update_user",
        user_id=USER_ID,
        user_configuration=USER_UPDATE,
        cluster_id=CLUSTER_ID,
        update_mask=USER_UPDATE_MASK,
        location=GCP_LOCATION,
        project_id=GCP_PROJECT_ID,
    )
    # [END howto_operator_alloy_db_update_user]

    # [START howto_operator_alloy_db_delete_user]
    delete_user = AlloyDBDeleteUserOperator(
        task_id="delete_user",
        cluster_id=CLUSTER_ID,
        user_id=USER_ID,
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
    )
    # [END howto_operator_alloy_db_delete_user]

    # [START howto_operator_alloy_db_delete_backup]
    delete_backup = AlloyDBDeleteBackupOperator(
        task_id="delete_backup",
        backup_id=BACKUP_ID,
        location=GCP_LOCATION_BACKUP,
        project_id=GCP_PROJECT_ID,
    )
    # [END howto_operator_alloy_db_delete_backup]
    delete_backup.trigger_rule = TriggerRule.ALL_DONE

    # [START howto_operator_alloy_db_delete_instance]
    delete_instance = AlloyDBDeleteInstanceOperator(
        task_id="delete_instance",
        cluster_id=CLUSTER_ID,
        instance_id=INSTANCE_ID,
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
    )
    # [END howto_operator_alloy_db_delete_instance]
    delete_instance.trigger_rule = TriggerRule.ALL_DONE

    delete_secondary_cluster = AlloyDBDeleteClusterOperator(
        task_id="delete_secondary_cluster",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION_SECONDARY,
        cluster_id=SECONDARY_CLUSTER_ID,
        trigger_rule=TriggerRule.ALL_DONE,
        force=True,
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

    (
        create_cluster
        >> update_cluster
        >> create_instance
        >> update_instance
        >> create_backup
        >> update_backup
        >> create_secondary_cluster
        >> create_secondary_instance
        >> creat_user
        >> update_user
        >> delete_user
        >> delete_backup
        >> delete_secondary_cluster
        >> delete_instance
        >> delete_cluster
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
