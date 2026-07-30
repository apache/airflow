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
from __future__ import annotations

import contextlib
from datetime import datetime

import boto3

from airflow.providers.amazon.aws.hooks.neptune_analytics import NeptuneAnalyticsHook
from airflow.providers.amazon.aws.operators.neptune_analytics import (
    NeptuneCancelImportTaskOperator,
    NeptuneCreateGraphOperator,
    NeptuneCreateGraphWithImportOperator,
    NeptuneCreatePrivateGraphEndpointOperator,
    NeptuneDeleteGraphOperator,
    NeptuneDeletePrivateGraphEndpointOperator,
    NeptuneStartImportTaskOperator,
)
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
)
from airflow.providers.common.compat.sdk import DAG, chain, task

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from system.amazon.aws.utils import SystemTestContextBuilder

DAG_ID = "example_neptune_analytics"

NEPTUNE_IMPORT_ROLE_ARN_KEY = "NEPTUNE_IMPORT_ROLE_ARN"

sys_test_context_task = SystemTestContextBuilder().add_variable(NEPTUNE_IMPORT_ROLE_ARN_KEY).build()

# Minimal OpenCypher CSV data for import testing.
NODES_CSV = """~id,~label,name:String
n1,Person,Alice
n2,Person,Bob
"""

EDGES_CSV = """~id,~from,~to,~label
e1,n1,n2,KNOWS
"""


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_graph_if_exists(graph_name: str) -> None:
    """Safety net to clean up the graph in case a previous task failed."""
    hook = NeptuneAnalyticsHook()
    with contextlib.suppress(Exception):
        # List graphs and find by name
        paginator = hook.conn.get_paginator("list_graphs")
        for page in paginator.paginate():
            for graph in page.get("graphs", []):
                if graph.get("name") == graph_name:
                    graph_id = graph["id"]

                    # Delete any attached private graph endpoints before deleting the graph
                    endpoints_paginator = hook.conn.get_paginator("list_private_graph_endpoints")
                    for ep_page in endpoints_paginator.paginate(graphIdentifier=graph_id):
                        for endpoint in ep_page.get("privateGraphEndpoints", []):
                            vpc_id = endpoint["vpcId"]
                            hook.conn.delete_private_graph_endpoint(graphIdentifier=graph_id, vpcId=vpc_id)
                            hook.conn.get_waiter("private_graph_endpoint_deleted").wait(
                                graphIdentifier=graph_id,
                                vpcId=vpc_id,
                                WaiterConfig={"Delay": 30, "MaxAttempts": 60},
                            )

                    # Disable deletion protection if enabled
                    hook.conn.update_graph(graphIdentifier=graph_id, deletionProtection=False)

                    hook.conn.delete_graph(graphIdentifier=graph_id, skipSnapshot=True)
                    hook.conn.get_waiter("graph_deleted").wait(
                        graphIdentifier=graph_id,
                        WaiterConfig={"Delay": 30, "MaxAttempts": 60},
                    )
                    return


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    test_context = sys_test_context_task()

    env_id = test_context["ENV_ID"]
    graph_name = f"{env_id}-graph"
    import_graph_name = f"{env_id}-import-graph"
    bucket_name = f"{env_id}-neptune-analytics"
    import_role_arn = test_context[NEPTUNE_IMPORT_ROLE_ARN_KEY]
    region = boto3.session.Session().region_name

    # --- TEST SETUP ---

    create_bucket = S3CreateBucketOperator(
        task_id="create_bucket",
        bucket_name=bucket_name,
    )

    upload_nodes = S3CreateObjectOperator(
        task_id="upload_nodes",
        s3_bucket=bucket_name,
        s3_key="data/nodes.csv",
        data=NODES_CSV,
        replace=True,
    )

    upload_edges = S3CreateObjectOperator(
        task_id="upload_edges",
        s3_bucket=bucket_name,
        s3_key="data/edges.csv",
        data=EDGES_CSV,
        replace=True,
    )

    # --- TEST BODY ---

    # [START howto_operator_neptune_analytics_create_graph]
    create_graph = NeptuneCreateGraphOperator(
        task_id="create_graph",
        graph_name=graph_name,
        vector_search_config={"dimension": 128},
        provisioned_memory=32,
        public_connectivity=True,
        replica_count=0,
        deletion_protection=False,
        wait_for_completion=True,
        deferrable=False,
        waiter_delay=30,
        waiter_max_attempts=60,
    )
    # [END howto_operator_neptune_analytics_create_graph]

    # [START howto_operator_neptune_analytics_create_private_endpoint]
    create_endpoint = NeptuneCreatePrivateGraphEndpointOperator(
        task_id="create_endpoint",
        graph_identifier="{{ ti.xcom_pull(task_ids='create_graph')['graph_id']}}",
        wait_for_completion=True,
    )
    # [END howto_operator_neptune_analytics_create_private_endpoint]

    # [START howto_operator_neptune_analytics_delete_private_endpoint]
    delete_endpoint = NeptuneDeletePrivateGraphEndpointOperator(
        task_id="delete_endpoint",
        graph_identifier="{{ ti.xcom_pull(task_ids='create_graph')['graph_id'] }}",
        vpc_id="{{ ti.xcom_pull(task_ids='create_endpoint')['vpc_id'] }}",
        wait_for_completion=True,
        deferrable=False,
        waiter_delay=30,
        waiter_max_attempts=60,
    )
    # [END howto_operator_neptune_analytics_delete_private_endpoint]

    # [START howto_operator_neptune_analytics_start_import_task]
    start_import = NeptuneStartImportTaskOperator(
        task_id="start_import",
        graph_identifier="{{ ti.xcom_pull(task_ids='create_graph')['graph_id'] }}",
        role_arn=import_role_arn,
        source=f"s3://{bucket_name}/data/",
        format="CSV",
        fail_on_error=True,
        wait_for_completion=False,
        deferrable=False,
        waiter_delay=30,
        waiter_max_attempts=60,
    )
    # [END howto_operator_neptune_analytics_start_import_task]

    # [START howto_operator_neptune_analytics_cancel_import_task]
    cancel_import = NeptuneCancelImportTaskOperator(
        task_id="cancel_import",
        import_task_id="{{ ti.xcom_pull(task_ids='start_import')['import_task_id']}}",
        wait_for_completion=True,
        aws_conn_id="aws_default",
    )
    # [END howto_operator_neptune_analytics_cancel_import_task]

    # [START howto_operator_neptune_analytics_delete_graph]
    delete_graph = NeptuneDeleteGraphOperator(
        task_id="delete_graph",
        graph_id="{{ ti.xcom_pull(task_ids='create_graph')['graph_id'] }}",
        skip_snapshot=True,
        wait_for_completion=True,
        deferrable=False,
        waiter_delay=30,
        waiter_max_attempts=60,
    )
    # [END howto_operator_neptune_analytics_delete_graph]

    # [START howto_operator_neptune_analytics_create_graph_with_import]
    create_graph_with_import = NeptuneCreateGraphWithImportOperator(
        task_id="create_graph_with_import",
        graph_name=import_graph_name,
        vector_search_config={"dimension": 128},
        source=f"s3://{bucket_name}/data/",
        role_arn=import_role_arn,
        format="CSV",
        fail_on_error=True,
        public_connectivity=True,
        replica_count=0,
        deletion_protection=False,
        min_provisioned_memory=32,
        max_provisioned_memory=32,
        wait_for_completion=True,
        deferrable=False,
        waiter_delay=30,
        waiter_max_attempts=60,
    )
    # [END howto_operator_neptune_analytics_create_graph_with_import]

    # [START howto_operator_neptune_analytics_delete_import_graph]
    delete_import_graph = NeptuneDeleteGraphOperator(
        task_id="delete_import_graph",
        graph_id="{{ ti.xcom_pull(task_ids='create_graph_with_import')['graph_id'] }}",
        skip_snapshot=True,
        wait_for_completion=True,
        deferrable=False,
        trigger_rule=TriggerRule.ALL_DONE,
        waiter_delay=30,
        waiter_max_attempts=60,
    )
    # [END howto_operator_neptune_analytics_delete_import_graph]

    # --- TEST TEARDOWN ---

    delete_bucket = S3DeleteBucketOperator(
        task_id="delete_bucket",
        trigger_rule=TriggerRule.ALL_DONE,
        bucket_name=bucket_name,
        force_delete=True,
    )

    cleanup_graph = delete_graph_if_exists.override(task_id="cleanup_graph")(graph_name)
    cleanup_import_graph = delete_graph_if_exists.override(task_id="cleanup_import_graph")(import_graph_name)

    chain(
        # TEST SETUP
        test_context,
        create_bucket,
        [upload_nodes, upload_edges],
        # TEST BODY: Create graph, import data, then delete
        create_graph,
        create_endpoint,
        start_import,
        cancel_import,
        delete_endpoint,
        delete_graph,
        # TEST BODY: Create graph with import, then delete
        create_graph_with_import,
        delete_import_graph,
        # TEST TEARDOWN
        [cleanup_graph, cleanup_import_graph],
        delete_bucket,
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
