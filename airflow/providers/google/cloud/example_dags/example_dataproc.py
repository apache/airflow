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
"""
Example Airflow DAG that show how to use various Dataproc
operators to manage a cluster and submit jobs.
"""

import os
from datetime import datetime
from uuid import uuid4

from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocCreateBatchOperator,
    DataprocCreateClusterOperator,
    DataprocCreateWorkflowTemplateOperator,
    DataprocDeleteBatchOperator,
    DataprocDeleteClusterOperator,
    DataprocGetBatchOperator,
    DataprocInstantiateInlineWorkflowTemplateOperator,
    DataprocInstantiateWorkflowTemplateOperator,
    DataprocListBatchesOperator,
    DataprocSubmitJobOperator,
    DataprocUpdateClusterOperator,
)
from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "an-id")
CLUSTER_NAME = os.environ.get("GCP_DATAPROC_CLUSTER_NAME", "example-cluster")
REGION = os.environ.get("GCP_LOCATION", "europe-west1")
ZONE = os.environ.get("GCP_REGION", "europe-west1-b")
BUCKET = os.environ.get("GCP_DATAPROC_BUCKET", "dataproc-system-tests")
OUTPUT_FOLDER = "wordcount"
OUTPUT_PATH = f"gs://{BUCKET}/{OUTPUT_FOLDER}/"
PYSPARK_MAIN = os.environ.get("PYSPARK_MAIN", "hello_world.py")
PYSPARK_URI = f"gs://{BUCKET}/{PYSPARK_MAIN}"
SPARKR_MAIN = os.environ.get("SPARKR_MAIN", "hello_world.R")
SPARKR_URI = f"gs://{BUCKET}/{SPARKR_MAIN}"

# Cluster definition
# [START how_to_cloud_dataproc_create_cluster]

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
}

# [END how_to_cloud_dataproc_create_cluster]

# Cluster definition: Generating Cluster Config for DataprocCreateClusterOperator
# [START how_to_cloud_dataproc_create_cluster_generate_cluster_config]
path = "gs://goog-dataproc-initialization-actions-us-central1/python/pip-install.sh"

CLUSTER_GENERATOR_CONFIG = ClusterGenerator(
    project_id="test",
    zone="us-central1-a",
    master_machine_type="n1-standard-4",
    worker_machine_type="n1-standard-4",
    num_workers=2,
    storage_bucket="test",
    init_actions_uris=[path],
    metadata={'PIP_PACKAGES': 'pyyaml requests pandas openpyxl'},
).make()

create_cluster_operator = DataprocCreateClusterOperator(
    task_id='create_dataproc_cluster',
    cluster_name="test",
    project_id="test",
    region="us-central1",
    cluster_config=CLUSTER_GENERATOR_CONFIG,
)
# [END how_to_cloud_dataproc_create_cluster_generate_cluster_config]

# Update options
# [START how_to_cloud_dataproc_updatemask_cluster_operator]
CLUSTER_UPDATE = {
    "config": {"worker_config": {"num_instances": 3}, "secondary_worker_config": {"num_instances": 3}}
}
UPDATE_MASK = {
    "paths": ["config.worker_config.num_instances", "config.secondary_worker_config.num_instances"]
}
# [END how_to_cloud_dataproc_updatemask_cluster_operator]

TIMEOUT = {"seconds": 1 * 24 * 60 * 60}

# Jobs definitions
# [START how_to_cloud_dataproc_pig_config]
PIG_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pig_job": {"query_list": {"queries": ["define sin HiveUDF('sin');"]}},
}
# [END how_to_cloud_dataproc_pig_config]

# [START how_to_cloud_dataproc_sparksql_config]
SPARK_SQL_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_sql_job": {"query_list": {"queries": ["SHOW DATABASES;"]}},
}
# [END how_to_cloud_dataproc_sparksql_config]

# [START how_to_cloud_dataproc_spark_config]
SPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_job": {
        "jar_file_uris": ["file:///usr/lib/spark/examples/jars/spark-examples.jar"],
        "main_class": "org.apache.spark.examples.SparkPi",
    },
}
# [END how_to_cloud_dataproc_spark_config]

# [START how_to_cloud_dataproc_pyspark_config]
PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
}
# [END how_to_cloud_dataproc_pyspark_config]

# [START how_to_cloud_dataproc_sparkr_config]
SPARKR_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_r_job": {"main_r_file_uri": SPARKR_URI},
}
# [END how_to_cloud_dataproc_sparkr_config]

# [START how_to_cloud_dataproc_hive_config]
HIVE_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "hive_job": {"query_list": {"queries": ["SHOW DATABASES;"]}},
}
# [END how_to_cloud_dataproc_hive_config]

# [START how_to_cloud_dataproc_hadoop_config]
HADOOP_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "hadoop_job": {
        "main_jar_file_uri": "file:///usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar",
        "args": ["wordcount", "gs://pub/shakespeare/rose.txt", OUTPUT_PATH],
    },
}
# [END how_to_cloud_dataproc_hadoop_config]
WORKFLOW_NAME = "airflow-dataproc-test"
WORKFLOW_TEMPLATE = {
    "id": WORKFLOW_NAME,
    "placement": {
        "managed_cluster": {
            "cluster_name": CLUSTER_NAME,
            "config": CLUSTER_CONFIG,
        }
    },
    "jobs": [{"step_id": "pig_job_1", "pig_job": PIG_JOB["pig_job"]}],
}
BATCH_ID = f"test-batch-id-{str(uuid4())}"
BATCH_CONFIG = {
    "spark_batch": {
        "jar_file_uris": ["file:///usr/lib/spark/examples/jars/spark-examples.jar"],
        "main_class": "org.apache.spark.examples.SparkPi",
    },
}
CLUSTER_GENERATOR_CONFIG_FOR_PHS = ClusterGenerator(
    project_id=PROJECT_ID,
    region=REGION,
    master_machine_type="n1-standard-4",
    worker_machine_type="n1-standard-4",
    num_workers=0,
    properties={
        "spark:spark.history.fs.logDirectory": f"gs://{BUCKET}/logging",
    },
    enable_component_gateway=True,
).make()
CLUSTER_NAME_FOR_PHS = "phs-cluster-name"
BATCH_CONFIG_WITH_PHS = {
    "spark_batch": {
        "jar_file_uris": ["file:///usr/lib/spark/examples/jars/spark-examples.jar"],
        "main_class": "org.apache.spark.examples.SparkPi",
    },
    "environment_config": {
        "peripherals_config": {
            "spark_history_server_config": {
                "dataproc_cluster": f"projects/{PROJECT_ID}/regions/{REGION}/clusters/{CLUSTER_NAME_FOR_PHS}"
            }
        }
    },
}


with models.DAG(
    "example_gcp_dataproc",
    schedule_interval='@once',
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    # [START how_to_cloud_dataproc_create_cluster_operator]
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )
    # [END how_to_cloud_dataproc_create_cluster_operator]

    # [START how_to_cloud_dataproc_update_cluster_operator]
    scale_cluster = DataprocUpdateClusterOperator(
        task_id="scale_cluster",
        cluster_name=CLUSTER_NAME,
        cluster=CLUSTER_UPDATE,
        update_mask=UPDATE_MASK,
        graceful_decommission_timeout=TIMEOUT,
        project_id=PROJECT_ID,
        region=REGION,
    )
    # [END how_to_cloud_dataproc_update_cluster_operator]

    # [START how_to_cloud_dataproc_create_workflow_template]
    create_workflow_template = DataprocCreateWorkflowTemplateOperator(
        task_id="create_workflow_template",
        template=WORKFLOW_TEMPLATE,
        project_id=PROJECT_ID,
        region=REGION,
    )
    # [END how_to_cloud_dataproc_create_workflow_template]

    # [START how_to_cloud_dataproc_trigger_workflow_template]
    trigger_workflow = DataprocInstantiateWorkflowTemplateOperator(
        task_id="trigger_workflow", region=REGION, project_id=PROJECT_ID, template_id=WORKFLOW_NAME
    )
    # [END how_to_cloud_dataproc_trigger_workflow_template]

    # [START how_to_cloud_dataproc_instantiate_inline_workflow_template]
    instantiate_inline_workflow_template = DataprocInstantiateInlineWorkflowTemplateOperator(
        task_id='instantiate_inline_workflow_template', template=WORKFLOW_TEMPLATE, region=REGION
    )
    # [END how_to_cloud_dataproc_instantiate_inline_workflow_template]

    pig_task = DataprocSubmitJobOperator(
        task_id="pig_task", job=PIG_JOB, region=REGION, project_id=PROJECT_ID
    )
    spark_sql_task = DataprocSubmitJobOperator(
        task_id="spark_sql_task", job=SPARK_SQL_JOB, region=REGION, project_id=PROJECT_ID
    )

    spark_task = DataprocSubmitJobOperator(
        task_id="spark_task", job=SPARK_JOB, region=REGION, project_id=PROJECT_ID
    )

    # [START cloud_dataproc_async_submit_sensor]
    spark_task_async = DataprocSubmitJobOperator(
        task_id="spark_task_async", job=SPARK_JOB, region=REGION, project_id=PROJECT_ID, asynchronous=True
    )

    spark_task_async_sensor = DataprocJobSensor(
        task_id='spark_task_async_sensor_task',
        region=REGION,
        project_id=PROJECT_ID,
        dataproc_job_id=spark_task_async.output,
        poke_interval=10,
    )
    # [END cloud_dataproc_async_submit_sensor]

    # [START how_to_cloud_dataproc_submit_job_to_cluster_operator]
    pyspark_task = DataprocSubmitJobOperator(
        task_id="pyspark_task", job=PYSPARK_JOB, region=REGION, project_id=PROJECT_ID
    )
    # [END how_to_cloud_dataproc_submit_job_to_cluster_operator]

    sparkr_task = DataprocSubmitJobOperator(
        task_id="sparkr_task", job=SPARKR_JOB, region=REGION, project_id=PROJECT_ID
    )

    hive_task = DataprocSubmitJobOperator(
        task_id="hive_task", job=HIVE_JOB, region=REGION, project_id=PROJECT_ID
    )

    hadoop_task = DataprocSubmitJobOperator(
        task_id="hadoop_task", job=HADOOP_JOB, region=REGION, project_id=PROJECT_ID
    )

    # [START how_to_cloud_dataproc_delete_cluster_operator]
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster", project_id=PROJECT_ID, cluster_name=CLUSTER_NAME, region=REGION
    )
    # [END how_to_cloud_dataproc_delete_cluster_operator]

    create_cluster >> scale_cluster
    scale_cluster >> create_workflow_template >> trigger_workflow >> delete_cluster
    scale_cluster >> hive_task >> delete_cluster
    scale_cluster >> pig_task >> delete_cluster
    scale_cluster >> spark_sql_task >> delete_cluster
    scale_cluster >> spark_task >> delete_cluster
    scale_cluster >> spark_task_async
    spark_task_async_sensor >> delete_cluster
    scale_cluster >> pyspark_task >> delete_cluster
    scale_cluster >> sparkr_task >> delete_cluster
    scale_cluster >> hadoop_task >> delete_cluster

    # Task dependency created via `XComArgs`:
    #   spark_task_async >> spark_task_async_sensor

with models.DAG(
    "example_gcp_batch_dataproc",
    schedule_interval='@once',
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag_batch:
    # [START how_to_cloud_dataproc_create_batch_operator]
    create_batch = DataprocCreateBatchOperator(
        task_id="create_batch",
        project_id=PROJECT_ID,
        region=REGION,
        batch=BATCH_CONFIG,
        batch_id=BATCH_ID,
    )
    # [END how_to_cloud_dataproc_create_batch_operator]

    # [START how_to_cloud_dataproc_create_cluster_for_persistent_history_server]
    create_cluster_for_phs = DataprocCreateClusterOperator(
        task_id="create_cluster_for_phs",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_GENERATOR_CONFIG_FOR_PHS,
        region=REGION,
        cluster_name=CLUSTER_NAME_FOR_PHS,
    )
    # [END how_to_cloud_dataproc_create_cluster_for_persistent_history_server]

    # [START how_to_cloud_dataproc_create_batch_operator_with_persistent_history_server]
    create_batch_with_phs = DataprocCreateBatchOperator(
        task_id="create_batch_with_phs",
        project_id=PROJECT_ID,
        region=REGION,
        batch=BATCH_CONFIG_WITH_PHS,
        batch_id=BATCH_ID,
    )
    # [END how_to_cloud_dataproc_create_batch_operator_with_persistent_history_server]

    # [START how_to_cloud_dataproc_get_batch_operator]
    get_batch = DataprocGetBatchOperator(
        task_id="get_batch", project_id=PROJECT_ID, region=REGION, batch_id=BATCH_ID
    )
    # [END how_to_cloud_dataproc_get_batch_operator]

    # [START how_to_cloud_dataproc_list_batches_operator]
    list_batches = DataprocListBatchesOperator(
        task_id="list_batches",
        project_id=PROJECT_ID,
        region=REGION,
    )
    # [END how_to_cloud_dataproc_list_batches_operator]

    # [START how_to_cloud_dataproc_delete_batch_operator]
    delete_batch = DataprocDeleteBatchOperator(
        task_id="delete_batch", project_id=PROJECT_ID, region=REGION, batch_id=BATCH_ID
    )
    # [END how_to_cloud_dataproc_delete_batch_operator]

    create_batch >> get_batch >> list_batches >> delete_batch
    create_cluster_for_phs >> create_batch_with_phs
