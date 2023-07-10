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
"""Example of a DAG that uses :py:class:`SparkKubernetesOperator` with sensor.
The operator creates the spark job on the kubernetes and the sensor watches the state of the job.
"""
from __future__ import annotations

import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor

with DAG("example_spark_k8s_with_sensor", start_date=datetime.datetime(2023, 1, 1)) as dag:
    t1 = SparkKubernetesOperator(
        task_id="spark_pi_submit",
        namespace="default",
        application_file="spark-pi.yaml",
        do_xcom_push=True,
    )

    t2 = SparkKubernetesSensor(
        task_id="spark_pi_sensor",
        namespace="default",
        application_name="{{ task_instance.xcom_pull(task_ids='spark_pi_submit')['metadata']['name'] }}",
    )

    t1 >> t2
