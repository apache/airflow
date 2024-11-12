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

import logging
from datetime import datetime

from opentelemetry import trace

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.traces import otel_tracer
from airflow.traces.otel_tracer import CTX_PROP_SUFFIX
from airflow.traces.tracer import Trace

logger = logging.getLogger("airflow.test_dag")

args = {
    "owner": "airflow",
    "start_date": datetime(2024, 9, 1),
    "retries": 0,
}

# DAG definition.
with DAG(
    "otel_test_dag",
    default_args=args,
    schedule=None,
    catchup=False,
) as dag:
    # Tasks.
    def task1_func(**dag_context):
        logger.info("Starting Task_1.")

        ti = dag_context["ti"]
        context_carrier = ti.context_carrier

        otel_task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)
        tracer_provider = otel_task_tracer.get_otel_tracer_provider()

        if context_carrier is not None:
            logger.info("Found ti.context_carrier: %s.", context_carrier)
            logger.info("Extracting the span context from the context_carrier.")
            parent_context = Trace.extract(context_carrier)
            with otel_task_tracer.start_child_span(
                span_name=f"{ti.task_id}_sub_span1{CTX_PROP_SUFFIX}",
                parent_context=parent_context,
                component=f"dag{CTX_PROP_SUFFIX}",
            ) as s1:
                s1.set_attribute("attr1", "val1")
                logger.info("From task sub_span1.")

                with otel_task_tracer.start_child_span(f"{ti.task_id}_sub_span2{CTX_PROP_SUFFIX}") as s2:
                    s2.set_attribute("attr2", "val2")
                    logger.info("From task sub_span2.")

                    tracer = trace.get_tracer("trace_test.tracer", tracer_provider=tracer_provider)
                    with tracer.start_as_current_span(name=f"{ti.task_id}_sub_span3{CTX_PROP_SUFFIX}") as s3:
                        s3.set_attribute("attr3", "val3")
                        logger.info("From task sub_span3.")

            with otel_task_tracer.start_child_span(
                span_name=f"{ti.task_id}_sub_span4{CTX_PROP_SUFFIX}",
                parent_context=parent_context,
                component=f"dag{CTX_PROP_SUFFIX}",
            ) as s4:
                s4.set_attribute("attr4", "val4")
                logger.info("From task sub_span4.")

        logger.info("Task_1 finished.")

    def task2_func():
        logger.info("Starting Task_2.")
        for i in range(3):
            logger.info("Task_2, iteration '%d'.", i)
        logger.info("Task_2 finished.")

    # Task operators.
    t1 = PythonOperator(
        task_id="task_1",
        python_callable=task1_func,
    )

    t2 = PythonOperator(
        task_id="task_2",
        python_callable=task2_func,
    )

    # Dependencies.
    t1 >> t2
