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

from datetime import datetime

from airflow.providers.amazon.aws.operators.athena_spark import AthenaSparkOperator
from airflow.providers.amazon.aws.sensors.athena_spark import AthenaSparkSensor

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import DAG, chain
else:
    # Airflow 2 path
    from airflow.models.dag import DAG, chain  # type: ignore[attr-defined,no-redef,assignment]

DAG_ID = "example_athena_spark"


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    # [START howto_operator_athena_spark]
    run_spark_calculation = AthenaSparkOperator(
        task_id="run_spark_calculation",
        session_id="my-athena-spark-session-id",
        code_block="print('hello from athena spark')",
        poll_interval=30,
        max_attempts=120,
    )
    # [END howto_operator_athena_spark]

    # [START howto_sensor_athena_spark]
    await_spark_calculation = AthenaSparkSensor(
        task_id="await_spark_calculation",
        calculation_execution_id=run_spark_calculation.output,
        poke_interval=30,
        timeout=3600,
    )
    # [END howto_sensor_athena_spark]

    chain(run_spark_calculation, await_spark_calculation)

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
