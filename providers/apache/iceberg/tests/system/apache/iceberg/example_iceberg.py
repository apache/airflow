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

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.iceberg.hooks.iceberg import IcebergHook
from airflow.providers.standard.operators.bash import BashOperator

bash_command = f"""
echo "Our token: {IcebergHook().get_token_macro()}"
echo "Also as an environment variable:"
env | grep TOKEN
"""

with DAG(
    "iceberg_example",
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email": ["airflow@airflow.com"],
        "email_on_failure": False,
        "email_on_retry": False,
    },
    start_date=datetime(2021, 1, 1),
    schedule=timedelta(1),
    catchup=False,
) as dag:
    # This also works for the SparkSubmit operator
    BashOperator(
        task_id="with_iceberg_environment_variable",
        bash_command=bash_command,
        env={"TOKEN": IcebergHook().get_token_macro()},
    )


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
