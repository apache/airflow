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

import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.on_demand import OnDemandSectionOperator
from airflow.sdk import DAG

with DAG(
    dag_id="example_on_demand_deployment",
    schedule=None,
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "ci-cd"],
) as dag:
    # [START howto_operator_on_demand_deployment]
    build_release = BashOperator(
        task_id="build_release",
        bash_command="echo 'Building and packaging the release artifact'",
    )
    run_tests = BashOperator(
        task_id="run_tests",
        bash_command="echo 'Running unit and integration tests'",
    )
    publish_artifact = BashOperator(
        task_id="publish_artifact",
        bash_command="echo 'Publishing the immutable release artifact'",
    )
    deploy_to_staging = BashOperator(
        task_id="deploy_to_staging",
        bash_command="echo 'Deploying the release to staging'",
    )
    verify_staging = BashOperator(
        task_id="verify_staging",
        bash_command="echo 'Verifying the staging deployment'",
    )

    production_release = OnDemandSectionOperator(
        task_id="production_release",
        label="Deploy this release to production",
    )

    deploy_to_production = BashOperator(
        task_id="deploy_to_production",
        bash_command="echo 'Deploying the validated release to production'",
    )
    run_production_smoke_tests = BashOperator(
        task_id="run_production_smoke_tests",
        bash_command="echo 'Running production smoke tests'",
    )

    (
        build_release
        >> run_tests
        >> publish_artifact
        >> deploy_to_staging
        >> verify_staging
        >> production_release
        >> deploy_to_production
        >> run_production_smoke_tests
    )
    # [END howto_operator_on_demand_deployment]
