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

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.plexus.operators.job import PlexusJobOperator

HOME = '/home/acc'
T3_PRERUN_SCRIPT = 'cp {home}/imdb/run_scripts/mlflow.sh {home}/ && chmod +x mlflow.sh'.format(home=HOME)
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "test"

with DAG(
    DAG_ID,
    default_args={'owner': 'core scientific', 'retries': 1},
    description='testing plexus operator',
    start_date=datetime(2021, 1, 1),
    schedule='@once',
    catchup=False,
) as dag:
    # [START plexus_job_op]
    t1 = PlexusJobOperator(
        task_id='test',
        job_params={
            'name': 'test',
            'app': 'MLFlow Pipeline 01',
            'queue': 'DGX-2 (gpu:Tesla V100-SXM3-32GB)',
            'num_nodes': 1,
            'num_cores': 1,
            'prerun_script': T3_PRERUN_SCRIPT,
        },
    )
    # [END plexus_job_op]


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
