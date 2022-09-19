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
from os import getenv

from airflow import DAG
from airflow.providers.amazon.aws.transfers.redshift_to_s3 import RedshiftToS3Operator
from tests.system.providers.amazon.aws.utils import set_env_id

ENV_ID = set_env_id()
DAG_ID = 'example_redshift_to_s3'
S3_BUCKET_NAME = getenv("S3_BUCKET_NAME", "s3_bucket_name")
S3_KEY = getenv("S3_KEY", "s3_key")
REDSHIFT_TABLE = getenv("REDSHIFT_TABLE", "redshift_table")

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    tags=['example'],
) as dag:
    # [START howto_transfer_redshift_to_s3]
    task_transfer_redshift_to_s3 = RedshiftToS3Operator(
        task_id='transfer_redshift_to_s3',
        s3_bucket=S3_BUCKET_NAME,
        s3_key=S3_KEY,
        schema='PUBLIC',
        table=REDSHIFT_TABLE,
    )
    # [END howto_transfer_redshift_to_s3]


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
