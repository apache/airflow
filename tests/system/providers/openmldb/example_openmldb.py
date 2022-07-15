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
Example use of OpenMLDB related operators.
"""
import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.openmldb.operators.openmldb import (
    Mode,
    OpenMLDBLoadDataOperator,
    OpenMLDBSelectIntoOperator,
)

PATH_TO_DATA_FILE = os.environ.get('OPENMLDB_PATH_TO_DATA_FILE', '/tmp/example-text.txt')
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_openmldb"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    default_args={'openmldb_conn_id': 'openmldb_conn_id'},
    max_active_runs=1,
    tags=['example'],
    catchup=False,
) as dag:
    database = "example_db"
    table = "example_table"

    # [START load_data_and_extract_feature_offline]
    load_data = OpenMLDBLoadDataOperator(
        task_id='load-data',
        db=database,
        mode=Mode.OFFSYNC,
        table=table,
        file=PATH_TO_DATA_FILE,
        options="mode='overwrite'",
    )

    feature_extract = OpenMLDBSelectIntoOperator(
        task_id='feature-extract',
        db=database,
        mode=Mode.OFFSYNC,
        sql=f"select * from {table}",
        file="/tmp/feature_data",
        options="mode='overwrite'",
    )
    # [END load_data_and_extract_feature_offline]

    load_data >> feature_extract

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
