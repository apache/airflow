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

import pendulum

from airflow.assets import Asset
from airflow.decorators.assets import asset
from airflow.models.dag import DAG
from airflow.providers.standard.operators.bash import BashOperator

asset1 = Asset(uri="s3://bucket/object")


@asset(uri="s3://bucket/asset_decorator", schedule=None)
def asset_decorator(asset1, asset2):
    pass


with DAG(
    dag_id="asset_consumes_1",
    catchup=False,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=[Asset(name="asset_decorator", uri="s3://bucket/asset_decorator")],
    tags=["consumes", "asset-scheduled"],
) as dag3:
    # [END dag_dep]
    BashOperator(
        outlets=[Asset("s3://consuming_1_task/asset_other.txt")],
        task_id="consuming_1",
        bash_command="sleep 5",
    )
