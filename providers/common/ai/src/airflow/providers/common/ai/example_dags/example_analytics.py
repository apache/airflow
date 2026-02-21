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

from airflow.providers.common.ai.operators.analytics import AnalyticsOperator
from airflow.providers.common.ai.utils.config import DataSourceConfig
from airflow.sdk import DAG

datasource_config_s3 = DataSourceConfig(
    conn_id="aws_default", table_name="users_data", uri="s3://bucket/path/", format="parquet"
)

datasource_config_local = DataSourceConfig(
    conn_id="", table_name="users_data", uri="file:///path/to/", format="parquet"
)

# Please replace uri with appropriate value

with DAG(
    dag_id="example_analytics",
    schedule=datetime.timedelta(hours=4),
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=["analytics", "commonai"],
) as dag:
    # [START howto_analytics_operator_with_s3]
    analytics_with_s3 = AnalyticsOperator(
        task_id="analytics_with_s3",
        datasource_configs=[datasource_config_s3],
        queries=["SELECT * FROM users_data", "SELECT count(*) FROM users_data"],
    )

    # [END howto_analytics_operator_with_s3]

    # [START howto_analytics_operator_with_local]
    analytics_with_local = AnalyticsOperator(
        task_id="analytics_with_local",
        datasource_configs=[datasource_config_local],
        queries=["SELECT * FROM users_data", "SELECT count(*) FROM users_data"],
    )
    analytics_with_s3 >> analytics_with_local
    # [END howto_analytics_operator_with_local]
