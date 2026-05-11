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
Example DAG demonstrating cross-team asset triggering with ``allow_producer_teams``.

When Multi-Team mode is enabled (``[core] multi_team = True``), asset events are filtered by team
membership. By default, a consuming DAG only receives events from DAGs within the same team.

Usage:
    - ``team_analytics_producer`` (belonging to ``team_analytics``) produces events on ``shared_data``.
    - ``team_ml_consumer`` (belonging to ``team_ml``) consumes ``shared_data``.
    - Because ``shared_data`` has ``allow_producer_teams=["team_analytics"]``, events from ``team_analytics``
      are accepted by ``team_ml_consumer``.
    - Without ``allow_producer_teams``, the cross-team event would be blocked.
"""

from __future__ import annotations

import pendulum

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Asset

# [START asset_allow_producer_teams]
# Define an asset that accepts events from team_analytics in addition to the consumer's own team.
shared_data = Asset(
    name="shared_data",
    uri="s3://data-lake/shared/output.csv",
    allow_producer_teams=["team_analytics"],
)

# Producer DAG — belongs to team_analytics (via its DAG bundle configuration).
# When this DAG's task completes, it emits an asset event for shared_data.
with DAG(
    dag_id="team_analytics_producer",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["team_analytics", "produces", "asset-scheduled", "allow-teams"],
) as producer_dag:
    BashOperator(
        task_id="produce_shared_data",
        outlets=[shared_data],
        bash_command="echo 'Producing shared data for cross-team consumption'",
    )

# Consumer DAG — belongs to team_ml (via its DAG bundle configuration).
# This DAG is triggered when shared_data is updated. Because shared_data has
# allow_producer_teams=["team_analytics"], events from team_analytics are accepted.
with DAG(
    dag_id="team_ml_consumer",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=[shared_data],
    catchup=False,
    tags=["team_ml", "consumes", "asset-scheduled", "allow-teams"],
) as consumer_dag:
    BashOperator(
        task_id="consume_shared_data",
        bash_command="echo 'Consuming shared data from team_analytics'",
    )
# [END asset_allow_producer_teams]
