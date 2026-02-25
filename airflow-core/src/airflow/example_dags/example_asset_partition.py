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

from typing import TYPE_CHECKING

from airflow.sdk import (
    DAG,
    Asset,
    CronPartitionTimetable,
    HourlyMapper,
    PartitionedAssetTimetable,
    YearlyMapper,
    asset,
    task,
)

team_a_player_stats = Asset(uri="file://incoming/player-stats/team_a.csv")
team_b_player_stats = Asset(uri="file://incoming/player-stats/team_b.csv")
combined_player_stats = Asset(uri="file://curated/player-stats/combined.csv")


with DAG(
    dag_id="ingest_team_a_player_stats",
    schedule=CronPartitionTimetable("0 * * * *", timezone="UTC"),
    tags=["player-stats", "ingestion"],
):
    """Produce hourly partitioned stats for Team A."""

    @task(outlets=[team_a_player_stats])
    def ingest_team_a_stats():
        """Materialize Team A player statistics for the current hourly partition."""
        pass

    ingest_team_a_stats()


@asset(
    uri="file://incoming/player-stats/team_b.csv",
    schedule=CronPartitionTimetable("15 * * * *", timezone="UTC"),
    tags=["player-stats", "ingestion"],
)
def ingest_team_b_player_stats():
    """Produce hourly partitioned stats for Team B."""
    pass


with DAG(
    dag_id="clean_and_combine_player_stats",
    schedule=PartitionedAssetTimetable(
        assets=team_a_player_stats & team_b_player_stats,
        default_partition_mapper=HourlyMapper(),
    ),
    catchup=False,
    tags=["player-stats", "cleanup"],
):
    """
    Combine hourly partitions from Team A and Team B into a single curated dataset.

    This Dag demonstrates multi-asset partition alignment using HourlyMapper.
    """

    @task(outlets=[combined_player_stats])
    def combine_player_stats(dag_run=None):
        """Merge the aligned hourly partitions into a combined dataset."""
        if TYPE_CHECKING:
            assert dag_run
        print(dag_run.partition_key)

    combine_player_stats()


@asset(
    uri="file://analytics/player-stats/computed-player-odds.csv",
    # Fallback to IdentityMapper if no partition_mapper is specified.
    # If we want to other temporal mapper (e.g., HourlyMapper) here,
    # make sure the input_format is changed since the partition_key is now in "%Y-%m-%dT%H" format
    # instead of a valid timestamp
    schedule=PartitionedAssetTimetable(assets=combined_player_stats),
    tags=["player-stats", "odds"],
)
def compute_player_odds(dag_run):
    """
    Compute player odds from the combined hourly statistics.

    This asset is partition-aware and triggered by the combined stats asset.
    """
    print(dag_run.partition_key)
    pass


with DAG(
    dag_id="player_odds_quality_check_unlikely_to_trigger",
    schedule=PartitionedAssetTimetable(
        assets=combined_player_stats & Asset.ref(name="compute_player_odds"),
        partition_mapper_config={
            combined_player_stats: YearlyMapper(),  # incompatible on purpose
            Asset.ref(name="compute_player_odds"): HourlyMapper(),
        },
    ),
    catchup=False,
    tags=["player-stats", "odds"],
):
    """
    Demonstrate a partition mapper mismatch scenario.

    The configured partition mapper transforms partition keys into formats
    that never matches ("%Y" v.s. "%Y-%m-%dT%H), so the Dag will never trigger.
    """

    @task
    def check_partition_alignment(dag_run=None):
        """Print the partition key if the DagRun is ever created."""
        if dag_run:
            print(dag_run.partition_key)

    check_partition_alignment()
