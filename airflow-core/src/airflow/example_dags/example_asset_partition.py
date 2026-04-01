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
    AllowedKeyMapper,
    Asset,
    CronPartitionTimetable,
    IdentityMapper,
    PartitionedAssetTimetable,
    ProductMapper,
    StartOfDayMapper,
    StartOfHourMapper,
    StartOfYearMapper,
    asset,
    task,
)

team_a_player_stats = Asset(uri="file://incoming/player-stats/team_a.csv", name="team_a_player_stats")
combined_player_stats = Asset(uri="file://curated/player-stats/combined.csv", name="combined_player_stats")


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
def team_b_player_stats():
    """Produce hourly partitioned stats for Team B."""
    pass


@asset(
    uri="file://incoming/player-stats/team_c.csv",
    schedule=CronPartitionTimetable("30 * * * *", timezone="UTC"),
    tags=["player-stats", "ingestion"],
)
def team_c_player_stats():
    """Produce hourly partitioned stats for Team C."""
    pass


with DAG(
    dag_id="clean_and_combine_player_stats",
    schedule=PartitionedAssetTimetable(
        assets=team_a_player_stats & team_b_player_stats & team_c_player_stats,
        default_partition_mapper=StartOfHourMapper(),
    ),
    catchup=False,
    tags=["player-stats", "cleanup"],
):
    """
    Combine hourly partitions from Team A, B and C into a single curated dataset.

    This Dag demonstrates multi-asset partition alignment using StartOfHourMapper.
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
    # If we want to other temporal mapper (e.g., StartOfHourMapper) here,
    # make sure the input_format is changed since the partition_key is now in "%Y-%m-%dT%H" format
    # instead of a valid timestamp
    schedule=PartitionedAssetTimetable(assets=combined_player_stats),
    tags=["player-stats", "odds"],
)
def compute_player_odds():
    """
    Compute player odds from the combined hourly statistics.

    This asset is partition-aware and triggered by the combined stats asset.
    """
    pass


with DAG(
    dag_id="player_odds_quality_check_wont_ever_to_trigger",
    schedule=PartitionedAssetTimetable(
        assets=(combined_player_stats & team_a_player_stats & Asset.ref(name="team_b_player_stats")),
        partition_mapper_config={
            combined_player_stats: StartOfYearMapper(),  # incompatible on purpose
            team_a_player_stats: StartOfHourMapper(),
            Asset.ref(name="team_b_player_stats"): StartOfHourMapper(),
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
    def check_partition_alignment():
        pass

    check_partition_alignment()


regional_sales = Asset(uri="file://incoming/sales/regional.csv", name="regional_sales")

with DAG(
    dag_id="ingest_regional_sales",
    schedule=CronPartitionTimetable("0 * * * *", timezone="UTC"),
    tags=["sales", "ingestion"],
):
    """Produce hourly regional sales data with composite partition keys."""

    @task(outlets=[regional_sales])
    def ingest_sales():
        """Ingest regional sales data partitioned by region and time."""
        pass

    ingest_sales()


with DAG(
    dag_id="aggregate_regional_sales",
    schedule=PartitionedAssetTimetable(
        assets=regional_sales,
        default_partition_mapper=ProductMapper(IdentityMapper(), StartOfDayMapper()),
    ),
    catchup=False,
    tags=["sales", "aggregation"],
):
    """
    Aggregate regional sales using ProductMapper.

    The ProductMapper splits the composite key "region|timestamp" and applies
    IdentityMapper to the region segment and StartOfDayMapper to the timestamp segment,
    aligning hourly partitions to daily granularity per region.
    """

    @task
    def aggregate_sales(dag_run=None):
        """Aggregate sales data for the matched region-day partition."""
        if TYPE_CHECKING:
            assert dag_run
        print(dag_run.partition_key)

    aggregate_sales()


region_raw_stats = Asset(uri="file://incoming/player-stats/by-region.csv", name="region_raw_stats")


with DAG(
    dag_id="ingest_region_stats",
    schedule=None,
    tags=["player-stats", "regional"],
):
    """
    Ingest player statistics per region.

    Externally triggered with partition_key set to a region code (``us``, ``eu``, ``apac``).
    """

    @task(outlets=[region_raw_stats])
    def ingest_region():
        """Materialize player statistics for a single region partition."""
        pass

    ingest_region()


@asset(
    uri="file://analytics/player-stats/regional-breakdown.csv",
    schedule=PartitionedAssetTimetable(
        assets=region_raw_stats,
        default_partition_mapper=AllowedKeyMapper(["us", "eu", "apac"]),
    ),
    tags=["player-stats", "regional"],
)
def regional_stats_breakdown():
    """
    Aggregate regional player statistics.

    This asset demonstrates AllowedKeyMapper, which validates that upstream partition
    keys belong to a fixed set of allowed values (``us``, ``eu``, ``apac``) rather than time-based partitions.
    """
    pass
