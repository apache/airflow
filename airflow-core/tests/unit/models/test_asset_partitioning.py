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

import pytest

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def clear_assets():
    from tests_common.test_utils.db import clear_db_assets

    clear_db_assets()
    yield
    clear_db_assets()


def test_by_default_partitioned_asset_event_created():
    """
    By default when a task runs, if it has a partitioned asset outlet,
    then an asset event should be created and it should have the partition
    key of the run (unless we provide for mapping from run partition key,
    and unless user does something to modify the asset event to be created)
    """


def test_user_can_override_the_asset_event_somehow():
    """
    User can change the asset event (or remove it) via some accessor in the task context.
    """


def test_when_dag_run_has_partition_key_its_accessible_in_context():
    """For now we can just access via dag run object."""


def test_partition_mapping():
    """
    The scheduling behavior.

    We need a clear interface for specifying when dag runs should be created in response to asset events.

    When a dag is listening to a partitioned asset, it's complicated

    Right now we have the queue table.

    What if we add partition key to that.

    What if we create a distinct partition queue table.

    From the partition mapping definition, we would know which target partitions would be created from
    a source asset event.

    So when firing the source asset event, we need to look at the downstream dags
        - for each listening dag:
            - evaluate the partition mapping to determine target partition
            - create a queue record
                - source asset
                - source key
                - target key
                - target dag
        - Then what does the dag scheduling evaluation process look like:
            - look in this partition mapping queue table
                - loop over all (dag_id, partition_key) combinations
                    * for each one, evaluate whether the conditions for running are met
    Evaluation of partition-driven dags and non-partition-driven dags can be done entirely separately
    """
