#
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
Example use of Teradata Compute Cluster Provision Operator
"""

from __future__ import annotations

import datetime
import os

import pytest

from airflow import DAG
from airflow.models import Param

try:
    from airflow.providers.teradata.operators.teradata_compute_cluster import (
        TeradataComputeClusterDecommissionOperator,
        TeradataComputeClusterProvisionOperator,
        TeradataComputeClusterResumeOperator,
        TeradataComputeClusterSuspendOperator,
    )
except ImportError:
    pytest.skip("TERADATA provider not available", allow_module_level=True)

# [START teradata_vantage_lake_compute_cluster_howto_guide]


ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_teradata_computer_cluster"

# The operators below build Teradata DDL by interpolating these names into SQL text.
# Object names cannot be passed as bind parameters, so anything reaching them must be
# constrained where it is declared. Params are settable by whoever triggers the Dag --
# a lower-trust role than the Dag author -- so every Param here is either restricted to
# a closed set of values (`enum`) or to an identifier shape (`pattern`).
#
# `teradata_conn_id` and `compute_attribute` are deliberately NOT Params: the first
# selects which credentials the task runs under, and the second is a free-form option
# string with no safe identifier shape. Neither belongs under trigger-time control.
TERADATA_CONN_ID = "teradata_lake"
COMPUTE_ATTRIBUTE = "MIN_COMPUTE_COUNT(1) MAX_COMPUTE_COUNT(5) INITIALLY_SUSPENDED('FALSE')"

# Unquoted Teradata object name: a letter followed by letters, digits or underscores.
OBJECT_NAME_PATTERN = "^[A-Za-z][A-Za-z0-9_]{0,127}$"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2020, 2, 2),
    schedule="@once",
    catchup=False,
    default_args={"teradata_conn_id": TERADATA_CONN_ID},
    render_template_as_native_obj=True,
    params={
        "compute_group_name": Param(
            "compute_group_test",
            type="string",
            pattern=OBJECT_NAME_PATTERN,
            title="Compute cluster group Name:",
            description="Enter compute cluster group name.",
        ),
        "compute_profile_name": Param(
            "compute_profile_test",
            type="string",
            pattern=OBJECT_NAME_PATTERN,
            title="Compute cluster profile Name:",
            description="Enter compute cluster profile name.",
        ),
        "query_strategy": Param(
            "STANDARD",
            type="string",
            enum=["STANDARD", "ANALYTIC"],
            title="Compute cluster instance type:",
            description="Enter compute cluster instance type. Valid values are STANDARD, ANALYTIC",
        ),
        "compute_map": Param(
            "TD_COMPUTE_XSMALL",
            type="string",
            pattern=OBJECT_NAME_PATTERN,
            title="Compute Map Name:",
            description="Enter compute cluster compute map name.",
        ),
        "delete_compute_group": Param(
            False,
            type="boolean",
            title="Delete the compute group on decommission:",
            description="Whether decommissioning also deletes the compute group.",
        ),
        "timeout": Param(
            20,
            type="integer",
            minimum=1,
            title="Timeout:",
            description="Time elapsed before the task times out and fails. Timeout is in minutes.",
        ),
    },
) as dag:
    # [START teradata_vantage_lake_compute_cluster_provision_howto_guide]
    compute_cluster_provision_operation = TeradataComputeClusterProvisionOperator(
        task_id="compute_cluster_provision_operation",
        compute_profile_name="{{ params.compute_profile_name }}",
        compute_group_name="{{ params.compute_group_name }}",
        teradata_conn_id=TERADATA_CONN_ID,
        timeout="{{ params.timeout }}",
        query_strategy="{{ params.query_strategy }}",
        compute_map="{{ params.compute_map }}",
        compute_attribute=COMPUTE_ATTRIBUTE,
    )
    # [END teradata_vantage_lake_compute_cluster_provision_howto_guide]
    # [START teradata_vantage_lake_compute_cluster_suspend_howto_guide]
    compute_cluster_suspend_operation = TeradataComputeClusterSuspendOperator(
        task_id="compute_cluster_suspend_operation",
        compute_profile_name="{{ params.compute_profile_name }}",
        compute_group_name="{{ params.compute_group_name }}",
        teradata_conn_id=TERADATA_CONN_ID,
        timeout="{{ params.timeout }}",
    )
    # [END teradata_vantage_lake_compute_cluster_suspend_howto_guide]
    # [START teradata_vantage_lake_compute_cluster_resume_howto_guide]
    compute_cluster_resume_operation = TeradataComputeClusterResumeOperator(
        task_id="compute_cluster_resume_operation",
        compute_profile_name="{{ params.compute_profile_name }}",
        compute_group_name="{{ params.compute_group_name }}",
        teradata_conn_id=TERADATA_CONN_ID,
        timeout="{{ params.timeout }}",
    )
    # [END teradata_vantage_lake_compute_cluster_resume_howto_guide]
    # [START teradata_vantage_lake_compute_cluster_decommission_howto_guide]
    compute_cluster_decommission_operation = TeradataComputeClusterDecommissionOperator(
        task_id="compute_cluster_decommission_operation",
        compute_profile_name="{{ params.compute_profile_name }}",
        compute_group_name="{{ params.compute_group_name }}",
        delete_compute_group="{{ params.delete_compute_group }}",
        teradata_conn_id=TERADATA_CONN_ID,
        timeout="{{ params.timeout }}",
    )
    # [END teradata_vantage_lake_compute_cluster_decommission_howto_guide]
    (
        compute_cluster_provision_operation
        >> compute_cluster_suspend_operation
        >> compute_cluster_resume_operation
        >> compute_cluster_decommission_operation
    )

    # [END teradata_vantage_lake_compute_cluster_howto_guide]

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
