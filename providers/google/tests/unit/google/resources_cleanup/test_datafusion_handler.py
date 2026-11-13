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
from __future__ import annotations

from unittest.mock import patch

import pytest
from airflow_google_provider_resource_cleanup.handlers import datafusion


@pytest.mark.anyio
@patch.object(datafusion, "curl", autospec=True)
async def test_delete_dns_peering(mock_curl):
    resource_name = (
        "//datafusion.googleapis.com/projects/test-project/locations/us-central1/instances/"
        "test-instance/dnsPeerings/test-peering"
    )

    await datafusion.DataFusionDeleteHandler.DELETERS["datafusion.googleapis.com/DnsPeering"](
        {"name": resource_name}, "[1/1] "
    )

    mock_curl.assert_awaited_once_with(
        "https://datafusion.googleapis.com/v1/projects/test-project/locations/us-central1/instances/"
        "test-instance/dnsPeerings/test-peering",
        log_prefix="[1/1] ",
    )


@pytest.mark.anyio
@patch.object(datafusion, "run_command_async", autospec=True)
async def test_delete_instance(mock_run_command_async):
    resource_name = (
        "//datafusion.googleapis.com/projects/test-project/locations/us-central1/instances/test-instance"
    )

    await datafusion.DataFusionDeleteHandler.DELETERS["datafusion.googleapis.com/Instance"](
        {"name": resource_name}, "[1/1] "
    )

    mock_run_command_async.assert_awaited_once_with(
        "gcloud beta data-fusion instances delete test-instance "
        "--location=us-central1 --project=test-project --quiet",
        "[1/1] ",
    )


def test_deletion_order_removes_dns_peerings_before_instances():
    assert datafusion.DataFusionDeleteHandler.DELETION_ORDER == [
        "datafusion.googleapis.com/DnsPeering",
        "datafusion.googleapis.com/Instance",
    ]
