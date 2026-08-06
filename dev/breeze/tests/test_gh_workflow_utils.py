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

from unittest import mock

from airflow_breeze.utils.gh_workflow_utils import trigger_workflow_and_monitor
from airflow_breeze.utils.shared_options import set_dry_run


@mock.patch("airflow_breeze.utils.gh_workflow_utils.monitor_workflow_run")
@mock.patch("airflow_breeze.utils.gh_workflow_utils.make_sure_gh_is_installed")
def test_trigger_workflow_and_monitor_stops_after_the_dispatch_in_dry_run(_, mock_monitor):
    """A dry run dispatches nothing, so the empty `gh run list` must not read as a missing run."""
    set_dry_run(True)
    try:
        trigger_workflow_and_monitor(
            workflow_name="release-constraints.yml",
            repo="apache/airflow",
            version="3.2.0rc1",
            ref="v3-2-stable",
        )
    finally:
        set_dry_run(False)

    mock_monitor.assert_not_called()
