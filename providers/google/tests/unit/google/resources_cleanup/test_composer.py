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

from unittest.mock import AsyncMock, patch

import pytest
from airflow_google_provider_resource_cleanup.handlers import composer


@pytest.mark.anyio
async def test_delete_composer_environment_does_not_check_age():
    resource = {
        "name": (
            "//composer.googleapis.com/projects/test-project/locations/us-central1/"
            "environments/test-environment"
        ),
        "location": "us-central1",
        "createTime": "2099-01-01T00:00:00Z",
    }

    with patch.object(composer, "run_command_async", AsyncMock()) as mock_run_command:
        await composer._delete_composer_environment(resource, "[1/1] ")

    mock_run_command.assert_awaited_once_with(
        "gcloud composer environments delete projects/test-project/locations/us-central1/"
        "environments/test-environment --location=us-central1 --quiet",
        "[1/1] ",
    )
