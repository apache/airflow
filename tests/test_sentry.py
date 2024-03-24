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

from unittest.mock import MagicMock, create_autospec, patch

from sentry_sdk.scope import Scope

from airflow.models.taskinstance import TaskInstance


@patch("sentry_sdk.configure_scope")
def test_configured_sentry_add_tagging(mock_configure_scope):
    mock_scope = create_autospec(Scope)
    mock_configure_scope.return_value = mock_scope

    from airflow.sentry.configured import ConfiguredSentry

    sentry = ConfiguredSentry()

    dummy_tags = ConfiguredSentry.SCOPE_DAG_RUN_TAGS | ConfiguredSentry.SCOPE_TASK_INSTANCE_TAGS

    # It should not raise error with both "dag_run" and "task" attributes, available.
    task_instance_1 = create_autospec(TaskInstance)
    task_instance_1.dag_run = MagicMock()
    task_instance_1.task = "task_1"
    for tag in dummy_tags:
        setattr(task_instance_1.dag_run, tag, "dummy")
    sentry.add_tagging(task_instance_1)

    # Verify tags
    for tag in dummy_tags:
        mock_scope.set_tag.assert_called_with(tag, "dummy")
    mock_scope.set_tag.assert_called_with("operator", "str")

    # Reset the mock
    mock_scope.reset_mock()

    # It should not raise error if "task" attribute is not set.
    task_instance_2 = create_autospec(TaskInstance)
    task_instance_2.dag_run = MagicMock()
    for tag in dummy_tags:
        setattr(task_instance_2.dag_run, tag, "dummy")
    sentry.add_tagging(task_instance_2)

    # Verify tags
    for tag in dummy_tags:
        mock_scope.set_tag.assert_called_with(tag, "dummy")
    # Also verify the "operator" tag, which is related to the "task attribute.
    mock_scope.set_tag.assert_called_with("operator", "str")
