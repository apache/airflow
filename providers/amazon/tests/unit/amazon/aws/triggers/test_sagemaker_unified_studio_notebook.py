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

from airflow.providers.amazon.aws.hooks.sagemaker_unified_studio_notebook import (
    SageMakerUnifiedStudioNotebookHook,
)
from airflow.providers.amazon.aws.triggers.base import AwsBaseWaiterTrigger
from airflow.providers.amazon.aws.triggers.sagemaker_unified_studio_notebook import (
    TWELVE_HOURS_IN_MINUTES,
    SageMakerUnifiedStudioNotebookTrigger,
)

DOMAIN_ID = "dzd_example"
PROJECT_ID = "proj_example"
NOTEBOOK_RUN_ID = "run_456"
MODULE_PATH = "airflow.providers.amazon.aws.triggers.sagemaker_unified_studio_notebook"


class TestSageMakerUnifiedStudioNotebookTrigger:
    def _create_trigger(self, **kwargs):
        defaults = {
            "notebook_run_id": NOTEBOOK_RUN_ID,
            "domain_id": DOMAIN_ID,
            "project_id": PROJECT_ID,
            "waiter_delay": 1,
        }
        defaults.update(kwargs)
        return SageMakerUnifiedStudioNotebookTrigger(**defaults)

    def test_inherits_aws_base_waiter_trigger(self):
        trigger = self._create_trigger()
        assert isinstance(trigger, AwsBaseWaiterTrigger)

    # --- serialization ---

    def test_serialize(self):
        trigger = self._create_trigger(waiter_delay=10, timeout_configuration={"run_timeout_in_minutes": 60})
        classpath, kwargs = trigger.serialize()
        assert classpath == f"{MODULE_PATH}.SageMakerUnifiedStudioNotebookTrigger"
        assert kwargs["notebook_run_id"] == NOTEBOOK_RUN_ID
        assert kwargs["domain_id"] == DOMAIN_ID
        assert kwargs["project_id"] == PROJECT_ID
        assert kwargs["waiter_delay"] == 10
        assert kwargs["timeout_configuration"] == {"run_timeout_in_minutes": 60}
        assert kwargs["waiter_max_attempts"] == int(60 * 60 / 10)

    # --- timeout calculation ---

    def test_default_timeout(self):
        trigger = self._create_trigger(waiter_delay=10)
        assert trigger.attempts == int(TWELVE_HOURS_IN_MINUTES * 60 / 10)

    def test_custom_timeout_configuration(self):
        trigger = self._create_trigger(waiter_delay=10, timeout_configuration={"run_timeout_in_minutes": 60})
        assert trigger.attempts == int(60 * 60 / 10)

    def test_empty_timeout_configuration_falls_back_to_default(self):
        trigger = self._create_trigger(waiter_delay=10, timeout_configuration={})
        assert trigger.attempts == int(TWELVE_HOURS_IN_MINUTES * 60 / 10)

    # --- waiter configuration ---

    def test_waiter_name(self):
        trigger = self._create_trigger()
        assert trigger.waiter_name == "notebook_run_complete"

    def test_waiter_args(self):
        trigger = self._create_trigger()
        assert trigger.waiter_args == {
            "domain_id": DOMAIN_ID,
            "notebook_run_id": NOTEBOOK_RUN_ID,
        }

    def test_return_value(self):
        trigger = self._create_trigger()
        assert trigger.return_key == "notebook_run_id"
        assert trigger.return_value == NOTEBOOK_RUN_ID

    # --- hook ---

    def test_hook_returns_notebook_hook(self):
        trigger = self._create_trigger()
        hook = trigger.hook()
        assert isinstance(hook, SageMakerUnifiedStudioNotebookHook)
        assert hook.client_type == "datazone"
