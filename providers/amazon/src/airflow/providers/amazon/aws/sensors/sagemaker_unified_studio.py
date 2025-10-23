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

"""This module contains the Amazon SageMaker Unified Studio Notebook sensor."""

from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.sagemaker_unified_studio import (
    SageMakerNotebookHook,
)
from airflow.providers.common.compat.sdk import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SageMakerNotebookSensor(BaseSensorOperator):
    """
    Waits for a Sagemaker Workflows Notebook execution to reach any of the status below.

    'FAILED', 'STOPPED', 'COMPLETED'

    :param execution_id: The Sagemaker Workflows Notebook running execution identifier
    :param execution_name: The Sagemaker Workflows Notebook unique execution name
    """

    def __init__(self, *, execution_id: str, execution_name: str, **kwargs):
        super().__init__(**kwargs)
        self.execution_id = execution_id
        self.execution_name = execution_name
        self.success_state = ["COMPLETED"]
        self.in_progress_states = ["PENDING", "RUNNING"]

    def hook(self):
        return SageMakerNotebookHook(execution_name=self.execution_name)

    # override from base sensor
    def poke(self, context=None):
        status = self.hook().get_execution_status(execution_id=self.execution_id)

        if status in self.success_state:
            log_info_message = f"Exiting Execution {self.execution_id} State: {status}"
            self.log.info(log_info_message)
            return True
        if status in self.in_progress_states:
            return False
        error_message = f"Exiting Execution {self.execution_id} State: {status}"
        self.log.info(error_message)
        raise AirflowException(error_message)

    def execute(self, context: Context):
        # This will invoke poke method in the base sensor
        log_info_message = f"Polling Sagemaker Workflows Artifact execution: {self.execution_name} and execution id: {self.execution_id}"
        self.log.info(log_info_message)
        super().execute(context=context)
