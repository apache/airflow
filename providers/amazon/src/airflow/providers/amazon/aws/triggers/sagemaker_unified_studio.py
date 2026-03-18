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

"""This module contains the Amazon SageMaker Unified Studio Notebook job trigger."""

from __future__ import annotations

from airflow.triggers.base import BaseTrigger


class SageMakerNotebookJobTrigger(BaseTrigger):
    """
    Watches for a notebook job, triggers when it finishes.

    Examples:
     .. code-block:: python

        from airflow.providers.amazon.aws.triggers.sagemaker_unified_studio import SageMakerNotebookJobTrigger

        notebook_trigger = SageMakerNotebookJobTrigger(
            execution_id="notebook_job_1234",
            execution_name="notebook_task",
            waiter_delay=10,
            waiter_max_attempts=1440,
        )

    :param execution_id: A unique, meaningful id for the task.
    :param execution_name: A unique, meaningful name for the task.
    :param waiter_delay: Interval in seconds to check the notebook execution status.
    :param waiter_max_attempts: Number of attempts to wait before returning FAILED.
    """

    def __init__(self, execution_id, execution_name, waiter_delay, waiter_max_attempts, **kwargs):
        super().__init__(**kwargs)
        self.execution_id = execution_id
        self.execution_name = execution_name
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts

    def serialize(self):
        return (
            # dynamically generate the fully qualified name of the class
            self.__class__.__module__ + "." + self.__class__.__qualname__,
            {
                "execution_id": self.execution_id,
                "execution_name": self.execution_name,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self):
        pass
