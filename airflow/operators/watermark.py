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

from abc import ABC, abstractmethod
from functools import cached_property
from typing import Any

from airflow.models import BaseOperator
from airflow.process_state import ProcessState
from airflow.utils.helpers import skip_if_not_latest


class AbstractWatermarkOperator(BaseOperator, ABC):
    """
    WatermarkOperator records state in variables using the ProcessState class.

    You must implement property ``watermark_execute`` and ``get_high_watermark``.

    Before ``watermark_execute`` is called, we set attribute ``new_high_watermark`` with the value
    returned from ``get_high_watermark``.

    After successful completion of ``watermark_execute``, the new high watermark be
    persisted to the state store.  Otherwise, the stored high watermark will not be updated.

    Here's a simple example of an incremental process where each time you just want to
    process from last run, using default namespace::
        >>> class MyS3WatermarkOperator(AbstractWatermarkOperator):
        >>>     def get_high_watermark(self):
        >>>         return pendulum.now().isoformat()
        >>>
        >>>     def watermark_execute(self, context: Any) -> Any:
        >>>         print(f"low_watermark = '{self.low_watermark}'")
        >>>
        >>> op = MyS3WatermarkOperator(
        >>>     task_id='hello',
        >>>     watermark_process_name='my-incremental-task',
        >>> )
        >>>
        >>> op.execute()
        '1900-01-01'
        >>> op.state.get_value()  # watermark has now been updated
        '2021-03-17T13:10:48.979881-07:00'

    Args:
        watermark_process_name: the name (unique within watermark namespace) used for identifying this task's watermmark
            Set to None if you want to disable watermark functionality.
        inital_load_value: on first run, the value that should be used as low watermark
        watermark_namespace: used to distinguish families of watermark processes
        latest_only: if this task should be skipped on backfills
    `"""

    def __init__(
        self,
        initial_load_value='1900-01-01',
        watermark_process_name: str = None,
        watermark_namespace='default',
        latest_only=True,
        **kwargs,
    ):
        self.watermark_process_name = watermark_process_name
        self.watermark_namespace = watermark_namespace
        self.initial_load_value = initial_load_value
        self.new_high_watermark = None
        self.latest_only = latest_only
        super().__init__(**kwargs)  # type: ignore

    @abstractmethod
    def get_high_watermark(self):
        """
        Put the logic to calculate the new high watermark here.

        If a simple incremental process it could simply be ``return str(pendulum.now())``.

        Alternatively you might retrieve the max ``updated_at`` value in a table.
        """

    @cached_property
    def state(self):
        """
        Handler for managing the watermark value (access and storage).
        """
        return ProcessState(
            namespace=self.watermark_namespace,
            process_name=self.watermark_process_name,
            default_value=self.initial_load_value,
        )

    @cached_property
    def low_watermark(self):
        return self.state.get_value()

    @abstractmethod
    def watermark_execute(self, context) -> Any:
        """
        Put here what would normally go in the ``execute`` method.

        Before ``watermark_execute`` is called, new high watermark will be calculated.

        After successful completion of ``watermark_execute``, watermark will be saved to the state store.

        Args:
            context: airflow context dictionary

        Returns: optionally anything
        """

    def execute(self, context) -> Any:
        """
        Orchestrates watermarking.

        Computes new high watermark before calling ``watermark_execute`` and updates watermark after successful completion.

        Args:
            context:

        Returns: optionally anything

        """
        if self.latest_only:
            skip_if_not_latest(self, context)
        if self.watermark_process_name:
            self.new_high_watermark = self.get_high_watermark()
        return_value = self.watermark_execute(context)
        if self.watermark_process_name:
            self.state.set_value(self.new_high_watermark)
        return return_value
