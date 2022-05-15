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
"""Databricks sensors"""

from typing import TYPE_CHECKING, Any, Dict, Optional, Sequence, Union

from airflow.exceptions import AirflowException
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context

if TYPE_CHECKING:
    from airflow.sensors.base import PokeReturnValue


class DatabricksJobRunSensor(BaseSensorOperator):
    """
    Check for the state of a submitted Databricks job run or specific task of a job run.

    :param run_id: Id of the submitted Databricks job run or specific task of a job run. (templated)
    :param databricks_conn_id: Reference to the :ref:`Databricks connection <howto/connection:databricks>`.
        By default and in the common case this will be ``databricks_default``. To use
        token based authentication, provide the key ``token`` in the extra field for the
        connection and create the key ``host`` and leave the ``host`` field empty.
    :param retry_limit: Amount of times retry if the Databricks backend is
        unreachable. Its value must be greater than or equal to 1.
    :param retry_delay_seconds: Number of seconds to wait between retries (it
            might be a floating point number).
    :param databricks_retry_args: An optional dictionary with arguments passed to ``tenacity.Retrying`` class.
    """

    template_fields: Sequence[str] = ('run_id',)

    # Databricks brand color (blue) under white text
    ui_color = '#1CB1C2'
    ui_fgcolor = '#fff'

    def __init__(
        self,
        *,
        run_id: int,
        databricks_conn_id: str = 'databricks_default',
        retry_limit: int = 3,
        retry_delay_seconds: int = 1,
        databricks_retry_args: Optional[Dict[Any, Any]] = None,
        **kwargs,
    ) -> None:
        """Creates a new ``DatabricksJobSensor`` instance."""
        super().__init__(**kwargs)
        self.run_id = run_id
        self.databricks_conn_id = databricks_conn_id
        self.retry_limit = retry_limit
        self.retry_delay_seconds = retry_delay_seconds
        self.databricks_retry_args = databricks_retry_args

    def poke(self, context: Context) -> Union[bool, 'PokeReturnValue']:
        hook = self._get_hook()
        run_state = hook.get_run_state(self.run_id)
        if run_state.is_terminal:
            if run_state.is_successful:
                self.log.info('%s completed successfully.', self.task_id)
                return True
            else:
                run_output = hook.get_run_output(self.run_id)
                notebook_error = run_output['error']
                error_message = (
                    f'{self.task_id} failed with terminal state: {run_state} '
                    f'and with the error: {notebook_error}'
                )
                raise AirflowException(error_message)
        else:
            run_page_url = hook.get_run_page_url(self.run_id)
            self.log.info(
                'Task %s is in state: %s, Spark UI and logs are available at %s',
                self.task_id,
                run_state,
                run_page_url,
            )
            self.log.info('Waiting for run %s to complete.', self.run_id)
            return False

    def _get_hook(self) -> DatabricksHook:
        return DatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.retry_limit,
            retry_delay=self.retry_delay_seconds,
            retry_args=self.databricks_retry_args,
        )
