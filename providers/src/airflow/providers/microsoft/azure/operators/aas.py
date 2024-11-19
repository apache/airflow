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
"""This module contains Azure Analysis Services operators."""

from __future__ import annotations

import time
from functools import cached_property
from typing import TYPE_CHECKING

from airflow.configuration import conf
from airflow.models import BaseOperator
from bassie.providers.microsoft.azure.hooks.aas import (
    AasHook,
    AasModelRefreshState,
    AasModelRefreshException,
)
from bassie.providers.microsoft.azure.triggers.aas import AasExecutionTrigger

if TYPE_CHECKING:
    from airflow.utils.context import Context

DEFER_METHOD_NAME = "execute_complete"


class AasRefreshOperator(BaseOperator):
    """
    Operator to initiate and monitor a refresh operation on an Azure Analysis Services model.
    """
    def __init__(
        self,
        aas_conn_id: str,
        server_name: str,
        server_location: str,
        database_name: str,
        polling_period_seconds: int = 30,
        wait_for_termination: bool = True,
        deferrable: bool = conf.getboolean(
            "operators", "default_deferrable", fallback=False
        ),
        cancel_previous_runs: bool = False,
        **kwargs,
    ) -> None:
        """
        Create a new AasRefreshOperator.

        Parameters
        ----------
        aas_conn_id : str
            The connection ID for Azure Analysis Services.
        server_name : str
            The name of the Azure Analysis Services server.
        server_location : str
            The location of the Azure Analysis Services server.
        database_name : str
            The name of the database in the Azure Analysis Services server.
        polling_period_seconds : int, optional
            The time in seconds between polling for the status of the refresh operation (default is 30).
        wait_for_termination : bool, optional
            Whether to wait for the refresh operation to terminate before completing the task (default is True).
        deferrable : bool, optional
            Whether the operator is deferrable (default is the value of 'operators.default_deferrable' config).
        cancel_previous_runs : bool, optional
            Whether to cancel any previous refresh operations before starting a new one (default is False).
        **kwargs
            Additional keyword arguments passed to the BaseOperator.
        """
        super().__init__(**kwargs)
        self.aas_conn_id = aas_conn_id
        self.polling_period_seconds = polling_period_seconds
        self.wait_for_termination = wait_for_termination
        self.deferrable = deferrable
        self.cancel_previous_runs = cancel_previous_runs
        self.server_name = server_name
        self.server_location = server_location
        self.database_name = database_name

    @cached_property
    def _hook(self):
        return AasHook(
            self.aas_conn_id,
        )

    def execute(self, context: Context):
        """
        Execute the refresh operation and monitor its status if wait_for_termination is True.

        This method initiates a refresh operation on the specified Azure Analysis Services model.
        If cancel_previous_runs is True, it cancels any previously scheduled refresh operations
        before starting a new one. It then logs the initiation of the refresh operation and, if
        wait_for_termination is True, monitors the execution of the refresh operation.

        Parameters
        ----------
        context : Context
            The task context during execution.
        """
        if self.cancel_previous_runs:
            self.cancel_all_runs()

        self.operation_id = self._hook.initiate_refresh(
            server_name=self.server_name,
            server_location=self.server_location,
            database_name=self.database_name,
        )
        self.log.info(
            "Task: refresh operation initiated for %s on %s: operation %s was started",
            self.database_name,
            self.server_name,
            self.operation_id,
        )

        if self.wait_for_termination:
            self.monitor_execution()

    def get_refresh_state(self):
        """
        Get the current state of the refresh operation.

        This method retrieves the details of the refresh operation using the operation ID and
        returns an AasModelRefreshState object representing the current state of the refresh operation.

        Returns
        -------
        AasModelRefreshState
            An object representing the current state of the refresh operation.
        """
        refresh = self._hook.get_refresh_details(
            server_location=self.server_location,
            server_name=self.server_name,
            database_name=self.database_name,
            operation_id=self.operation_id,
        )
        return AasModelRefreshState(**refresh)

    def cancel_all_runs(self):
        """
        Cancel all active refresh operations on the specified model.

        This method retrieves the refresh history of the specified model and cancels any refresh
        operations that are not in a terminal state. It logs the cancellation of each operation.
        """
        refreshes = self._hook.get_refresh_history(
            server_location=self.server_location,
            server_name=self.server_name,
            database_name=self.database_name,
        )
        active_refreshes = [
            r for r in refreshes if not AasModelRefreshState(**r).is_terminal
        ]
        for refresh in active_refreshes:
            op = AasModelRefreshState(**refresh)
            self._hook.cancel_refresh(
                server_location=self.server_location,
                server_name=self.server_name,
                database_name=self.database_name,
                operation_id=op.operation_id,
            )
            self.log.info(
                "Task: canceling previously scheduled refreshes, operation %s was canceled.",
                op.operation_id,
            )

    def monitor_execution(self):
        """
        Monitor the execution of the refresh operation.

        This method monitors the status of the refresh operation. If the operator is deferrable,
        it defers the task and waits for the trigger to resume it. Otherwise, it polls the status
        of the refresh operation at regular intervals until the operation reaches a terminal state.

        Raises
        ------
        AasModelRefreshException
            If the refresh operation fails.
        """
        if self.deferrable:
            self.defer(
                trigger=AasExecutionTrigger(
                    server_name=self.server_name,
                    server_location=self.server_location,
                    database_name=self.database_name,
                    aas_conn_id=self.aas_conn_id,
                    operation_id=self.operation_id,
                ),
                method_name=self.execute_complete.__name__,
            )
        else:
            refresh_state = self.get_refresh_state()
            while not refresh_state.is_terminal:
                time.sleep(self.polling_period_seconds)
                refresh_state = self.get_refresh_state()
                self.log.info(
                    "Current state of the Azure Analysis Services model refresh for database %s on %s is %s",
                    self.database_name,
                    self.server_name,
                    refresh_state,
                )
        if refresh_state.is_succeeded:
            self.log.info("Processing succeeded")
        else:
            raise AasModelRefreshException(
                f"Task failed. Final state {refresh_state}. Reason:\n{refresh_state.error_message}"
            )

    def execute_complete(self, context: Context, event: dict):
        """
        Callback for when the deferred task is resumed.

        This method is called when the deferred task is resumed. It checks the final state of the
        refresh operation and raises an exception if the operation did not succeed.

        Parameters
        ----------
        context : Context
            The task context during execution.
        event : dict
            The event that triggered the resumption of the task.

        Raises
        ------
        AasModelRefreshException
            If the refresh operation failed.
        """
        if not AasModelRefreshState(event["state"]).is_succeeded:
            raise AasModelRefreshException(
                f"The refresh operation {event['operation_id']} failed:{event['errors']}"
            )

    def on_kill(self) -> None:
        """
        Cancel the refresh operation if the task is killed.

        This method is called when the task is killed. It cancels the refresh operation if it is
        currently running.
        """
        if self.operation_id:
            self._hook.cancel_refresh(self.operation_id)
            self.log.info(
                "Task: refresh operation for %s on %s: operation %s was requested to be cancelled.",
                self.database_name,
                self.server_name,
                self.operation_id,
            )
        else:
            self.log.error(
                "Task: refresh operation for %s on %s was requested to be cancelled.",
                self.database_name,
                self.server_name,
            )
