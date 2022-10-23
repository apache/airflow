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
"""This module contains the Apache Livy sensor."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence

from airflow.providers.apache.livy.hooks.livy import LivyHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class LivySensor(BaseSensorOperator):
    """
    Monitor a Livy sessions for termination.

    :param livy_conn_id: reference to a pre-defined Livy connection
    :param batch_id: identifier of the monitored batch
        depends on the option that's being modified.
    """

    template_fields: Sequence[str] = ("batch_id",)

    def __init__(
        self,
        *,
        batch_id: int | str,
        livy_conn_id: str = "livy_default",
        livy_conn_auth_type: Any | None = None,
        extra_options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.batch_id = batch_id
        self._livy_conn_id = livy_conn_id
        self._livy_conn_auth_type = livy_conn_auth_type
        self._livy_hook: LivyHook | None = None
        self._extra_options = extra_options or {}

    def get_hook(self) -> LivyHook:
        """
        Get valid hook.

        :return: hook
        :rtype: LivyHook
        """
        if self._livy_hook is None or not isinstance(self._livy_hook, LivyHook):
            self._livy_hook = LivyHook(
                livy_conn_id=self._livy_conn_id,
                extra_options=self._extra_options,
                auth_type=self._livy_conn_auth_type,
            )
        return self._livy_hook

    def poke(self, context: Context) -> bool:
        batch_id = self.batch_id

        status = self.get_hook().get_batch_state(batch_id)
        return status in self.get_hook().TERMINAL_STATES
