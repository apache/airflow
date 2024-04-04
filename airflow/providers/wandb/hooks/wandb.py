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

import logging
import os
from typing import Any

import wandb
from airflow.hooks.base import BaseHook


class WandbHook(BaseHook):
    """
    Use Wandb Python Library to interact with Weights and Biases API.

    :param wandb_conn_id: Weights and Biases connection
    :type wandb_conn_id: str
    """

    conn_name_attr = "conn_id"
    default_conn_name = "wandb_default"
    conn_type = "wandb"
    hook_name = "WandbHook"

    def __init__(
        self,
        conn_id: str = default_conn_name,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["schema", "port", "extra", "host", "login"],
            "relabeling": {"password": "API Key"},
            "placeholders": {},
        }

    def _set_env_variables(self) -> None:
        """Set the WANDB_API_KEY environment variable."""
        conn = self.get_connection(self.conn_id)

        os.environ["WANDB_API_KEY"] = conn.password

    def _unset_env_variables(self) -> None:
        """Unset the WANDB_API_KEY environment variable."""

        for variable in ["WANDB_API_KEY"]:
            try:
                del os.environ[variable]
            except KeyError:
                logging.warning(f"{variable} not found in environment variables.")

    def get_conn(self) -> wandb:
        """
        Configure the WANDB_API_KEY environment variable and return the Wandb client.

        :return: Authenticated Wandb client
        :rtype: wandb
        """
        self._set_env_variables()

        return wandb
