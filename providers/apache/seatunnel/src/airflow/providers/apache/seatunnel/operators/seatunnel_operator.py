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

import os
from typing import TYPE_CHECKING

from airflow.providers.apache.seatunnel.hooks.seatunnel_hook import (
    SeaTunnelHook,
)
from airflow.sdk import BaseOperator

if TYPE_CHECKING:
    from airflow.sdk.definitions.context import Context


class SeaTunnelOperator(BaseOperator):
    """
    Execute a SeaTunnel job.

    :param config_file: Path to the SeaTunnel configuration file,
        mutually exclusive with config_content.
    :param config_content: Content of the SeaTunnel configuration,
        mutually exclusive with config_file.
    :param engine: Engine to use (flink, spark, or zeta). Default is 'zeta'.
    :param seatunnel_conn_id: Connection ID to use.
    """

    template_fields = ("config_file", "config_content", "engine")
    template_ext = (".conf", ".hocon")
    ui_color = "#1CB8FF"  # SeaTunnel blue color

    def __init__(
        self,
        *,
        config_file: str | None = None,
        config_content: str | None = None,
        engine: str = "zeta",
        seatunnel_conn_id: str = "seatunnel_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        if (config_file is None and config_content is None) or (
            config_file is not None and config_content is not None
        ):
            raise ValueError("Either config_file or config_content must be provided, but not both.")

        # Validate engine type
        valid_engines = ["flink", "spark", "zeta"]
        if engine not in valid_engines:
            raise ValueError(f"Invalid engine '{engine}'. Must be one of: {valid_engines}")

        self.config_file = config_file
        self.config_content = config_content
        self.engine = engine
        self.seatunnel_conn_id = seatunnel_conn_id
        self.temp_config_file: str | None = None

    def execute(self, context: Context) -> str:
        """
        Execute the SeaTunnel job.

        :param context: Airflow context.
        """
        hook = SeaTunnelHook(seatunnel_conn_id=self.seatunnel_conn_id, engine=self.engine)

        if self.config_content:
            self.temp_config_file = hook.create_temp_config(self.config_content)
            config_file_to_use = self.temp_config_file
        elif self.config_file:
            config_file_to_use = self.config_file
        else:
            raise ValueError("Either config_file or config_content must be provided")

        try:
            output = hook.run_job(config_file=config_file_to_use, engine=self.engine)
            self.log.info("SeaTunnel job completed successfully")
            return output
        finally:
            # Clean up temporary file if it was created
            if self.temp_config_file and os.path.exists(self.temp_config_file):
                os.unlink(self.temp_config_file)
                self.log.info("Deleted temporary config file: %s", self.temp_config_file)
