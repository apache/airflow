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

from typing import Any

from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
from flask_babel import lazy_gettext
from wtforms import StringField

from airflow.providers.apache.hive.hooks.hive import HiveCliHook


class KyuubiHook(HiveCliHook):
    """
    Wrapper around the Kyuubi connection.

    Inherits from HiveCliHook to leverage Hive CLI/Beeline functionality,
    but allows specifying a custom Beeline path for Kyuubi.

    :param kyuubi_conn_id: Reference to the Kyuubi connection id.
    """

    conn_name_attr = "kyuubi_conn_id"
    default_conn_name = "kyuubi_default"
    conn_type = "kyuubi"
    hook_name = "Kyuubi"

    def __init__(self, kyuubi_conn_id: str = default_conn_name, **kwargs) -> None:
        super().__init__(hive_cli_conn_id=kyuubi_conn_id, **kwargs)

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to Kyuubi connection form."""
        widgets = super().get_connection_form_widgets()
        widgets["beeline_path"] = StringField(
            lazy_gettext("Beeline Path"),  # type: ignore[arg-type]
            widget=BS3TextFieldWidget(),  # type: ignore[arg-type]
            description="Path to the Kyuubi Beeline executable (e.g., /path/to/bin/beeline)",
        )
        return widgets

    def _prepare_cli_cmd(self) -> list[Any]:
        """Create the command list from available information, using custom beeline path if provided."""
        cmd = super()._prepare_cli_cmd()
        
        # If use_beeline is enabled (which is default in HiveCliHook widgets, but check logic)
        # HiveCliHook sets defaults.
        
        beeline_path = self.conn.extra_dejson.get("beeline_path")
        if self.use_beeline and beeline_path:
            self.log.info("Using custom Beeline path: %s", beeline_path)
            cmd[0] = beeline_path
            
        return cmd
