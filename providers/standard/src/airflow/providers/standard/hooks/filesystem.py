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
from __future__ import annotations

from pathlib import Path
from typing import Any

from airflow.providers.standard.version_compat import BaseHook


class FSHook(BaseHook):
    """
    Allows for interaction with an file server.

    Connection should have a name and a path specified under extra:

    example:
    Connection Id: fs_test
    Connection Type: File (path)
    Host, Schema, Login, Password, Port: empty
    Extra: {"path": "/tmp"}
    """

    conn_name_attr = "fs_conn_id"
    default_conn_name = "fs_default"
    conn_type = "fs"
    hook_name = "File (path)"

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {"path": StringField(lazy_gettext("Path"), widget=BS3TextFieldWidget())}

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["host", "schema", "port", "login", "password", "extra"],
            "relabeling": {},
            "placeholders": {},
        }

    def __init__(self, fs_conn_id: str = default_conn_name, **kwargs):
        super().__init__(**kwargs)
        conn = self.get_connection(fs_conn_id)
        self.basepath = conn.extra_dejson.get("path", "")
        self.conn = conn

    def get_conn(self) -> None:
        pass

    def get_path(self) -> str:
        """
        Get the path to the filesystem location.

        :return: the path.
        """
        return self.basepath

    def test_connection(self):
        """Test File connection."""
        try:
            p = self.get_path()
            if not p:
                return False, "File Path is undefined."
            if not Path(p).exists():
                return False, f"Path {p} does not exist."
            return True, f"Path {p} is existing."
        except Exception as e:
            return False, str(e)
