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

import logging
import sys
from functools import cached_property
from typing import Any

from airflow.hooks.base import BaseHook

log = logging.getLogger(__name__)


class PythonVirtualenvHook(BaseHook):
    """
    Interact with Python Virtualenv.

    :param venv_conn_id: The ID of Python Virtualenv Connection.
    """

    conn_name_attr = "venv_conn_id"
    default_conn_name = "venv_default"
    conn_type = "python_venv"
    hook_name = "Python Virtualenv"

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        v = sys.version_info
        return {
            "hidden_fields": ["host", "schema", "port", "login", "password", "extra"],
            "relabeling": {},
            "placeholders": {"python_version": f"{v.major}.{v.minor}"},
        }

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        from flask_appbuilder.fieldwidgets import BS3TextAreaFieldWidget, BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import BooleanField, StringField
        from wtforms.validators import InputRequired, Optional

        return {
            "requirements": StringField(
                lazy_gettext("Requirements"),
                description=lazy_gettext("Python requirement strings separated by newline."),
                widget=BS3TextAreaFieldWidget(),
                validators=[InputRequired()],
            ),
            "python_version": StringField(
                lazy_gettext("Python Version"),
                description=lazy_gettext("The Python version to run the virtual environment with."),
                widget=BS3TextFieldWidget(),
                validators=[Optional()],
            ),
            "system_site_packages": BooleanField(
                lazy_gettext("System Site Packages"),
                description=lazy_gettext(
                    "Whether to include system_site_packages in the virtual environment."
                ),
                default=False,
            ),
            "pip_install_options": StringField(
                lazy_gettext("Pip Install Options"),
                description=lazy_gettext("A list of pip install options when installing requirements."),
                widget=BS3TextFieldWidget(),
                validators=[Optional()],
            ),
            "index_urls": StringField(
                lazy_gettext("Index URLs"),
                description=lazy_gettext("Comma separated list of URLs to search for packages."),
                widget=BS3TextFieldWidget(),
                validators=[Optional()],
            ),
            "venv_cache_path": StringField(
                lazy_gettext("Virtual Environment Cache Path"),
                description=lazy_gettext("Path to cache the virtual environment."),
                widget=BS3TextFieldWidget(),
                validators=[Optional()],
            ),
        }

    def __init__(self, venv_conn_id: str | None = default_conn_name, **kwargs) -> None:
        super().__init__(**kwargs)

        self._venv_conn_id = venv_conn_id
        self._conn = self.get_connection(venv_conn_id) if venv_conn_id else None

    def get_conn(self) -> None:
        pass

    @cached_property
    def extra(self) -> dict[str, Any]:
        if self._conn:
            return self._conn.extra_dejson
        return {}

    @cached_property
    def requirements(self) -> list[str]:
        """Get the list of requirements to install in the virtual environment."""
        if self.extra.get("requirements"):
            return [requirement.strip() for requirement in self.extra["requirements"].split("\n")]
        return []

    @cached_property
    def python_version(self) -> str | None:
        return self.extra.get("python_version")

    @cached_property
    def pip_install_options(self) -> list[str] | None:
        if self.extra.get("pip_install_options"):
            return self.extra["pip_install_options"].split()
        return []

    @cached_property
    def index_urls(self) -> list[str] | None:
        if self.extra.get("index_urls"):
            return [index_url.strip() for index_url in self.extra["index_urls"].split(",")]
        return None

    @cached_property
    def venv_cache_path(self) -> str | None:
        if self.extra.get("venv_cache_path"):
            return self.extra["venv_cache_path"]
        return None

    @cached_property
    def system_site_packages(self) -> bool:
        return self.extra.get("system_site_packages", True)
