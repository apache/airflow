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
"""Custom logging formatter for Airflow"""

import logging
from typing import TYPE_CHECKING, Optional

from airflow.utils.helpers import parse_template_string, render_template_to_string
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from jinja2 import Template
    from sqlalchemy.orm import Session

    from airflow.models.taskinstance import TaskInstance


class TaskHandlerWithCustomFormatter(logging.StreamHandler):
    """Custom implementation of StreamHandler, a class which writes logging records for Airflow"""

    prefix_jinja_template: Optional["Template"] = None

    @provide_session
    def set_context(self, ti, *, session: "Session" = NEW_SESSION) -> None:
        """
        Accept the run-time context (i.e. the current task) and configure the formatter accordingly.

        :param ti:
        :return:
        """
        if ti.raw or self.formatter is None:
            return
        prefix = ti.get_dagrun().get_task_prefix_template(session=session)

        if prefix:
            _, self.prefix_jinja_template = parse_template_string(prefix)
            rendered_prefix = self._render_prefix(ti)
        else:
            rendered_prefix = ""
        formatter = logging.Formatter(f"{rendered_prefix}:{self.formatter._fmt}")
        self.setFormatter(formatter)
        self.setLevel(self.level)

    def _render_prefix(self, ti: "TaskInstance") -> str:
        if self.prefix_jinja_template:
            jinja_context = ti.get_template_context()
            return render_template_to_string(self.prefix_jinja_template, jinja_context)
        logging.warning("'task_log_prefix_template' is in invalid format, ignoring the variable value")
        return ""
