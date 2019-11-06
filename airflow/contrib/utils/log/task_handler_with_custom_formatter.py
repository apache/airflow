# -*- coding: utf-8 -*-
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

import logging

from logging import StreamHandler
from airflow import configuration as conf
from airflow.utils.helpers import parse_template_string


class TaskHandlerWithCustomFormatter(StreamHandler):
    def __init__(self, stream):
        super(TaskHandlerWithCustomFormatter, self).__init__()

    def set_context(self, ti):
        if ti.raw:
            return
        prefix = conf.get('core', 'task_log_prefix_template')

        rendered_prefix = ""
        if prefix:
            _, self.prefix_jinja_template = parse_template_string(prefix)
            rendered_prefix = self._render_prefix(ti)

        self.setFormatter(logging.Formatter(rendered_prefix + ":" + self.formatter._fmt))
        self.setLevel(self.level)

    def _render_prefix(self, ti):
        if self.prefix_jinja_template:
            jinja_context = ti.get_template_context()
            return self.prefix_jinja_template.render(**jinja_context)
        logging.warning("'task_log_prefix_template' is in invalid format, ignoring the variable value")
        return ""
