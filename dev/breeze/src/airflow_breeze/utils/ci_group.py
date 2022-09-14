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
from contextlib import contextmanager

from airflow_breeze.utils.console import MessageType, get_console
from airflow_breeze.utils.parallel import Output
from airflow_breeze.utils.path_utils import skip_group_output

# only allow top-level group
_in_ci_group = False


@contextmanager
def ci_group(title: str, message_type: MessageType | None = MessageType.INFO, output: Output | None = None):
    """
    If used in GitHub Action, creates an expandable group in the GitHub Action log.
    Otherwise, display simple text groups.

    For more information, see:
    https://docs.github.com/en/free-pro-team@latest/actions/reference/workflow-commands-for-github-actions#grouping-log-lines
    """
    global _in_ci_group
    if _in_ci_group or skip_group_output():
        yield
        return
    if os.environ.get('GITHUB_ACTIONS', 'false') != "true":
        if message_type is not None:
            get_console(output=output).print(f"\n[{message_type.value}]{title}\n")
        else:
            get_console(output=output).print(f"\n{title}\n")
        yield
        return
    _in_ci_group = True
    if message_type is not None:
        get_console().print(f"::group::[{message_type.value}]{title}[/]")
    else:
        get_console().print(f"::group::{title}")
    try:
        yield
    finally:
        get_console().print("::endgroup::")
        _in_ci_group = False
