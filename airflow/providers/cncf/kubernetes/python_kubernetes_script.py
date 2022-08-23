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
#
"""Utilities for using the kubernetes decorator"""
import os
from collections import deque

import jinja2


def _balance_parens(after_decorator):
    num_paren = 1
    after_decorator = deque(after_decorator)
    after_decorator.popleft()
    while num_paren:
        current = after_decorator.popleft()
        if current == "(":
            num_paren = num_paren + 1
        elif current == ")":
            num_paren = num_paren - 1
    return ''.join(after_decorator)


def remove_task_decorator(python_source: str, task_decorator_name: str) -> str:
    """
    Removed @kubernetes_task

    :param python_source:
    """
    if task_decorator_name not in python_source:
        return python_source
    split = python_source.split(task_decorator_name)
    before_decorator, after_decorator = split[0], split[1]
    if after_decorator[0] == "(":
        after_decorator = _balance_parens(after_decorator)
    if after_decorator[0] == "\n":
        after_decorator = after_decorator[1:]
    return before_decorator + after_decorator


def write_python_script(
    jinja_context: dict,
    filename: str,
    render_template_as_native_obj: bool = False,
):
    """
    Renders the python script to a file to execute in the virtual environment.

    :param jinja_context: The jinja context variables to unpack and replace with its placeholders in the
        template file.
    :param filename: The name of the file to dump the rendered script to.
    :param render_template_as_native_obj: If ``True``, rendered Jinja template would be converted
        to a native Python object
    """
    template_loader = jinja2.FileSystemLoader(searchpath=os.path.dirname(__file__))
    template_env: jinja2.Environment
    if render_template_as_native_obj:
        template_env = jinja2.nativetypes.NativeEnvironment(
            loader=template_loader, undefined=jinja2.StrictUndefined
        )
    else:
        template_env = jinja2.Environment(loader=template_loader, undefined=jinja2.StrictUndefined)
    template = template_env.get_template('python_kubernetes_script.jinja2')
    template.stream(**jinja_context).dump(filename)
