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
"""Utilities initializing and managing Go modules."""
from __future__ import annotations

import os

from airflow.utils.process_utils import execute_in_subprocess


def init_module(go_module_name: str, go_module_path: str) -> None:
    """Initialize a Go module. If a ``go.mod`` file already exists, this function
    will do nothing.

    :param go_module_name: The name of the Go module to initialize.
    :param go_module_path: The path to the directory containing the Go module.
    :return:
    """
    if os.path.isfile(os.path.join(go_module_path, "go.mod")):
        return
    go_mod_init_cmd = ["go", "mod", "init", go_module_name]
    execute_in_subprocess(go_mod_init_cmd, cwd=go_module_path)


def install_dependencies(go_module_path: str) -> None:
    """Install dependencies for a Go module.

    :param go_module_path: The path to the directory containing the Go module.
    :return:
    """
    go_mod_tidy = ["go", "mod", "tidy"]
    execute_in_subprocess(go_mod_tidy, cwd=go_module_path)
