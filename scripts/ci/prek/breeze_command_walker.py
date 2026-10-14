#!/usr/bin/env python
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
"""Introspect Breeze's click command tree and print it as structured JSON.

Runs inside Breeze's virtualenv (``uv run --project dev/breeze``) because the
consumer, ``check_documented_commands.py``, runs in the prek hook venv where
``airflow_breeze`` cannot be imported.

Emits one ``REGISTRY_JSON:`` line per command path with its group / passthrough
/ options. Walks the live click objects rather than ``Context.to_info_dict()``,
which drops the ``context_settings`` that ``breeze k8s deploy-cluster`` relies
on to mark itself passthrough.
"""

from __future__ import annotations

import json

import click
from airflow_breeze.breeze import main


def is_passthrough(cmd: click.Command) -> bool:
    """True when trailing words are forwarded verbatim, so options can't be validated."""
    if (cmd.context_settings or {}).get("ignore_unknown_options"):
        return True
    return any(isinstance(param, click.Argument) and param.nargs == -1 for param in cmd.params)


def option_strings(cmd: click.Command) -> list[str]:
    strings: set[str] = set()
    for param in cmd.params:
        if isinstance(param, click.Option):
            strings.update(param.opts)
            strings.update(param.secondary_opts)
    return sorted(strings)


def walk(cmd: click.Command, path: str) -> dict[str, dict]:
    is_group = isinstance(cmd, click.Group)
    tree: dict[str, dict] = {
        path: {"group": is_group, "passthrough": is_passthrough(cmd), "opts": option_strings(cmd)}
    }
    if isinstance(cmd, click.Group):
        for name, sub in cmd.commands.items():
            tree.update(walk(sub, f"{path} {name}"))
    return tree


if __name__ == "__main__":
    print("REGISTRY_JSON:" + json.dumps(walk(main, "breeze")))
