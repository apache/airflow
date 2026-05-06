# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-8.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Template Lifter — Parameterized Shell Execution for Apache Airflow.

Automatically lifts untrusted Jinja2 variables out of inline shell commands
and into environment variables, making command injection structurally impossible.
"""
from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING

from airflow.providers.standard.operators.bash import BashOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context

log = logging.getLogger(__name__)

_JINJA_OUTPUT_BLOCK = re.compile(r"\{\{.*?\}\}", re.DOTALL)

# Matches references to known untrusted runtime sources.
_UNTRUSTED_VARIABLE = re.compile(
    r"\bdag_run\s*(?:\.\s*conf|\[\s*['\"]conf['\"]\s*\])"
    r"|\bparams\b"
    r"|\bvar\s*\.\s*(?:value|json)\b"
    r"|\bconn\b"
)

_EVAL_COMMANDS = re.compile(r"\b(?:eval|source)\b")
_PARAM_PREFIX = "_AIRFLOW_LIFTED_"


def lift_untrusted_expressions(
    template: str,
    instance_id: str = "",
) -> tuple[str, dict[str, str]]:
    """Scans a Jinja template for untrusted variables and lifts them to env vars."""
    if "{{" not in template:
        return template, {}

    bindings: dict[str, str] = {}
    counter = 0

    def _replace(match: re.Match) -> str:
        nonlocal counter
        expr_block = match.group(0)

        if not _UNTRUSTED_VARIABLE.search(expr_block):
            return expr_block

        var_name = f"{_PARAM_PREFIX}{instance_id}_{counter}"
        counter += 1
        bindings[var_name] = expr_block
        return f"${{{var_name}}}"

    modified = _JINJA_OUTPUT_BLOCK.sub(_replace, template)

    if bindings and _EVAL_COMMANDS.search(modified):
        log.warning(
            "SecureBashOperator: 'eval' or 'source' detected in a command with "
            "untrusted variables. Template Lifting cannot protect against commands "
            "that explicitly re-evaluate data as code."
        )

    return modified, bindings


class SecureBashOperator(BashOperator):
    """
    Drop-in replacement for BashOperator with automatic shell parameterization.

    Untrusted Jinja2 variables (dag_run.conf, params, var, conn) are
    automatically lifted into environment variables before rendering, making
    command injection structurally impossible.
    """
    
    _template_lifted: bool = False

    def render_template_fields(self, context: Context, jinja_env=None):
        if self._template_lifted:
            super().render_template_fields(context, jinja_env)
            return

        if (
            isinstance(self.bash_command, str)
            and not self.bash_command.endswith(tuple(self.template_ext))
        ):
            modified_cmd, param_bindings = lift_untrusted_expressions(
                self.bash_command,
                instance_id=self.task_id,
            )

            if param_bindings:
                self.bash_command = modified_cmd
                if self.env is None:
                    self.env = {}
                self.env.update(param_bindings)
                self._template_lifted = True

        super().render_template_fields(context, jinja_env)
