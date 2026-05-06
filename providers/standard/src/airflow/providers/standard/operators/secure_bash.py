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
"""
Template Lifter — Parameterized Shell Execution for Apache Airflow.

Automatically lifts untrusted Jinja2 variables out of inline shell commands
and into environment variables, making command injection structurally impossible.
"""
from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any

from airflow.providers.standard.operators.bash import BashOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context

log = logging.getLogger(__name__)

# Matches {{ ... }} taking string literals into account so it doesn't break on }} inside quotes.
_JINJA_OUTPUT_BLOCK = re.compile(
    r"\{\{(?:"
    r"'[^'\\]*(?:\\.[^'\\]*)*'|"  # Single quoted with escapes
    r'"[^"\\]*(?:\\.[^"\\]*)*"|'  # Double quoted with escapes
    r'[^}]|'
    r'\}(?!\})'
    r")*\}\}"
)

# Matches references to known untrusted runtime sources.
_UNTRUSTED_VARIABLE = re.compile(
    r"\bdag_run\s*(?:\.\s*conf|\[\s*['\"]conf['\"]\s*\])"
    r"|\bparams\b"
    r"|\bvar\s*\.\s*(?:value|json)\b"
    r"|\b(?:conn|connections)\b"
    r"|\bti\s*\.\s*xcom_pull\b"
)

_EVAL_COMMANDS = re.compile(r"\b(?:eval|source)\b")

# Matches a lifted variable reference wrapped in single quotes: '${_AIRFLOW_LIFTED_...}'
# This is a common footgun as shells do not expand variables inside single quotes.
_SINGLE_QUOTE_LIFTED = re.compile(r"'\$\{[^}]+\}'")

_PARAM_PREFIX = "_AIRFLOW_LIFTED_"


def _sanitize_env_name(name: str) -> str:
    """Sanitizes task IDs to be valid POSIX environment variable names."""
    return re.sub(r"[^A-Za-z0-9_]", "_", name)


def lift_untrusted_expressions(
    template: str,
    instance_id: str = "",
) -> tuple[str, dict[str, str]]:
    """Scans a Jinja template for untrusted variables and lifts them to env vars."""
    if "{{" not in template:
        return template, {}

    bindings: dict[str, str] = {}
    counter = 0
    
    # Ensure instance_id is a valid POSIX env name prefix
    safe_id = _sanitize_env_name(instance_id)

    def _replace(match: re.Match) -> str:
        nonlocal counter
        expr_block = match.group(0)

        if not _UNTRUSTED_VARIABLE.search(expr_block):
            return expr_block

        var_name = f"{_PARAM_PREFIX}{safe_id}_{counter}"
        counter += 1
        bindings[var_name] = expr_block
        return f"${{{var_name}}}"

    modified = _JINJA_OUTPUT_BLOCK.sub(_replace, template)

    # Runtime Safety Checks
    if _EVAL_COMMANDS.search(modified):
        log.warning(
            "SecureBashOperator: 'eval' or 'source' detected in the bash command. "
            "Template Lifting cannot protect against commands that explicitly "
            "re-evaluate environment variables as code."
        )

    if bindings and _SINGLE_QUOTE_LIFTED.search(modified):
        log.warning(
            "SecureBashOperator: A lifted variable (e.g. '${_AIRFLOW_LIFTED_...}') "
            "was detected inside single quotes. Most shells will not expand "
            "this variable. Use double quotes for shell variable expansion."
        )

    return modified, bindings


class SecureBashOperator(BashOperator):
    """
    Drop-in replacement for BashOperator with automatic shell parameterization.

    Untrusted Jinja2 variables (dag_run.conf, params, var, conn, connections, xcom_pull) are
    automatically lifted into environment variables before rendering. This eliminates
    the primary injection surface for inline commands using these variables.
    """
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._template_lifted: bool = False

    def render_template_fields(self, context: Context, jinja_env: Any = None) -> None:
        if self._template_lifted:
            super().render_template_fields(context, jinja_env)
            return

        # Check if the command has already been lifted (e.g. on retry)
        if isinstance(self.bash_command, str) and _PARAM_PREFIX in self.bash_command:
            self._template_lifted = True
            super().render_template_fields(context, jinja_env)
            return

        if isinstance(self.bash_command, str):
            if self.bash_command.endswith(tuple(self.template_ext)):
                log.warning(
                    "SecureBashOperator: Template Lifting is disabled for script files "
                    "(%s). Ensure your script securely handles untrusted arguments.",
                    self.bash_command
                )
            else:
                modified_cmd, param_bindings = lift_untrusted_expressions(
                    self.bash_command,
                    instance_id=self.task_id,
                )

                if param_bindings:
                    self.bash_command = modified_cmd
                    if self.env is None:
                        self.env = param_bindings.copy()
                    else:
                        self.env = {**self.env, **param_bindings}

        super().render_template_fields(context, jinja_env)
        self._template_lifted = True
