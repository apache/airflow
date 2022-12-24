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

import functools
import inspect
import os
from typing import Any


def get_python_source(x: Any) -> str | None:
    """Helper function to get Python source (or not), preventing exceptions"""
    if isinstance(x, str):
        return x

    if x is None:
        return None

    source_code = None

    if isinstance(x, functools.partial):
        source_code = inspect.getsource(x.func)

    if source_code is None:
        try:
            source_code = inspect.getsource(x)
        except TypeError:
            pass

    if source_code is None:
        try:
            source_code = inspect.getsource(x.__call__)
        except (TypeError, AttributeError):
            pass

    if source_code is None:
        source_code = f"No source code available for {type(x)}"
    return source_code


def prepare_code_snippet(file_path: str, line_no: int, context_lines_count: int = 5) -> str:
    """
    Prepare code snippet with line numbers and  a specific line marked.

    :param file_path: File name
    :param line_no: Line number
    :param context_lines_count: The number of lines that will be cut before and after.
    :return: str
    """
    with open(file_path) as text_file:
        # Highlight code
        code = text_file.read()
        code_lines = code.split("\n")
        # Prepend line number
        code_lines = [
            f">{lno:3} | {line}" if line_no == lno else f"{lno:4} | {line}"
            for lno, line in enumerate(code_lines, 1)
        ]
        # # Cut out the snippet
        start_line_no = max(0, line_no - context_lines_count - 1)
        end_line_no = line_no + context_lines_count
        code_lines = code_lines[start_line_no:end_line_no]
        # Join lines
        code = "\n".join(code_lines)
    return code


def get_terminal_formatter(**opts):
    """Returns the best formatter available in the current terminal."""
    if "256" in os.environ.get("TERM", ""):
        from pygments.formatters.terminal256 import Terminal256Formatter

        formatter = Terminal256Formatter(**opts)
    else:
        from pygments.formatters.terminal import TerminalFormatter

        formatter = TerminalFormatter(**opts)
    return formatter
