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

import atexit
import os

import rich
from rich.console import Console

from airflow_breeze.utils.path_utils import in_autocomplete

help_console: Console | None = None

DEFAULT_COLUMNS = 129


def generating_command_images() -> bool:
    return 'RECORD_BREEZE_TITLE' in os.environ


def enable_recording_of_help_output(path: str, title: str | None, width: str | None):
    import rich_click as click

    if not title:
        title = "Breeze screenshot"
    if not width:
        width_int = DEFAULT_COLUMNS
    else:
        width_int = int(width)

    def save_ouput_as_svg():
        if help_console:
            help_console.save_svg(path=path, title=title)

    class RecordingConsole(rich.console.Console):
        def __init__(self, **kwargs):
            kwargs["force_terminal"] = True
            kwargs["width"] = width_int
            super().__init__(record=True, **kwargs)
            global help_console
            help_console = self

    atexit.register(save_ouput_as_svg)
    click.rich_click.MAX_WIDTH = width_int
    click.formatting.FORCED_WIDTH = width_int - 2  # type: ignore[attr-defined]
    click.rich_click.COLOR_SYSTEM = "standard"
    # monkeypatch rich_click console to record help (rich_click does not allow passing extra args to console)
    click.rich_click.Console = RecordingConsole  # type: ignore[misc]


output_file = os.environ.get('RECORD_BREEZE_OUTPUT_FILE')


if output_file and not in_autocomplete():
    enable_recording_of_help_output(
        path=output_file,
        title=os.environ.get('RECORD_BREEZE_TITLE'),
        width=os.environ.get('RECORD_BREEZE_WIDTH'),
    )
else:
    try:
        import click

        columns = os.get_terminal_size().columns
        click.formatting.FORCED_WIDTH = columns - 2 if columns else DEFAULT_COLUMNS
    except OSError:
        pass
