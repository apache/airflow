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
import sys
from copy import deepcopy
from typing import IO, TYPE_CHECKING

from airflow_breeze.utils.path_utils import in_autocomplete

if TYPE_CHECKING:
    from rich.console import Console

DEFAULT_COLUMNS = 129


def generating_command_images() -> bool:
    return "RECORD_BREEZE_TITLE" in os.environ or "regenerate-command-images" in sys.argv


def enable_recording_of_help_output(path: str, title: str | None, width: str | None, unique_id: str | None):
    import rich_click as click

    help_consoles: list[Console] = []

    if not title:
        title = "Breeze screenshot"
    if not width:
        width_int = DEFAULT_COLUMNS
    else:
        width_int = int(width)

    def save_output_as_svg():
        for console in help_consoles:
            console.save_svg(path=path, title=title, unique_id=unique_id)

    atexit.register(save_output_as_svg)
    click.rich_click.MAX_WIDTH = width_int
    click.formatting.FORCED_WIDTH = width_int - 2  # type: ignore[attr-defined]
    click.rich_click.COLOR_SYSTEM = "standard"
    # monkeypatch rich_click console to record help
    import rich_click

    original_create_console = rich_click.rich_help_formatter.create_console

    from rich_click import RichHelpConfiguration

    def create_recording_console(config: RichHelpConfiguration, file: IO[str] | None = None) -> Console:
        recording_config = deepcopy(config)
        recording_config.width = width_int
        recording_config.force_terminal = True
        recording_console = original_create_console(recording_config, file)
        recording_console.record = True
        help_consoles.append(recording_console)
        return recording_console

    rich_click.rich_help_formatter.create_console = create_recording_console


output_file = os.environ.get("RECORD_BREEZE_OUTPUT_FILE")


if output_file and not in_autocomplete():
    enable_recording_of_help_output(
        path=output_file,
        title=os.environ.get("RECORD_BREEZE_TITLE"),
        width=os.environ.get("RECORD_BREEZE_WIDTH"),
        unique_id=os.environ.get("RECORD_BREEZE_UNIQUE_ID"),
    )
else:
    try:
        import click

        columns = os.get_terminal_size().columns
        click.formatting.FORCED_WIDTH = columns - 2 if columns else DEFAULT_COLUMNS
    except OSError:
        pass
