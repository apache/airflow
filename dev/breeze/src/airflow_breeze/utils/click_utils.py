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

from typing import TYPE_CHECKING

try:
    from rich_click import RichCommand as _BaseCommand, RichGroup as _BaseGroup
except ImportError:
    from click import (  # type: ignore[assignment]
        Command as _BaseCommand,
        Group as _BaseGroup,
    )

if TYPE_CHECKING:
    import click


class BreezeCommand(_BaseCommand):
    """Breeze CLI command that automatically prints reproduction instructions in CI."""

    def invoke(self, ctx: click.Context) -> None:
        try:
            return super().invoke(ctx)
        finally:
            from airflow_breeze.utils.reproduce_ci import maybe_print_reproduction

            maybe_print_reproduction(ctx)


class BreezeGroup(_BaseGroup):
    command_class = BreezeCommand
