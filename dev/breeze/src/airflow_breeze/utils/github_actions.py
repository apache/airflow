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

from typing import Any

from rich.markup import escape

from airflow_breeze.utils.console import get_console


def get_ga_output(name: str, value: Any) -> str:
    output_name = name.replace('_', '-')
    printed_value = str(value).lower() if isinstance(value, bool) else value
    get_console().print(f"[info]{output_name}[/] = [green]{escape(str(printed_value))}[/]")
    return f"::set-output name={output_name}::{printed_value}"
