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

from dataclasses import dataclass
from typing import Any

import click

from airflow_breeze.utils.cache import (
    check_if_values_allowed,
    read_and_validate_value_from_cache,
    read_from_cache_file,
    write_to_cache_file,
)
from airflow_breeze.utils.console import console
from airflow_breeze.utils.recording import output_file_for_recording


class BetterChoice(click.Choice):
    """
    Nicer formatted choice class for click. We have a lot of parameters sometimes, and formatting
    them without spaces causes ugly artifacts as the words are broken. This one adds spaces so
    that when the long list of choices does not wrap on words.
    """

    name = "BetterChoice"

    def get_metavar(self, param) -> str:
        choices_str = " | ".join(self.choices)
        # Use curly braces to indicate a required argument.
        if param.required and param.param_type_name == "argument":
            return f"{{{choices_str}}}"

        if param.param_type_name == "argument" and param.nargs == -1:
            # avoid double [[ for multiple args
            return f"{choices_str}"

        # Use square braces to indicate an option or optional argument.
        return f"[{choices_str}]"


@dataclass
class CacheableDefault:
    value: Any

    def __repr__(self):
        return self.value


class CacheableChoice(click.Choice):
    """
    This class implements caching of values from the last use.
    """

    def convert(self, value, param, ctx):
        param_name = param.envvar if param.envvar else param.name.upper()
        if isinstance(value, CacheableDefault):
            is_cached, new_value = read_and_validate_value_from_cache(param_name, value.value)
            if not is_cached:
                console.print(
                    f"\n[bright_blue]Default value of {param.name} " f"parameter {new_value} used.[/]\n"
                )
        else:
            allowed, allowed_values = check_if_values_allowed(param_name, value)
            if allowed:
                new_value = value
                write_to_cache_file(param_name, new_value, check_allowed_values=False)
            else:
                new_value = allowed_values[0]
                console.print(
                    f"\n[yellow]The value {value} is not allowed for parameter {param.name}. "
                    f"Setting default value to {new_value}"
                )
                write_to_cache_file(param_name, new_value, check_allowed_values=False)
        return super().convert(new_value, param, ctx)

    def get_metavar(self, param) -> str:
        param_name = param.envvar if param.envvar else param.name.upper()
        current_value = (
            read_from_cache_file(param_name) if not output_file_for_recording else param.default.value
        )
        if not current_value:
            current_choices = self.choices
        else:
            current_choices = [
                f">{choice}<" if choice == current_value else choice for choice in self.choices
            ]
        choices_str = " | ".join(current_choices)
        # Use curly braces to indicate a required argument.
        if param.required and param.param_type_name == "argument":
            return f"{{{choices_str}}}"

        if param.param_type_name == "argument" and param.nargs == -1:
            # avoid double [[ for multiple args
            return f"{choices_str}"

        # Use square braces to indicate an option or optional argument.
        return f"[{choices_str}]"

    def __init__(self, choices, case_sensitive: bool = True) -> None:
        super().__init__(choices=choices, case_sensitive=case_sensitive)
