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
from collections.abc import Callable
from typing import Any


def dry_run_message_wrap(func: Callable) -> Callable:
    """Wrap CLI commands to display dry-run messages."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # detect args object (first positional or keyword)
        if args:
            arg_obj = args[0]
        else:
            arg_obj = kwargs.get("args")

        dry_run = getattr(arg_obj, "dry_run", False)

        if dry_run:
            print(
                "Performing dry run. "
                "It will check the connection to Keycloak but won't create any resources.\n"
            )

        result = func(*args, **kwargs)

        if dry_run:
            print("Dry run completed.")

        return result

    return wrapper


def dry_run_preview(preview_func: Callable[..., None]) -> Callable:
    """
    Handle dry-run preview logic for create functions.

    When dry_run=True, executes the preview function and returns early.
    Otherwise, proceeds with normal execution without passing dry_run
    to the decorated function.

    :param preview_func: Function to call for previewing what would be created.
                        Should accept the same arguments as the decorated function.
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            # Extract dry_run from kwargs (default to False if not provided)
            dry_run = kwargs.pop("_dry_run", False)

            if dry_run:
                # Pass args and remaining kwargs to preview function
                preview_func(*args, **kwargs)
                return None

            # Pass args and remaining kwargs to actual function
            return func(*args, **kwargs)

        return wrapper

    return decorator
