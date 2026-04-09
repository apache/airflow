#
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

from collections.abc import Callable
from typing import TYPE_CHECKING, TypeVar

from airflow.utils.deprecation_tools import add_deprecated_classes

if TYPE_CHECKING:
    from airflow.sdk.bases.decorator import KeywordParameters, determine_kwargs  # noqa: F401

__all__ = ["make_kwargs_callable"]

R = TypeVar("R")

add_deprecated_classes(
    {
        __name__: {
            "KeywordParameters": "airflow.sdk.bases.decorator.KeywordParameters",
            "determine_kwargs": "airflow.sdk.bases.decorator.determine_kwargs",
        },
    },
    package=__name__,
)


def make_kwargs_callable(func: Callable[..., R]) -> Callable[..., R]:
    """
    Create a new callable that only forwards necessary arguments from any provided input.

    Make a new callable that can accept any number of positional or keyword arguments
    but only forwards those required by the given callable func.
    """
    import functools

    from airflow.sdk.bases.decorator import determine_kwargs

    @functools.wraps(func)
    def kwargs_func(*args, **kwargs):
        kwargs = determine_kwargs(func, args, kwargs)
        return func(*args, **kwargs)

    return kwargs_func
