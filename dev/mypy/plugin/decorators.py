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

import copy
import functools

from mypy.nodes import ARG_NAMED_OPT
from mypy.plugin import FunctionContext, Plugin
from mypy.types import CallableType, NoneType, UnionType

TYPED_DECORATORS = {
    "airflow.utils.session.provide_session": [],
    "airflow.providers.google.cloud.hooks.dataflow._fallback_to_project_id_from_variables": ["project_id"],
    "fallback_to_default_project_id of GoogleBaseHook": ["project_id"],
    "provide_gcp_credential_file of GoogleBaseHook": [],
}


class TypedDecoratorPlugin(Plugin):
    """Mypy plugin for typed decorators."""

    def get_function_hook(self, fullname: str):
        """Check for known typed decorators by name."""
        if fullname in TYPED_DECORATORS:
            return functools.partial(
                _analyze_decorator,
                provided_arguments=TYPED_DECORATORS[fullname],
            )
        return None


def _analyze_decorator(function_ctx: FunctionContext, provided_arguments: list[str]):
    if not isinstance(function_ctx.arg_types[0][0], CallableType):
        return function_ctx.default_return_type
    if not isinstance(function_ctx.default_return_type, CallableType):
        return function_ctx.default_return_type
    return _change_decorator_function_type(
        function_ctx.arg_types[0][0],
        function_ctx.default_return_type,
        provided_arguments,
    )


def _change_decorator_function_type(
    decorated: CallableType,
    decorator: CallableType,
    provided_arguments: list[str],
) -> CallableType:
    decorator.arg_kinds = decorated.arg_kinds
    decorator.arg_names = decorated.arg_names

    # Mark provided arguments as optional
    decorator.arg_types = copy.copy(decorated.arg_types)
    for argument in provided_arguments:
        try:
            index = decorated.arg_names.index(argument)
        except ValueError:
            continue
        decorated_type = decorated.arg_types[index]
        decorator.arg_types[index] = UnionType.make_union([decorated_type, NoneType()])
        decorated.arg_kinds[index] = ARG_NAMED_OPT

    return decorator


def plugin(version: str):
    """Mypy plugin entrypoint."""
    return TypedDecoratorPlugin
