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
"""Provides lineage support functions."""

from __future__ import annotations

from functools import wraps
from typing import Callable, TypeVar, cast

from airflow.sdk.execution_time.lineage import (
    AUTO,
    PIPELINE_INLETS,
    PIPELINE_OUTLETS,
    _get_backend as get_backend,
    apply_lineage as _apply_lineage,
    prepare_lineage as _prepare_lineage,
)

__all__ = ["AUTO", "PIPELINE_INLETS", "PIPELINE_OUTLETS", "apply_lineage", "get_backend", "prepare_lineage"]

T = TypeVar("T", bound=Callable)


def apply_lineage(func: T) -> T:
    """
    Conditionally send lineage to the backend.

    Saves the lineage to XCom and if configured to do so sends it
    to the backend.
    """

    @wraps(func)
    def wrapper(self, context, *args, **kwargs):
        ret_val = func(self, context, *args, **kwargs)
        _apply_lineage(context, self.log)
        return ret_val

    return cast(T, wrapper)


def prepare_lineage(func: T) -> T:
    """
    Prepare the lineage inlets and outlets.

    Inlets can be:

    * "auto" -> picks up any outlets from direct upstream tasks that have outlets defined, as such that
      if A -> B -> C and B does not have outlets but A does, these are provided as inlets.
    * "list of task_ids" -> picks up outlets from the upstream task_ids
    * "list of datasets" -> manually defined list of dataset
    """

    @wraps(func)
    def wrapper(self, context, *args, **kwargs):
        _prepare_lineage(context, self.log)
        return func(self, context, *args, **kwargs)

    return cast(T, wrapper)
