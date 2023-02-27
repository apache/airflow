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
import types
from typing import Callable

from airflow.decorators import python_task
from airflow.utils.setup_teardown import SetupTeardownContext


def setup_task(python_callable: Callable) -> Callable:
    # Using FunctionType here since _TaskDecorator is also a callable
    if isinstance(python_callable, types.FunctionType):
        python_callable = python_task(python_callable)

    @functools.wraps(python_callable)
    def wrapper(*args, **kwargs):
        with SetupTeardownContext.setup():
            return python_callable(*args, **kwargs)

    return wrapper


def teardown_task(python_callable: Callable) -> Callable:
    # Using FunctionType here since _TaskDecorator is also a callable
    if isinstance(python_callable, types.FunctionType):
        python_callable = python_task(python_callable)

    @functools.wraps(python_callable)
    def wrapper(*args, **kwargs) -> Callable:
        with SetupTeardownContext.teardown():
            return python_callable(*args, **kwargs)

    return wrapper
