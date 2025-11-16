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

import gc
from functools import wraps


def with_gc_freeze(func):
    """
    Freeze the GC before executing the function and unfreeze it after execution.

    This is done to prevent memory increase due to COW (Copy-on-Write) by moving all
    existing objects to the permanent generation before forking the process. After the
    function executes, unfreeze is called to ensure there is no impact on gc operations
    in the original running process.

    Ref: https://docs.python.org/3/library/gc.html#gc.freeze
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        gc.freeze()
        try:
            return func(*args, **kwargs)
        finally:
            gc.unfreeze()

    return wrapper
