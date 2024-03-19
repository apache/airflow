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
import os
from contextlib import contextmanager

import psutil


def _get_process_memory():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss


def _human_readable_size(size, decimal_places=3):
    for unit in ["B", "KiB", "MiB", "GiB", "TiB"]:  # noqa: B007
        if size < 1024.0:
            break
        size /= 1024.0
    return f"{size:.{decimal_places}f}{unit}"


class TraceMemoryResult:
    """Trace results of memory,"""

    def __init__(self):
        self.before = 0
        self.after = 0
        self.value = 0


@contextmanager
def trace_memory(human_readable=True, gc_collect=False):
    """
    Calculates the amount of difference in free memory before and after script execution.

    In other words, how much data the code snippet has used up memory.

    :param human_readable: If yes, the result will be displayed in human readable units.
        If no, the result will be displayed as bytes.
    :param gc_collect: If yes, the garbage collector will be started before checking used memory.
    """
    if gc_collect:
        gc.collect()
    before = _get_process_memory()
    result = TraceMemoryResult()
    try:
        yield result
    finally:
        if gc_collect:
            gc.collect()
        after = _get_process_memory()
        diff = after - before

        result.before = before
        result.after = after
        result.value = diff

        if human_readable:
            human_diff = _human_readable_size(diff)
            print(f"Memory: {human_diff}")
        else:
            print(f"Memory: {diff} bytes")


if __name__ == "__main__":
    # Example:

    with trace_memory():
        import airflow  # noqa: F401
