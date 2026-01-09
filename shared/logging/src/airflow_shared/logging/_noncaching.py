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

import os
from typing import BinaryIO, TextIO, TypeVar

__all__ = [
    "make_file_io_non_caching",
]

_IO = TypeVar("_IO", TextIO, BinaryIO)


def make_file_io_non_caching(io: _IO) -> _IO:
    try:
        fd = io.fileno()
        os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
    except Exception:
        # in case either file descriptor cannot be retrieved or fadvise is not available
        # we should simply return the wrapper retrieved by FileHandler's open method
        # the advice to the kernel is just an advice and if we cannot give it, we won't
        pass
    return io
