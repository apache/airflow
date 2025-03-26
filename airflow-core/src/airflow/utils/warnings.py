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

import warnings
from collections.abc import Generator
from contextlib import contextmanager


@contextmanager
def capture_with_reraise() -> Generator[list[warnings.WarningMessage], None, None]:
    """Capture warnings in context and re-raise it on exit from the context manager."""
    captured_warnings = []
    try:
        with warnings.catch_warnings(record=True) as captured_warnings:
            yield captured_warnings
    finally:
        if captured_warnings:
            for cw in captured_warnings:
                warnings.warn_explicit(
                    message=cw.message,
                    category=cw.category,
                    filename=cw.filename,
                    lineno=cw.lineno,
                    source=cw.source,
                )
