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

from contextlib import contextmanager


class SetupTeardownContext:
    """Track whether the next added task is a setup or teardown task"""

    is_setup: bool = False
    is_teardown: bool = False

    @classmethod
    @contextmanager
    def setup(cls):
        cls.is_setup = True
        yield
        cls.is_setup = False

    @classmethod
    @contextmanager
    def teardown(cls):
        cls.is_teardown = True
        yield
        cls.is_teardown = False
