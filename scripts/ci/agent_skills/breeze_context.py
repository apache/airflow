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

"""Context detection helpers for the agent-skills PoC."""

from __future__ import annotations

import os


def get_context() -> str:
    """Return the current execution context.

    For the first PoC, we keep detection intentionally simple:
    - if AIRFLOW_BREEZE_CONTAINER=1, treat it as Breeze
    - otherwise treat it as host

    :return: "breeze" if inside Breeze, otherwise "host"
    """
    if os.environ.get("AIRFLOW_BREEZE_CONTAINER") == "1":
        return "breeze"
    return "host"
