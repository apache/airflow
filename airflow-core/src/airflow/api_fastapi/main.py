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
import sys

# Mark this as a server context before any airflow imports
# This ensures plugins loaded at import time get the correct secrets backend chain
os.environ["_AIRFLOW_PROCESS_CONTEXT"] = "server"

# Warn if PYTHONASYNCIODEBUG or PYTHONDEVMODE is set on Python 3.12+ (see Airflow issue #61214)
if sys.version_info >= (3, 12) and (
    os.environ.get("PYTHONASYNCIODEBUG") == "1" or os.environ.get("PYTHONDEVMODE") == "1"
):
    import warnings

    warnings.warn(
        "PYTHONASYNCIODEBUG or PYTHONDEVMODE detected on Python 3.12+: "
        "The API server may crash due to uvloop incompatibility with asyncio debug mode. "
        "See https://github.com/apache/airflow/issues/61214 for details.",
        RuntimeWarning,
        stacklevel=2,
    )

from airflow.api_fastapi.app import cached_app

# There is no way to pass the apps to this file from Airflow CLI
# because fastapi dev command does not accept any additional arguments
# so environment variable is being used to pass it
app = cached_app(apps=os.environ.get("AIRFLOW_API_APPS", "all"))
