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

try:
    from airflow.utils.module_loading import (
        import_string,
        iter_namespace,
        qualname,
    )

except ImportError:
    from airflow.sdk.module_loading import import_string, iter_namespace, qualname

try:
    # This function was not available in Airflow 3.0/3.1 in module_loading, but it's good to keep it in the
    # same shared module - good for reuse
    from airflow.sdk._shared.module_loading import is_valid_dotpath

except ImportError:
    # TODO: Remove it when Airflow 3.2.0 is the minimum version
    def is_valid_dotpath(path: str) -> bool:
        import re

        if not isinstance(path, str):
            return False
        pattern = r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$"
        return bool(re.match(pattern, path))


__all__ = [
    "import_string",
    "qualname",
    "iter_namespace",
]
