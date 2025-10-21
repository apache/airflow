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

from airflow.providers.common.compat._compat_utils import create_module_getattr

_IMPORT_MAP: dict[str, str | tuple[str, ...]] = {
    "write_python_script": (
        "airflow.providers.standard.utils.python_virtualenv",
        "airflow.utils.python_virtualenv",
    ),
    "prepare_virtualenv": (
        "airflow.providers.standard.utils.python_virtualenv",
        "airflow.utils.python_virtualenv",
    ),
}

__getattr__ = create_module_getattr(import_map=_IMPORT_MAP)

__all__ = sorted(_IMPORT_MAP.keys())
