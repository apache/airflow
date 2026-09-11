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

# ``AccessView.IMPORT_ERRORS_ALL`` and ``AccessView.AUDIT_LOGS_ALL`` were added in
# Airflow 3.4.0, and providers are released independently of core. ``None`` signals the
# view is unavailable on the running core and callers should skip mapping it.
try:
    from airflow.api_fastapi.auth.managers.models.resource_details import AccessView

    IMPORT_ERRORS_ALL_ACCESS_VIEW: AccessView | None = getattr(AccessView, "IMPORT_ERRORS_ALL", None)
    AUDIT_LOGS_ALL_ACCESS_VIEW: AccessView | None = getattr(AccessView, "AUDIT_LOGS_ALL", None)
except ImportError:
    IMPORT_ERRORS_ALL_ACCESS_VIEW = None
    AUDIT_LOGS_ALL_ACCESS_VIEW = None

__all__ = ["AUDIT_LOGS_ALL_ACCESS_VIEW", "IMPORT_ERRORS_ALL_ACCESS_VIEW"]
