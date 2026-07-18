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

# ``AccessView.IMPORT_ERRORS_ALL`` was added to Airflow core in 3.4.0. Auth-manager
# providers are released independently and may run against an older core that does
# not yet define the member (or, on Airflow 2.x, does not ship ``api_fastapi`` at
# all). Resolve it defensively so importing a provider that references the dedicated
# "all import errors" view does not raise on older core: ``None`` signals the view
# is unavailable and callers should skip mapping it.
try:
    from airflow.api_fastapi.auth.managers.models.resource_details import AccessView

    IMPORT_ERRORS_ALL_ACCESS_VIEW: AccessView | None = getattr(AccessView, "IMPORT_ERRORS_ALL", None)
except ImportError:
    IMPORT_ERRORS_ALL_ACCESS_VIEW = None

__all__ = ["IMPORT_ERRORS_ALL_ACCESS_VIEW"]
