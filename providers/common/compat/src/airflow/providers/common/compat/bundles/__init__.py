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

import inspect
from typing import Any

import structlog

from airflow.dag_processing.bundles.base import BaseDagBundle as _BaseDagBundle

if isinstance(inspect.getattr_static(_BaseDagBundle, "_log", None), property):
    # Modern Airflow already provides BaseDagBundle._log – use it as-is.
    BaseDagBundle = _BaseDagBundle
else:
    # Older Airflow: inject _log via a transparent subclass so that all
    # bundle implementations can use self._log without version checks.
    # Remove this branch once every Airflow version the providers support has BaseDagBundle._log.
    class BaseDagBundle(_BaseDagBundle):  # type: ignore[no-redef]
        """Drop-in replacement for BaseDagBundle with a back-filled _log property."""

        def _get_log_context(self) -> dict[str, Any]:
            """Return extra structlog context fields for this bundle. Override in subclasses."""
            return {}

        @property
        def _log(self) -> Any:
            """Lazy structlog logger bound with common bundle context plus subclass-specific fields."""
            if not hasattr(self, "_compat_structlog"):
                self._compat_structlog = structlog.get_logger(type(self).__module__).bind(
                    bundle_name=self.name, version=self.version, **self._get_log_context()
                )
            return self._compat_structlog

        @_log.setter
        def _log(self, value) -> None:
            self._compat_structlog = value


__all__ = ["BaseDagBundle"]
