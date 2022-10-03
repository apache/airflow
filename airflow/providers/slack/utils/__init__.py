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

from typing import Any

try:
    from airflow.utils.types import NOTSET
except ImportError:  # TODO: Remove when the provider has an Airflow 2.3+ requirement.

    class ArgNotSet:
        """Sentinel type for annotations, useful when None is not viable."""

    NOTSET = ArgNotSet()  # type: ignore[assignment]


def prefixed_extra_field(field: str, conn_type: str) -> str:
    """Get prefixed extra field name."""
    return f"extra__{conn_type}__{field}"


class ConnectionExtraConfig:
    """Helper class for rom Connection Extra.

    :param conn_type: Hook connection type.
    :param conn_id: Connection ID uses for appropriate error messages.
    :param extra: Connection extra dictionary.
    """

    def __init__(self, conn_type: str, conn_id: str | None = None, extra: dict[str, Any] | None = None):
        super().__init__()
        self.conn_type = conn_type
        self.conn_id = conn_id
        self.extra = extra or {}

    def get(self, field, default: Any = NOTSET):
        """Get specified field from Connection Extra.

        :param field: Connection extra field name.
        :param default: If specified then use as default value if field not present in Connection Extra.
        """
        prefixed_field = prefixed_extra_field(field, self.conn_type)
        if prefixed_field in self.extra and self.extra[prefixed_field] not in (None, ""):
            # Addition validation with non-empty required for connection which created in the UI
            # in Airflow 2.2. In these connections always present key-value pair for all prefixed extras
            # even if user do not fill this fields.
            # In additional fields from `wtforms.IntegerField` might contain None value.
            # E.g.: `{'extra__slackwebhook__proxy': '', 'extra__slackwebhook__timeout': None}`
            return self.extra[prefixed_field]
        elif field in self.extra:
            return self.extra[field]
        else:
            if default is NOTSET:
                raise KeyError(
                    f"Couldn't find {prefixed_field!r} or {field!r} "
                    f"in Connection ({self.conn_id!r}) Extra and no default value specified."
                )
            return default

    def getint(self, field, default: Any = NOTSET) -> Any:
        """Get specified field from Connection Extra and evaluate as integer.

        :param field: Connection extra field name.
        :param default: If specified then use as default value if field not present in Connection Extra.
        """
        value = self.get(field=field, default=default)
        if value != default:
            value = int(value)
        return value
