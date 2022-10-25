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
from typing import Any

from airflow.utils.types import NOTSET


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
        backcompat_key = f"extra__{self.conn_type}__{field}"
        if self.extra.get(field) not in (None, ""):
            if self.extra.get(backcompat_key) not in (None, ""):
                warnings.warn(
                    f"Conflicting params `{field}` and `{backcompat_key}` found in extras for conn "
                    f"{self.conn_id}. Using value for `{field}`.  Please ensure this is the correct value "
                    f"and remove the backcompat key `{backcompat_key}`."
                )
            return self.extra[field]
        elif backcompat_key in self.extra and self.extra[backcompat_key] not in (None, ""):
            # Addition validation with non-empty required for connection which created in the UI
            # in Airflow 2.2. In these connections always present key-value pair for all prefixed extras
            # even if user do not fill this fields.
            # In additional fields from `wtforms.IntegerField` might contain None value.
            # E.g.: `{'extra__slackwebhook__proxy': '', 'extra__slackwebhook__timeout': None}`
            # From Airflow 2.3, using the prefix is no longer required.
            return self.extra[backcompat_key]
        else:
            if default is NOTSET:
                raise KeyError(
                    f"Couldn't find {backcompat_key!r} or {field!r} "
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
