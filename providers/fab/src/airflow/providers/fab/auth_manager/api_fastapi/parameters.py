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

import logging

from fastapi import Query

from airflow.providers.common.compat.sdk import conf

log = logging.getLogger(__name__)


def get_effective_limit(default: int = 100):
    """
    Return a FastAPI dependency that enforces API page limit rules.

    :param default: Default limit if not provided by client.
    """

    def _limit(
        limit: int = Query(
            default,
            ge=0,
        ),
    ) -> int:
        max_val = conf.getint("api", "maximum_page_limit")
        fallback = conf.getint("api", "fallback_page_limit")

        if limit == 0:
            return fallback
        if limit > max_val:
            log.warning(
                "The limit param value %s passed in API exceeds the configured maximum page limit %s",
                limit,
                max_val,
            )
            return max_val
        return limit

    return _limit
