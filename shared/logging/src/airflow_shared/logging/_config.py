#
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

import structlog.processors

OLD_DEFAULT_LOG_FORMAT = "[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
OLD_DEFAULT_COLOR_LOG_FORMAT = (
    "[%(blue)s%(asctime)s%(reset)s] {%(blue)s%(filename)s:%(reset)s%(lineno)d} "
    "%(log_color)s%(levelname)s%(reset)s - %(log_color)s%(message)s%(reset)s"
)


# This doesn't load the values from config, to avoid a cross dependency between shared logging and shared
# config modules.
def translate_config_values(
    log_format: str, callsite_params: list[str]
) -> tuple[str, tuple[structlog.processors.CallsiteParameter, ...]]:
    if log_format == OLD_DEFAULT_LOG_FORMAT:
        # It's the default, use the coloured version by default. This will automatically not put color codes
        # if we're not a tty, or if colors are disabled
        log_format = OLD_DEFAULT_COLOR_LOG_FORMAT

    # This will raise an exception if the value isn't valid
    params_out = tuple(
        getattr(structlog.processors.CallsiteParameter, p, None) or structlog.processors.CallsiteParameter(p)
        for p in filter(None, callsite_params)
    )

    return log_format, params_out
