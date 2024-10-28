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

from typing import TYPE_CHECKING

from airflow_breeze.global_constants import (
    DEFAULT_UV_HTTP_TIMEOUT,
    DEFAULT_WSL2_HTTP_TIMEOUT,
)

if TYPE_CHECKING:
    from airflow_breeze.params.common_build_params import CommonBuildParams
from airflow_breeze.utils.platforms import is_wsl2


def get_uv_timeout(build_params: CommonBuildParams) -> int:
    """
    Get the timeout for the uvicorn server. We do not want to change the default value to not slow down
    the --help and command line in general and also it might be useful to give escape hatch in case our
    WSL1 detection is wrong (it will fail default --use-uv build, but you will be able to skip the check by
    manually specifying --uv-http-timeout or --no-use-uv). So we only check for wsl2 when default value is
    used and when uv is enabled.
    """

    if build_params.uv_http_timeout != DEFAULT_UV_HTTP_TIMEOUT:
        # a bit of hack: if you specify 300 in command line it will also be overridden in case of WSL2
        # but this is a corner case
        return build_params.uv_http_timeout
    if is_wsl2():
        return DEFAULT_WSL2_HTTP_TIMEOUT
    return build_params.uv_http_timeout
