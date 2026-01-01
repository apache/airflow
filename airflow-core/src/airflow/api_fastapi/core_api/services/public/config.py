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

from fastapi import HTTPException, status, Request
from fastapi.responses import Response

from airflow.api_fastapi.common.types import Mimetype
from airflow.api_fastapi.core_api.datamodels.config import Config
from airflow.configuration import conf


def _check_expose_config(request: Request | None = None) -> bool:
    if conf.get("api", "expose_config").lower() == "non-sensitive-only":
        expose_config = True
    else:
        expose_config = conf.getboolean("api", "expose_config")

    if not expose_config:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Your Airflow administrator chose not to expose the configuration, most likely for security reasons.",
        )
    if request:
        user_agent = request.headers.get("user-agent", "")
        if "apache-airflow-ctl/" in user_agent:
            return False
    if conf.get("api", "expose_config").lower() == "non-sensitive-only":
        return False
    return True


def _response_based_on_accept(accept: Mimetype, config: Config):
    if accept == Mimetype.TEXT:
        return Response(content=config.text_format, media_type=Mimetype.TEXT)
    return config
