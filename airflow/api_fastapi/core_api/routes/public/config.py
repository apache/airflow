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

import textwrap

from fastapi import Depends, HTTPException, status
from fastapi.responses import Response

from airflow.api_fastapi.common.headers import HeaderAcceptJsonOrText
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.common.types import Mimetype
from airflow.api_fastapi.core_api.datamodels.config import (
    Config,
    ConfigOption,
    ConfigSection,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import requires_access_configuration
from airflow.configuration import conf

text_example_response_for_get_config_value = {
    Mimetype.TEXT: {
        "schema": {
            "type": "string",
            "example": textwrap.dedent(
                """\
    [core]
    dags_folder = /opt/airflow/dags
    base_log_folder = /opt/airflow/logs
    """
            ),
        }
    }
}

text_example_response_for_get_config = {
    Mimetype.TEXT: {
        "schema": {
            "type": "string",
            "example": textwrap.dedent(
                """\
    [core]
    dags_folder = /opt/airflow/dags
    base_log_folder = /opt/airflow/logs

    [smtp]
    smtp_host = localhost
    smtp_mail_from = airflow@example.com
    """
            ),
        },
    }
}


def _check_expose_config() -> bool:
    display_sensitive: bool | None = None
    if conf.get("webserver", "expose_config").lower() == "non-sensitive-only":
        expose_config = True
        display_sensitive = False
    else:
        expose_config = conf.getboolean("webserver", "expose_config")
        display_sensitive = True

    if not expose_config:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Your Airflow administrator chose not to expose the configuration, most likely for security reasons.",
        )
    return display_sensitive


def _response_based_on_accept(accept: Mimetype, config: Config):
    if accept == Mimetype.TEXT:
        return Response(content=config.text_format, media_type=Mimetype.TEXT)
    return config


config_router = AirflowRouter(tags=["Config"], prefix="/config")


@config_router.get(
    "",
    responses={
        **create_openapi_http_exception_doc(
            [
                status.HTTP_404_NOT_FOUND,
                status.HTTP_406_NOT_ACCEPTABLE,
            ]
        ),
        status.HTTP_200_OK: {
            "description": "Successful Response",
            "content": text_example_response_for_get_config,
        },
    },
    response_model=Config,
    dependencies=[Depends(requires_access_configuration("GET"))],
)
def get_config(
    accept: HeaderAcceptJsonOrText,
    section: str | None = None,
):
    display_sensitive = _check_expose_config()

    if section and not conf.has_section(section):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Section {section} not found.",
        )
    conf_dict = conf.as_dict(display_source=False, display_sensitive=display_sensitive)

    if section:
        conf_section_value = conf_dict[section]
        conf_dict.clear()
        conf_dict[section] = conf_section_value

    config = Config(
        sections=[
            ConfigSection(
                name=section, options=[ConfigOption(key=key, value=value) for key, value in options.items()]
            )
            for section, options in conf_dict.items()
        ]
    )
    return _response_based_on_accept(accept, config)


@config_router.get(
    "/section/{section}/option/{option}",
    responses={
        **create_openapi_http_exception_doc(
            [
                status.HTTP_404_NOT_FOUND,
                status.HTTP_406_NOT_ACCEPTABLE,
            ]
        ),
        status.HTTP_200_OK: {
            "description": "Successful Response",
            "content": text_example_response_for_get_config_value,
        },
    },
    response_model=Config,
    dependencies=[Depends(requires_access_configuration("GET"))],
)
def get_config_value(
    section: str,
    option: str,
    accept: HeaderAcceptJsonOrText,
):
    _check_expose_config()

    if not conf.has_option(section, option):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Option [{section}/{option}] not found.",
        )

    if (section.lower(), option.lower()) in conf.sensitive_config_values:
        value = "< hidden >"
    else:
        value = conf.get(section, option)

    config = Config(sections=[ConfigSection(name=section, options=[ConfigOption(key=option, value=value)])])
    return _response_based_on_accept(accept, config)
