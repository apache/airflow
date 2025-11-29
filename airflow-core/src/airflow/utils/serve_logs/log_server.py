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
"""Log server written in FastAPI."""

from __future__ import annotations

import logging
import os
from functools import cache
from typing import cast

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.staticfiles import StaticFiles
from jwt.exceptions import (
    ExpiredSignatureError,
    ImmatureSignatureError,
    InvalidAudienceError,
    InvalidIssuedAtError,
    InvalidSignatureError,
)

from airflow.api_fastapi.auth.tokens import JWTValidator, get_signing_key
from airflow.configuration import conf
from airflow.utils.docs import get_docs_url
from airflow.utils.module_loading import import_string

logger = logging.getLogger(__name__)


class JWTAuthStaticFiles(StaticFiles):
    """StaticFiles with JWT authentication."""

    # reference from https://github.com/fastapi/fastapi/issues/858#issuecomment-876564020

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    async def __call__(self, scope, receive, send) -> None:
        request = Request(scope, receive)
        await self.validate_jwt_token(request)
        await super().__call__(scope, receive, send)

    async def validate_jwt_token(self, request: Request):
        # we get the signer from the app state instead of creating a new instance for each request
        signer = cast("JWTValidator", request.app.state.signer)
        try:
            auth = request.headers.get("Authorization")
            if auth is None:
                logger.warning("The Authorization header is missing: %s.", request.headers)
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN, detail="Authorization header missing"
                )
            payload = await signer.avalidated_claims(auth)
            token_filename = payload.get("filename")
            # Extract filename from url path
            request_filename = request.url.path.lstrip("/log/")
            if token_filename is None:
                logger.warning("The payload does not contain 'filename' key: %s.", payload)
                raise HTTPException(status_code=status.HTTP_403_FORBIDDEN)
            if token_filename != request_filename:
                logger.warning(
                    "The payload log_relative_path key is different than the one in token:"
                    "Request path: %s. Token path: %s.",
                    request_filename,
                    token_filename,
                )
                raise HTTPException(status_code=status.HTTP_403_FORBIDDEN)
        except HTTPException:
            raise
        except InvalidAudienceError:
            logger.warning("Invalid audience for the request", exc_info=True)
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN)
        except InvalidSignatureError:
            logger.warning("The signature of the request was wrong", exc_info=True)
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN)
        except ImmatureSignatureError:
            logger.warning("The signature of the request was sent from the future", exc_info=True)
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN)
        except ExpiredSignatureError:
            logger.warning(
                "The signature of the request has expired. Make sure that all components "
                "in your system have synchronized clocks. "
                "See more at %s",
                get_docs_url("configurations-ref.html#secret-key"),
                exc_info=True,
            )
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN)
        except InvalidIssuedAtError:
            logger.warning(
                "The request was issued in the future. Make sure that all components "
                "in your system have synchronized clocks. "
                "See more at %s",
                get_docs_url("configurations-ref.html#secret-key"),
                exc_info=True,
            )
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN)
        except Exception:
            logger.warning("Unknown error", exc_info=True)
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN)


def create_app():
    leeway = conf.getint("webserver", "log_request_clock_grace", fallback=30)
    log_directory = os.path.expanduser(conf.get("logging", "BASE_LOG_FOLDER"))
    log_config_class = conf.get("logging", "logging_config_class")
    if log_config_class:
        logger.info("Detected user-defined logging config. Attempting to load %s", log_config_class)
        try:
            logging_config = import_string(log_config_class)
            try:
                base_log_folder = logging_config["handlers"]["task"]["base_log_folder"]
            except KeyError:
                base_log_folder = None
            if base_log_folder is not None:
                log_directory = base_log_folder
                logger.info(
                    "Successfully imported user-defined logging config. FastAPI App will serve log from %s",
                    log_directory,
                )
            else:
                logger.warning(
                    "User-defined logging config does not specify 'base_log_folder'. "
                    "FastAPI App will use default log directory %s",
                    base_log_folder,
                )
        except Exception as e:
            raise ImportError(f"Unable to load {log_config_class} due to error: {e}")

    fastapi_app = FastAPI()
    fastapi_app.state.signer = JWTValidator(
        issuer=None,
        secret_key=get_signing_key("api", "secret_key"),
        algorithm="HS512",
        leeway=leeway,
        audience="task-instance-logs",
    )

    fastapi_app.mount(
        "/log",
        JWTAuthStaticFiles(directory=log_directory, html=False),
        name="serve_logs",
    )

    return fastapi_app


@cache
def get_app() -> FastAPI:
    """Create a cached FastAPI app instance."""
    return create_app()
