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
"""Serve logs process"""
from __future__ import annotations

import collections
import logging
import os
import socket

import gunicorn.app.base
from flask import Flask, abort, request, send_from_directory
from jwt.exceptions import (
    ExpiredSignatureError,
    ImmatureSignatureError,
    InvalidAudienceError,
    InvalidIssuedAtError,
    InvalidSignatureError,
)
from setproctitle import setproctitle

from airflow.configuration import conf
from airflow.utils.docs import get_docs_url
from airflow.utils.jwt_signer import JWTSigner

logger = logging.getLogger(__name__)


def create_app():
    flask_app = Flask(__name__, static_folder=None)
    expiration_time_in_seconds = conf.getint("webserver", "log_request_clock_grace", fallback=30)
    log_directory = os.path.expanduser(conf.get("logging", "BASE_LOG_FOLDER"))

    signer = JWTSigner(
        secret_key=conf.get("webserver", "secret_key"),
        expiration_time_in_seconds=expiration_time_in_seconds,
        audience="task-instance-logs",
    )

    # Prevent direct access to the logs port
    @flask_app.before_request
    def validate_pre_signed_url():
        try:
            auth = request.headers.get("Authorization")
            if auth is None:
                logger.warning("The Authorization header is missing: %s.", request.headers)
                abort(403)
            payload = signer.verify_token(auth)
            token_filename = payload.get("filename")
            request_filename = request.view_args["filename"]
            if token_filename is None:
                logger.warning("The payload does not contain 'filename' key: %s.", payload)
                abort(403)
            if token_filename != request_filename:
                logger.warning(
                    "The payload log_relative_path key is different than the one in token:"
                    "Request path: %s. Token path: %s.",
                    request_filename,
                    token_filename,
                )
                abort(403)
        except InvalidAudienceError:
            logger.warning("Invalid audience for the request", exc_info=True)
            abort(403)
        except InvalidSignatureError:
            logger.warning("The signature of the request was wrong", exc_info=True)
            abort(403)
        except ImmatureSignatureError:
            logger.warning("The signature of the request was sent from the future", exc_info=True)
            abort(403)
        except ExpiredSignatureError:
            logger.warning(
                "The signature of the request has expired. Make sure that all components "
                "in your system have synchronized clocks. "
                "See more at %s",
                get_docs_url("configurations-ref.html#secret-key"),
                exc_info=True,
            )
            abort(403)
        except InvalidIssuedAtError:
            logger.warning(
                "The request was issues in the future. Make sure that all components "
                "in your system have synchronized clocks. "
                "See more at %s",
                get_docs_url("configurations-ref.html#secret-key"),
                exc_info=True,
            )
            abort(403)
        except Exception:
            logger.warning("Unknown error", exc_info=True)
            abort(403)

    @flask_app.route("/log/<path:filename>")
    def serve_logs_view(filename):
        return send_from_directory(log_directory, filename, mimetype="application/json", as_attachment=False)

    return flask_app


GunicornOption = collections.namedtuple("GunicornOption", ["key", "value"])


class StandaloneGunicornApplication(gunicorn.app.base.BaseApplication):
    """
    Standalone Gunicorn application/serve for usage with any WSGI-application.

    Code inspired by an example from the Gunicorn documentation.
    https://github.com/benoitc/gunicorn/blob/cf55d2cec277f220ebd605989ce78ad1bb553c46/examples/standalone_app.py

    For details, about standalone gunicorn application, see:
    https://docs.gunicorn.org/en/stable/custom.html
    """

    def __init__(self, app, options=None):
        self.options = options or []
        self.application = app
        super().__init__()

    def load_config(self):
        for option in self.options:
            self.cfg.set(option.key.lower(), option.value)

    def load(self):
        return self.application


def serve_logs():
    """Serves logs generated by Worker"""
    setproctitle("airflow serve-logs")
    wsgi_app = create_app()

    worker_log_server_port = conf.getint("logging", "WORKER_LOG_SERVER_PORT")

    # If dual stack is available and IPV6_V6ONLY is not enabled on the socket
    # then when IPV6 is bound to it will also bind to IPV4 automatically
    if getattr(socket, "has_dualstack_ipv6", lambda: False)():
        bind_option = GunicornOption("bind", f"[::]:{worker_log_server_port}")
    else:
        bind_option = GunicornOption("bind", f"0.0.0.0:{worker_log_server_port}")

    options = [bind_option, GunicornOption("workers", 2)]
    StandaloneGunicornApplication(wsgi_app, options).run()


if __name__ == "__main__":
    serve_logs()
