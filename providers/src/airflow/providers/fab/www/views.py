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

import sys
import traceback

from flask import (
    render_template,
)

from airflow.api_fastapi.app import get_auth_manager
from airflow.configuration import conf
from airflow.utils.net import get_hostname
from airflow.version import version


def not_found(error):
    """Show Not Found on screen for any error in the Webserver."""
    return (
        render_template(
            "airflow/error.html",
            hostname=get_hostname() if conf.getboolean("webserver", "EXPOSE_HOSTNAME") else "",
            status_code=404,
            error_message="Page cannot be found.",
        ),
        404,
    )


def show_traceback(error):
    """Show Traceback for a given error."""
    is_logged_in = get_auth_manager().is_logged_in()
    return (
        render_template(
            "airflow/traceback.html",
            python_version=sys.version.split(" ")[0] if is_logged_in else "redacted",
            airflow_version=version if is_logged_in else "redacted",
            hostname=(
                get_hostname()
                if conf.getboolean("webserver", "EXPOSE_HOSTNAME") and is_logged_in
                else "redacted"
            ),
            info=(
                traceback.format_exc()
                if conf.getboolean("webserver", "EXPOSE_STACKTRACE") and is_logged_in
                else "Error! Please contact server admin."
            ),
        ),
        500,
    )
