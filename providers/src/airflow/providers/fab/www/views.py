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

import lazy_object_proxy
from flask import (
    current_app,
    render_template,
)
from flask_appbuilder import BaseView, expose
from itsdangerous import URLSafeSerializer

from airflow import settings
from airflow.api_fastapi.app import get_auth_manager
from airflow.auth.managers.models.resource_details import DagDetails
from airflow.configuration import conf
from airflow.executors.executor_loader import ExecutorLoader
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.jobs.triggerer_job_runner import TriggererJobRunner
from airflow.utils.docs import get_docs_url
from airflow.utils.net import get_hostname
from airflow.version import version

FILTER_TAGS_COOKIE = "tags_filter"
FILTER_LASTRUN_COOKIE = "last_run_filter"


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


def method_not_allowed(error):
    """Show Method Not Allowed on screen for any error in the Webserver."""
    return (
        render_template(
            "airflow/error.html",
            hostname=get_hostname() if conf.getboolean("webserver", "EXPOSE_HOSTNAME") else "",
            status_code=405,
            error_message="Received an invalid request.",
        ),
        405,
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


class AirflowBaseView(BaseView):
    """Base View to set Airflow related properties."""

    from airflow import macros

    route_base = ""

    extra_args = {
        # Make our macros available to our UI templates too.
        "macros": macros,
        "get_docs_url": get_docs_url,
    }

    if not conf.getboolean("core", "unit_test_mode"):
        executor, _ = ExecutorLoader.import_default_executor_cls()
        extra_args["sqlite_warning"] = settings.engine and (settings.engine.dialect.name == "sqlite")
        if not executor.is_production:
            extra_args["production_executor_warning"] = executor.__name__
        extra_args["otel_metrics_on"] = conf.getboolean("metrics", "otel_on")
        extra_args["otel_traces_on"] = conf.getboolean("traces", "otel_on")

    line_chart_attr = {
        "legend.maxKeyLength": 200,
    }

    def render_template(self, *args, **kwargs):
        # Add triggerer_job only if we need it
        if TriggererJobRunner.is_needed():
            kwargs["triggerer_job"] = lazy_object_proxy.Proxy(TriggererJobRunner.most_recent_job)

        if "dag" in kwargs:
            kwargs["can_edit_dag"] = get_auth_manager().is_authorized_dag(
                method="PUT", details=DagDetails(id=kwargs["dag"].dag_id)
            )
            url_serializer = URLSafeSerializer(current_app.config["SECRET_KEY"])
            kwargs["dag_file_token"] = url_serializer.dumps(kwargs["dag"].fileloc)

        return super().render_template(
            *args,
            # Cache this at most once per request, not for the lifetime of the view instance
            scheduler_job=lazy_object_proxy.Proxy(SchedulerJobRunner.most_recent_job),
            **kwargs,
        )


class Airflow(AirflowBaseView):
    """Main Airflow application."""

    @expose("/home")
    def index(self):
        return self.render_template("airflow/main.html")
