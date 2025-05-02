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

import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from flask import Blueprint, redirect, request, url_for
from flask_appbuilder import BaseView, expose
from markupsafe import Markup
from sqlalchemy import select

from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException
from airflow.models.taskinstance import TaskInstanceState
from airflow.plugins_manager import AirflowPlugin
from airflow.providers.edge3.version_compat import AIRFLOW_V_3_0_PLUS
from airflow.utils.state import State

if AIRFLOW_V_3_0_PLUS:
    from airflow.api_fastapi.auth.managers.models.resource_details import AccessView
    from airflow.providers.fab.www.auth import has_access_view

else:
    from airflow.auth.managers.models.resource_details import AccessView  # type: ignore[no-redef]
    from airflow.www.auth import has_access_view  # type: ignore[no-redef]
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.yaml import safe_load

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


def _get_airflow_2_api_endpoint() -> Blueprint:
    from airflow.www.constants import SWAGGER_BUNDLE, SWAGGER_ENABLED
    from airflow.www.extensions.init_views import _CustomErrorRequestBodyValidator, _LazyResolver

    folder = Path(__file__).parents[1].resolve()  # this is airflow/providers/edge3/
    with folder.joinpath("openapi", "edge_worker_api_v1.yaml").open() as f:
        specification = safe_load(f)
    from connexion import FlaskApi

    bp = FlaskApi(
        specification=specification,
        resolver=_LazyResolver(),
        base_path="/edge_worker/v1",
        strict_validation=True,
        options={"swagger_ui": SWAGGER_ENABLED, "swagger_path": SWAGGER_BUNDLE.__fspath__()},
        validate_responses=True,
        validator_map={"body": _CustomErrorRequestBodyValidator},
    ).blueprint
    # Need to exempt CSRF to make API usable
    from airflow.www.app import csrf

    csrf.exempt(bp)
    return bp


def _get_api_endpoint() -> dict[str, Any]:
    from airflow.providers.edge3.worker_api.app import create_edge_worker_api_app

    return {
        "app": create_edge_worker_api_app(),
        "url_prefix": "/edge_worker/v1",
        "name": "Airflow Edge Worker API",
    }


def _state_token(state):
    """Return a formatted string with HTML for a given State."""
    color = State.color(state)
    fg_color = State.color_fg(state)
    return Markup(
        """
        <span class="label" style="color:{fg_color}; background-color:{color};"
            title="Current State: {state}">{state}</span>
        """
    ).format(color=color, state=state, fg_color=fg_color)


def modify_maintenance_comment_on_update(maintenance_comment: str | None, username: str) -> str:
    if maintenance_comment:
        if re.search(
            r"^\[[-\d:\s]+\] - .+ put node into maintenance mode\r?\nComment:.*", maintenance_comment
        ):
            return re.sub(
                r"^\[[-\d:\s]+\] - .+ put node into maintenance mode\r?\nComment:",
                f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] - {username} updated maintenance mode\nComment:",
                maintenance_comment,
            )
        if re.search(r"^\[[-\d:\s]+\] - .+ updated maintenance mode\r?\nComment:.*", maintenance_comment):
            return re.sub(
                r"^\[[-\d:\s]+\] - .+ updated maintenance mode\r?\nComment:",
                f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] - {username} updated maintenance mode\nComment:",
                maintenance_comment,
            )
        return f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] - {username} updated maintenance mode\nComment: {maintenance_comment}"
    return f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] - {username} updated maintenance mode\nComment:"


# registers airflow/providers/edge3/plugins/templates as a Jinja template folder
template_bp = Blueprint(
    "template_blueprint",
    __name__,
    template_folder="templates",
)


class EdgeWorkerJobs(BaseView):
    """Simple view to show Edge Worker jobs."""

    default_view = "jobs"

    @expose("/jobs")
    @has_access_view(AccessView.JOBS)
    @provide_session
    def jobs(self, session: Session = NEW_SESSION):
        from airflow.providers.edge3.models.edge_job import EdgeJobModel

        jobs = session.scalars(select(EdgeJobModel).order_by(EdgeJobModel.queued_dttm)).all()
        html_states = {
            str(state): _state_token(str(state)) for state in TaskInstanceState.__members__.values()
        }
        return self.render_template("edge_worker_jobs.html", jobs=jobs, html_states=html_states)


class EdgeWorkerHosts(BaseView):
    """Simple view to show Edge Worker status."""

    default_view = "status"

    @expose("/status")
    @has_access_view(AccessView.JOBS)
    @provide_session
    def status(self, session: Session = NEW_SESSION):
        from airflow.providers.edge3.models.edge_worker import EdgeWorkerModel

        hosts = session.scalars(select(EdgeWorkerModel).order_by(EdgeWorkerModel.worker_name)).all()
        five_min_ago = datetime.now() - timedelta(minutes=5)
        return self.render_template("edge_worker_hosts.html", hosts=hosts, five_min_ago=five_min_ago)

    @expose("/status/maintenance/<string:worker_name>/on", methods=["POST"])
    @has_access_view(AccessView.JOBS)
    def worker_to_maintenance(self, worker_name: str):
        from flask_login import current_user

        from airflow.providers.edge3.models.edge_worker import request_maintenance

        maintenance_comment = request.form.get("maintenance_comment")
        maintenance_comment = f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] - {current_user.username} put node into maintenance mode\nComment: {maintenance_comment}"
        request_maintenance(worker_name, maintenance_comment)
        return redirect(url_for("EdgeWorkerHosts.status"))

    @expose("/status/maintenance/<string:worker_name>/off", methods=["POST"])
    @has_access_view(AccessView.JOBS)
    def remove_worker_from_maintenance(self, worker_name: str):
        from airflow.providers.edge3.models.edge_worker import exit_maintenance

        exit_maintenance(worker_name)
        return redirect(url_for("EdgeWorkerHosts.status"))

    @expose("/status/maintenance/<string:worker_name>/remove", methods=["POST"])
    @has_access_view(AccessView.JOBS)
    def remove_worker(self, worker_name: str):
        from airflow.providers.edge3.models.edge_worker import remove_worker

        remove_worker(worker_name)
        return redirect(url_for("EdgeWorkerHosts.status"))

    @expose("/status/maintenance/<string:worker_name>/change_comment", methods=["POST"])
    @has_access_view(AccessView.JOBS)
    def change_maintenance_comment(self, worker_name: str):
        from flask_login import current_user

        from airflow.providers.edge3.models.edge_worker import change_maintenance_comment

        maintenance_comment = request.form.get("maintenance_comment")
        maintenance_comment = modify_maintenance_comment_on_update(maintenance_comment, current_user.username)
        change_maintenance_comment(worker_name, maintenance_comment)
        return redirect(url_for("EdgeWorkerHosts.status"))


# Check if EdgeExecutor is actually loaded
try:
    EDGE_EXECUTOR_ACTIVE = conf.getboolean("edge", "api_enabled", fallback="False")
except AirflowConfigException:
    EDGE_EXECUTOR_ACTIVE = False


class EdgeExecutorPlugin(AirflowPlugin):
    """EdgeExecutor Plugin - provides API endpoints for Edge Workers in Webserver."""

    name = "edge_executor"
    if EDGE_EXECUTOR_ACTIVE:
        appbuilder_views = [
            {
                "name": "Edge Worker Jobs",
                "category": "Admin",
                "view": EdgeWorkerJobs(),
            },
            {
                "name": "Edge Worker Hosts",
                "category": "Admin",
                "view": EdgeWorkerHosts(),
            },
        ]

        if AIRFLOW_V_3_0_PLUS:
            fastapi_apps = [_get_api_endpoint()]
            flask_blueprints = [template_bp]
        else:
            flask_blueprints = [_get_airflow_2_api_endpoint(), template_bp]
