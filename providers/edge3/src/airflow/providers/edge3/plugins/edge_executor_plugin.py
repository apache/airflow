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
from typing import TYPE_CHECKING, Any

from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException
from airflow.plugins_manager import AirflowPlugin
from airflow.providers.edge3.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_1_PLUS
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

if AIRFLOW_V_3_0_PLUS:
    from airflow.utils.db import DBLocks, create_global_lock

    @provide_session
    def _get_api_endpoint(session: Session = NEW_SESSION) -> dict[str, Any]:
        # Ensure all required DB modeals are created before starting the API
        with create_global_lock(session=session, lock=DBLocks.MIGRATIONS):
            engine = session.get_bind().engine
            from airflow.providers.edge3.models.edge_job import EdgeJobModel
            from airflow.providers.edge3.models.edge_logs import EdgeLogsModel
            from airflow.providers.edge3.models.edge_worker import EdgeWorkerModel

            EdgeJobModel.metadata.create_all(engine)
            EdgeLogsModel.metadata.create_all(engine)
            EdgeWorkerModel.metadata.create_all(engine)

        from airflow.providers.edge3.worker_api.app import create_edge_worker_api_app

        return {
            "app": create_edge_worker_api_app(),
            "url_prefix": "/edge_worker",
            "name": "Airflow Edge Worker",
        }

else:
    # This is for back-compatibility with Airflow 2.x and we only make this
    # to prevents dependencies and breaking imports in Airflow 3.x
    import re
    from datetime import datetime, timedelta
    from pathlib import Path

    from flask import Blueprint, redirect, request, url_for
    from flask_appbuilder import BaseView, expose
    from markupsafe import Markup
    from sqlalchemy import select

    from airflow.auth.managers.models.resource_details import AccessView
    from airflow.utils.state import State, TaskInstanceState
    from airflow.utils.yaml import safe_load
    from airflow.www.auth import has_access_view

    def _get_airflow_2_api_endpoint() -> Blueprint:
        from airflow.www.app import csrf
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
        csrf.exempt(bp)
        return bp

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
        return (
            f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] - {username} updated maintenance mode\nComment:"
        )

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
            maintenance_comment = modify_maintenance_comment_on_update(
                maintenance_comment, current_user.username
            )
            change_maintenance_comment(worker_name, maintenance_comment)
            return redirect(url_for("EdgeWorkerHosts.status"))


# Check if EdgeExecutor is actually loaded
try:
    EDGE_EXECUTOR_ACTIVE = conf.getboolean("edge", "api_enabled", fallback="False")
except AirflowConfigException:
    EDGE_EXECUTOR_ACTIVE = False

# Load the API endpoint only on api-server (Airflow 3.x) or webserver (Airflow 2.x)
# todo(jscheffl): Remove this check when the discussion in
#                 https://lists.apache.org/thread/w170czq6r7bslkqp1tk6bjjjo0789wgl
#                 resulted in a proper API to selective initialize. Maybe backcompat-shim
#                 is also needed to support Airflow-versions prior the rework.
if AIRFLOW_V_3_0_PLUS:
    RUNNING_ON_APISERVER = (len(sys.argv) > 1 and sys.argv[1] in ["api-server"]) or (
        len(sys.argv) > 2 and sys.argv[2] == "airflow-core/src/airflow/api_fastapi/main.py"
    )
else:
    RUNNING_ON_APISERVER = "gunicorn" in sys.argv[0] and "airflow-webserver" in sys.argv


def _get_base_url_path(path: str) -> str:
    """Construct URL path with webserver base_url prefix."""
    base_url = conf.get("api", "base_url", fallback="/")
    # Extract pathname from base_url (handles both full URLs and path-only)
    if base_url.startswith(("http://", "https://")):
        from urllib.parse import urlparse

        base_path = urlparse(base_url).path
    else:
        base_path = base_url

    # Normalize paths: remove trailing slash from base, ensure leading slash on path
    base_path = base_path.rstrip("/")
    return base_path + path


class EdgeExecutorPlugin(AirflowPlugin):
    """EdgeExecutor Plugin - provides API endpoints for Edge Workers in Webserver."""

    name = "edge_executor"
    if EDGE_EXECUTOR_ACTIVE and RUNNING_ON_APISERVER:
        if AIRFLOW_V_3_1_PLUS:
            fastapi_apps = [_get_api_endpoint()]
            react_apps = [
                {
                    "name": "Edge Executor",
                    "bundle_url": _get_base_url_path("/edge_worker/static/main.umd.cjs"),
                    "destination": "nav",
                    "url_route": "edge_executor",
                    "category": "admin",
                    "icon": _get_base_url_path("/edge_worker/res/cloud-computer.svg"),
                    "icon_dark_mode": _get_base_url_path("/edge_worker/res/cloud-computer-dark.svg"),
                },
            ]
            external_views = [
                {
                    "name": "Edge Worker API docs",
                    "href": _get_base_url_path("/edge_worker/docs"),
                    "destination": "nav",
                    "category": "docs",
                    "icon": _get_base_url_path("/edge_worker/res/cloud-computer.svg"),
                    "icon_dark_mode": _get_base_url_path("/edge_worker/res/cloud-computer-dark.svg"),
                    "url_route": "edge_worker_api_docs",
                }
            ]
        if AIRFLOW_V_3_0_PLUS:
            # Airflow 3.0 does not know about react_apps, so we only provide the API endpoint
            fastapi_apps = [_get_api_endpoint()]
        else:
            appbuilder_menu_items = [
                {
                    "name": "Edge Worker API docs",
                    "href": _get_base_url_path("/edge_worker/v1/ui"),
                    "category": "Docs",
                }
            ]
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
            flask_blueprints = [_get_airflow_2_api_endpoint(), template_bp]
