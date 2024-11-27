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

from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from flask import Blueprint
from flask_appbuilder import BaseView, expose
from packaging.version import Version
from sqlalchemy import select

from airflow import __version__ as airflow_version
from airflow.auth.managers.models.resource_details import AccessView
from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException
from airflow.models.taskinstance import TaskInstanceState
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.yaml import safe_load
from airflow.www import utils as wwwutils
from airflow.www.auth import has_access_view
from airflow.www.constants import SWAGGER_BUNDLE, SWAGGER_ENABLED
from airflow.www.extensions.init_views import _CustomErrorRequestBodyValidator, _LazyResolver

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

AIRFLOW_VERSION = Version(airflow_version)
AIRFLOW_V_3_0_PLUS = Version(AIRFLOW_VERSION.base_version) >= Version("3.0.0")


def _get_airflow_2_api_endpoint() -> Blueprint:
    folder = Path(__file__).parents[1].resolve()  # this is airflow/providers/edge/
    with folder.joinpath("openapi", "edge_worker_api_v1.yaml").open() as f:
        specification = safe_load(f)
    from connexion import FlaskApi

    bp = FlaskApi(
        specification=specification,
        resolver=_LazyResolver(),
        base_path="/edge_worker/v1",
        options={"swagger_ui": SWAGGER_ENABLED, "swagger_path": SWAGGER_BUNDLE.__fspath__()},
        strict_validation=True,
        validate_responses=True,
        validator_map={"body": _CustomErrorRequestBodyValidator},
    ).blueprint
    # Need to exempt CSRF to make API usable
    from airflow.www.app import csrf

    csrf.exempt(bp)
    return bp


def _get_api_endpoint() -> dict[str, Any]:
    from airflow.providers.edge.worker_api.app import create_edge_worker_api_app

    return {
        "app": create_edge_worker_api_app(),
        "url_prefix": "/edge_worker/v1",
        "name": "Airflow Edge Worker API",
    }


# registers airflow/providers/edge/plugins/templates as a Jinja template folder
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
        from airflow.providers.edge.models.edge_job import EdgeJobModel

        jobs = session.scalars(select(EdgeJobModel).order_by(EdgeJobModel.queued_dttm)).all()
        html_states = {
            str(state): wwwutils.state_token(str(state)) for state in TaskInstanceState.__members__.values()
        }
        return self.render_template("edge_worker_jobs.html", jobs=jobs, html_states=html_states)


class EdgeWorkerHosts(BaseView):
    """Simple view to show Edge Worker status."""

    default_view = "status"

    @expose("/status")
    @has_access_view(AccessView.JOBS)
    @provide_session
    def status(self, session: Session = NEW_SESSION):
        from airflow.providers.edge.models.edge_worker import EdgeWorkerModel

        hosts = session.scalars(select(EdgeWorkerModel).order_by(EdgeWorkerModel.worker_name)).all()
        five_min_ago = datetime.now() - timedelta(minutes=5)
        return self.render_template("edge_worker_hosts.html", hosts=hosts, five_min_ago=five_min_ago)


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
