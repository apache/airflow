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
from typing import TYPE_CHECKING

from connexion import FlaskApi
from flask import Blueprint
from flask_appbuilder import BaseView, expose
from sqlalchemy import select

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


def _get_api_endpoints() -> Blueprint:
    folder = Path(__file__).parents[1].resolve()  # this is airflow/providers/edge/
    with folder.joinpath("openapi", "edge_worker_api_v1.yaml").open() as f:
        specification = safe_load(f)
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
    EDGE_EXECUTOR_ACTIVE = conf.getboolean("edge", "api_enabled")
except AirflowConfigException:
    EDGE_EXECUTOR_ACTIVE = False


class EdgeExecutorPlugin(AirflowPlugin):
    """EdgeExecutor Plugin - provides API endpoints for Edge Workers in Webserver."""

    name = "edge_executor"
    flask_blueprints = [_get_api_endpoints(), template_bp] if EDGE_EXECUTOR_ACTIVE else []
    appbuilder_views = (
        [
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
        if EDGE_EXECUTOR_ACTIVE
        else []
    )
