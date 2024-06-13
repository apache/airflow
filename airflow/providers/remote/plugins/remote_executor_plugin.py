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

from pathlib import Path
from typing import TYPE_CHECKING

from connexion import FlaskApi
from flask import Blueprint
from flask_appbuilder import BaseView, expose
from sqlalchemy import select

from airflow.auth.managers.models.resource_details import AccessView
from airflow.executors.executor_loader import ExecutorLoader
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
    folder = Path(__file__).parents[1].resolve()  # this is airflow/providers/remote/
    with folder.joinpath("openapi", "remote_worker_api_v1.yaml").open() as f:
        specification = safe_load(f)
    bp = FlaskApi(
        specification=specification,
        resolver=_LazyResolver(),
        base_path="/remote_worker/v1",
        options={"swagger_ui": SWAGGER_ENABLED, "swagger_path": SWAGGER_BUNDLE.__fspath__()},
        strict_validation=True,
        validate_responses=True,
        validator_map={"body": _CustomErrorRequestBodyValidator},
    ).blueprint
    # Need to excemp CSRF to make API usable
    from airflow.www.app import csrf

    csrf.exempt(bp)
    return bp


# registers airflow/providers/remote/plugins/templates as a Jinja template folder
template_bp = Blueprint(
    "template_blueprint",
    __name__,
    template_folder="templates",
)


class RemoteWorker(BaseView):
    """Simple view to show remote worker status."""

    default_view = "status"

    @expose("/status")
    @has_access_view(AccessView.JOBS)
    @provide_session
    def status(self, session: Session = NEW_SESSION):
        from airflow.providers.remote.models.remote_job import RemoteJobModel

        jobs = session.scalars(select(RemoteJobModel)).all()
        html_states = {
            str(state): wwwutils.state_token(str(state)) for state in TaskInstanceState.__members__.values()
        }
        return self.render_template("remote_worker_status.html", jobs=jobs, html_states=html_states)


# Check if RemoteExecutor is actually loaded
REMOTE_EXECUTOR_ACTIVE = "RemoteExecutor" in str(ExecutorLoader.get_executor_names())


class RemoteExecutorPlugin(AirflowPlugin):
    """Remote Executor Plugin - provides API endpoints for remote workers in Webserver."""

    name = "remote_executor"
    flask_blueprints = [_get_api_endpoints(), template_bp] if REMOTE_EXECUTOR_ACTIVE else []
    appbuilder_views = (
        [
            {
                "name": "Remote Worker Status",
                "category": "Admin",
                "view": RemoteWorker(),
            }
        ]
        if REMOTE_EXECUTOR_ACTIVE
        else []
    )
