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

import logging
from typing import TYPE_CHECKING, Any

from fastapi import FastAPI

from airflow.api_fastapi.app import create_auth_manager
from airflow.plugins_manager import AirflowPlugin
from airflow.providers.standard.version_compat import AIRFLOW_V_3_1_PLUS
from airflow.utils.session import NEW_SESSION, provide_session

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

if AIRFLOW_V_3_1_PLUS:

    @provide_session
    def _get_api_endpoint(session: Session = NEW_SESSION) -> dict[str, Any]:
        from airflow.utils.db import DBLocks, create_global_lock

        with create_global_lock(session=session, lock=DBLocks.MIGRATIONS):
            engine = session.get_bind().engine
            from airflow.providers.standard.models import HITLResponseModel

            HITLResponseModel.metadata.create_all(engine)

        from airflow.providers.standard.api_fastapi.core_api.routes.hitl import hitl_router

        hitl_api_app = FastAPI(
            title="Airflow Human-in-the-loop API",
            description=(
                "This is Airflow Human-in-the-loop API - which allow human interactions."
                "You can find more information in AIP-90 "
                "https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-90+Human+in+the+loop"
            ),
        )
        hitl_api_app.include_router(hitl_router)
        am = create_auth_manager()
        am.init()
        hitl_api_app.state.auth_manager = am

        return {
            "app": hitl_api_app,
            "url_prefix": "/hitl-responses",
            "name": "Airflow Human in the loop API",
        }


class HumanInTheLoopPlugin(AirflowPlugin):
    """Human in the loop plugin for Airflow."""

    name = "standard_hitl"
    if AIRFLOW_V_3_1_PLUS:
        fastapi_apps = [_get_api_endpoint()]
    else:
        log.warning("Human in the loop functionality needs Airflow 3.1+. Skip loadding HITLDBManager.")
