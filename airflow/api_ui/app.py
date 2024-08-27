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

import os

from fastapi import FastAPI

from airflow.models.dagbag import DagBag
from airflow.settings import DAGS_FOLDER


def init_dag_bag(app: FastAPI) -> None:
    """
    Create global DagBag for the FastAPI application.

    To access it use ``request.app.state.dag_bag``.
    """
    if os.environ.get("SKIP_DAGS_PARSING") == "True":
        app.state.dag_bag = DagBag(os.devnull, include_examples=False)
    else:
        app.state.dag_bag = DagBag(DAGS_FOLDER, read_dags_from_db=True)


def create_app() -> FastAPI:
    app = FastAPI(
        description="Internal Rest API for the UI frontend. It is subject to breaking change "
        "depending on the need of the frontend. Users should not rely on this API but use the "
        "public API instead."
    )

    init_dag_bag(app)

    return app
