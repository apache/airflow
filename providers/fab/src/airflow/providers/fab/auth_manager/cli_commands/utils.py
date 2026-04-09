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
from collections.abc import Generator
from contextlib import contextmanager
from functools import cache
from os.path import isabs
from typing import TYPE_CHECKING

from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.engine import make_url

import airflow
from airflow.exceptions import AirflowConfigException
from airflow.providers.common.compat.sdk import conf
from airflow.providers.fab.www.extensions.init_appbuilder import init_appbuilder
from airflow.providers.fab.www.extensions.init_session import init_airflow_session_interface
from airflow.providers.fab.www.extensions.init_views import init_plugins

if TYPE_CHECKING:
    from airflow.providers.fab.www.extensions.init_appbuilder import AirflowAppBuilder


@cache
def _return_appbuilder(app: Flask, db) -> AirflowAppBuilder:
    """Return an appbuilder instance for the given app."""
    init_appbuilder(app, enable_plugins=False)
    init_plugins(app)
    init_airflow_session_interface(app, db)
    return app.appbuilder  # type: ignore[attr-defined]


@contextmanager
def get_application_builder() -> Generator[AirflowAppBuilder, None, None]:
    static_folder = os.path.join(os.path.dirname(airflow.__file__), "www", "static")
    flask_app = Flask(__name__, static_folder=static_folder)
    webserver_config = conf.get_mandatory_value("fab", "config_file")
    with flask_app.app_context():
        # Enable customizations in webserver_config.py to be applied via Flask.current_app.
        flask_app.config.from_pyfile(webserver_config, silent=True)
        flask_app.config["SQLALCHEMY_DATABASE_URI"] = conf.get("database", "SQL_ALCHEMY_CONN")
        url = make_url(flask_app.config["SQLALCHEMY_DATABASE_URI"])
        if url.drivername == "sqlite" and url.database and not isabs(url.database):
            raise AirflowConfigException(
                f"Cannot use relative path: `{conf.get('database', 'SQL_ALCHEMY_CONN')}` to connect to sqlite. "
                "Please use absolute path such as `sqlite:////tmp/airflow.db`."
            )
        flask_app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

        db = SQLAlchemy(flask_app)
        yield _return_appbuilder(flask_app, db)
        db.engine.dispose(close=True)
