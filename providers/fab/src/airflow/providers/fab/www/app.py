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

from datetime import timedelta
from functools import cache
from os.path import isabs

from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_wtf.csrf import CSRFProtect
from sqlalchemy.engine.url import make_url

from airflow import settings
from airflow.api_fastapi.app import get_auth_manager
from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException
from airflow.logging_config import configure_logging
from airflow.providers.fab.www.extensions.init_appbuilder import AirflowAppBuilder
from airflow.providers.fab.www.extensions.init_jinja_globals import init_jinja_globals
from airflow.providers.fab.www.extensions.init_manifest_files import configure_manifest_files
from airflow.providers.fab.www.extensions.init_security import init_api_auth
from airflow.providers.fab.www.extensions.init_session import init_airflow_session_interface
from airflow.providers.fab.www.extensions.init_views import (
    init_api_auth_provider,
    init_api_error_handlers,
    init_error_handlers,
    init_plugins,
)
from airflow.providers.fab.www.extensions.init_wsgi_middlewares import init_wsgi_middleware
from airflow.providers.fab.www.utils import get_session_lifetime_config

# Initializes at the module level, so plugins can access it.
# See: /docs/plugins.rst
csrf = CSRFProtect()


def create_app(enable_plugins: bool):
    """Create a new instance of Airflow WWW app."""
    from airflow.providers.fab.auth_manager.fab_auth_manager import FabAuthManager

    flask_app = Flask(__name__)
    flask_app.secret_key = conf.get("api", "SECRET_KEY")
    flask_app.config["SQLALCHEMY_DATABASE_URI"] = conf.get("database", "SQL_ALCHEMY_CONN")
    flask_app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    flask_app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(minutes=get_session_lifetime_config())

    flask_app.config["SESSION_COOKIE_HTTPONLY"] = True
    if conf.has_option("fab", "COOKIE_SECURE"):
        flask_app.config["SESSION_COOKIE_SECURE"] = conf.getboolean("fab", "COOKIE_SECURE")
    if conf.has_option("fab", "COOKIE_SAMESITE"):
        flask_app.config["SESSION_COOKIE_SAMESITE"] = conf.get("fab", "COOKIE_SAMESITE")

    webserver_config = conf.get_mandatory_value("fab", "config_file")
    # Enable customizations in webserver_config.py to be applied via Flask.current_app.
    with flask_app.app_context():
        flask_app.config.from_pyfile(webserver_config, silent=True)
        flask_app.config.from_prefixed_env(prefix="AIRFLOW__FAB__CONFIG__")

    url = make_url(flask_app.config["SQLALCHEMY_DATABASE_URI"])
    if url.drivername == "sqlite" and url.database and not isabs(url.database):
        raise AirflowConfigException(
            f"Cannot use relative path: `{conf.get('database', 'SQL_ALCHEMY_CONN')}` to connect to sqlite. "
            "Please use absolute path such as `sqlite:////tmp/airflow.db`."
        )

    if "SQLALCHEMY_ENGINE_OPTIONS" not in flask_app.config:
        flask_app.config["SQLALCHEMY_ENGINE_OPTIONS"] = settings.prepare_engine_args()

    csrf.init_app(flask_app)

    db = SQLAlchemy(flask_app)
    if settings.Session is None:
        raise RuntimeError("Session not configured. Call configure_orm() first.")
    db.session = settings.Session

    configure_logging()
    configure_manifest_files(flask_app)
    init_api_auth(flask_app)

    with flask_app.app_context():
        AirflowAppBuilder(
            app=flask_app,
            session=db.session(),
            base_template="airflow/main.html",
            enable_plugins=enable_plugins,
        )
        init_error_handlers(flask_app)
        # In two scenarios a Flask application can be created:
        # - To support Airflow 2 plugins relying on Flask (``enable_plugins`` is True)
        # - To support FAB auth manager (``enable_plugins`` is False)
        # There are some edge cases where ``enable_plugins`` is False but the auth manager configured is not
        # FAB auth manager. One example is ``run_update_fastapi_api_spec``, it calls
        # ``FabAuthManager().get_fastapi_app()`` to generate the openapi documentation regardless of the
        # configured auth manager.
        if enable_plugins:
            init_plugins(flask_app)
        elif isinstance(get_auth_manager(), FabAuthManager):
            init_api_auth_provider(flask_app)
            init_api_error_handlers(flask_app)
            init_airflow_session_interface(flask_app, db)
        init_jinja_globals(flask_app, enable_plugins=enable_plugins)
        init_wsgi_middleware(flask_app)
    return flask_app


@cache
def cached_app():
    """Return cached instance of Airflow WWW app."""
    return create_app()


def purge_cached_app():
    """Remove the cached version of the app in global state."""
    cached_app.cache_clear()
