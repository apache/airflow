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

from os.path import isabs

from flask import Flask
from flask_appbuilder import SQLA
from flask_wtf.csrf import CSRFProtect
from sqlalchemy.engine.url import make_url

from airflow import settings
from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException
from airflow.logging_config import configure_logging
from airflow.providers.fab.www.extensions.init_appbuilder import init_appbuilder
from airflow.providers.fab.www.extensions.init_jinja_globals import init_jinja_globals
from airflow.providers.fab.www.extensions.init_manifest_files import configure_manifest_files
from airflow.providers.fab.www.extensions.init_security import init_xframe_protection
from airflow.providers.fab.www.extensions.init_views import init_error_handlers, init_plugins

app: Flask | None = None

# Initializes at the module level, so plugins can access it.
# See: /docs/plugins.rst
csrf = CSRFProtect()


def create_app(config=None, testing=False):
    """Create a new instance of Airflow WWW app."""
    flask_app = Flask(__name__)
    flask_app.secret_key = conf.get("webserver", "SECRET_KEY")
    flask_app.config["SQLALCHEMY_DATABASE_URI"] = conf.get("database", "SQL_ALCHEMY_CONN")

    url = make_url(flask_app.config["SQLALCHEMY_DATABASE_URI"])
    if url.drivername == "sqlite" and url.database and not isabs(url.database):
        raise AirflowConfigException(
            f'Cannot use relative path: `{conf.get("database", "SQL_ALCHEMY_CONN")}` to connect to sqlite. '
            "Please use absolute path such as `sqlite:////tmp/airflow.db`."
        )

    if "SQLALCHEMY_ENGINE_OPTIONS" not in flask_app.config:
        flask_app.config["SQLALCHEMY_ENGINE_OPTIONS"] = settings.prepare_engine_args()

    db = SQLA()
    db.session = settings.Session
    db.init_app(flask_app)

    configure_logging()
    configure_manifest_files(flask_app)

    with flask_app.app_context():
        init_appbuilder(flask_app)
        init_plugins(flask_app)
        init_error_handlers(flask_app)
        init_jinja_globals(flask_app)
        init_xframe_protection(flask_app)
    return flask_app


def cached_app(config=None, testing=False):
    """Return cached instance of Airflow WWW app."""
    global app
    if not app:
        app = create_app(config=config, testing=testing)
    return app


def purge_cached_app():
    """Remove the cached version of the app in global state."""
    global app
    app = None
