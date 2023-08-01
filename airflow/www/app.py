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

import warnings
from datetime import timedelta

from flask import Flask
from flask_appbuilder import SQLA
from flask_wtf.csrf import CSRFProtect
from markupsafe import Markup
from sqlalchemy.engine.url import make_url

from airflow import settings
from airflow.api_internal.internal_api_call import InternalApiConfig
from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException, RemovedInAirflow3Warning
from airflow.logging_config import configure_logging
from airflow.models import import_all_models
from airflow.settings import _ENABLE_AIP_44
from airflow.utils.json import AirflowJsonProvider
from airflow.www.extensions.init_appbuilder import init_appbuilder
from airflow.www.extensions.init_appbuilder_links import init_appbuilder_links
from airflow.www.extensions.init_cache import init_cache
from airflow.www.extensions.init_dagbag import init_dagbag
from airflow.www.extensions.init_jinja_globals import init_jinja_globals
from airflow.www.extensions.init_manifest_files import configure_manifest_files
from airflow.www.extensions.init_robots import init_robots
from airflow.www.extensions.init_security import (
    init_api_experimental_auth,
    init_check_user_active,
    init_xframe_protection,
)
from airflow.www.extensions.init_session import init_airflow_session_interface
from airflow.www.extensions.init_views import (
    init_api_connexion,
    init_api_experimental,
    init_api_internal,
    init_appbuilder_views,
    init_error_handlers,
    init_flash_views,
    init_plugins,
)
from airflow.www.extensions.init_wsgi_middlewares import init_wsgi_middleware

app: Flask | None = None

# Initializes at the module level, so plugins can access it.
# See: /docs/plugins.rst
csrf = CSRFProtect()


def sync_appbuilder_roles(flask_app):
    """Sync appbuilder roles to DB."""
    # Garbage collect old permissions/views after they have been modified.
    # Otherwise, when the name of a view or menu is changed, the framework
    # will add the new Views and Menus names to the backend, but will not
    # delete the old ones.
    if conf.getboolean("webserver", "UPDATE_FAB_PERMS"):
        flask_app.appbuilder.sm.sync_roles()


def create_app(config=None, testing=False):
    """Create a new instance of Airflow WWW app."""
    flask_app = Flask(__name__)
    flask_app.secret_key = conf.get("webserver", "SECRET_KEY")

    flask_app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(minutes=settings.get_session_lifetime_config())

    webserver_config = conf.get_mandatory_value("webserver", "config_file")
    flask_app.config.from_pyfile(webserver_config, silent=True)

    flask_app.config["TESTING"] = testing
    flask_app.config["SQLALCHEMY_DATABASE_URI"] = conf.get("database", "SQL_ALCHEMY_CONN")

    instance_name = conf.get(section="webserver", key="instance_name", fallback="Airflow")
    instance_name_has_markup = conf.getboolean(
        section="webserver", key="instance_name_has_markup", fallback=False
    )
    if instance_name_has_markup:
        instance_name = Markup(instance_name).striptags()

    flask_app.config["APP_NAME"] = instance_name

    url = make_url(flask_app.config["SQLALCHEMY_DATABASE_URI"])
    if url.drivername == "sqlite" and url.database and not url.database.startswith("/"):
        raise AirflowConfigException(
            f'Cannot use relative path: `{conf.get("database", "SQL_ALCHEMY_CONN")}` to connect to sqlite. '
            "Please use absolute path such as `sqlite:////tmp/airflow.db`."
        )

    flask_app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

    flask_app.config["SESSION_COOKIE_HTTPONLY"] = True
    flask_app.config["SESSION_COOKIE_SECURE"] = conf.getboolean("webserver", "COOKIE_SECURE")

    cookie_samesite_config = conf.get("webserver", "COOKIE_SAMESITE")
    if cookie_samesite_config == "":
        warnings.warn(
            "Old deprecated value found for `cookie_samesite` option in `[webserver]` section. "
            "Using `Lax` instead. Change the value to `Lax` in airflow.cfg to remove this warning.",
            RemovedInAirflow3Warning,
        )
        cookie_samesite_config = "Lax"
    flask_app.config["SESSION_COOKIE_SAMESITE"] = cookie_samesite_config

    if config:
        flask_app.config.from_mapping(config)

    if "SQLALCHEMY_ENGINE_OPTIONS" not in flask_app.config:
        flask_app.config["SQLALCHEMY_ENGINE_OPTIONS"] = settings.prepare_engine_args()

    # Configure the JSON encoder used by `|tojson` filter from Flask
    flask_app.json_provider_class = AirflowJsonProvider
    flask_app.json = AirflowJsonProvider(flask_app)

    InternalApiConfig.force_database_direct_access()

    csrf.init_app(flask_app)

    init_wsgi_middleware(flask_app)

    db = SQLA()
    db.session = settings.Session
    db.init_app(flask_app)

    init_dagbag(flask_app)

    init_api_experimental_auth(flask_app)

    init_robots(flask_app)

    init_cache(flask_app)

    init_flash_views(flask_app)

    configure_logging()
    configure_manifest_files(flask_app)

    import_all_models()

    with flask_app.app_context():
        init_appbuilder(flask_app)

        init_appbuilder_views(flask_app)
        init_appbuilder_links(flask_app)
        init_plugins(flask_app)
        init_error_handlers(flask_app)
        init_api_connexion(flask_app)
        if conf.getboolean("webserver", "run_internal_api", fallback=False):
            if not _ENABLE_AIP_44:
                raise RuntimeError("The AIP_44 is not enabled so you cannot use it.")
            init_api_internal(flask_app)
        init_api_experimental(flask_app)

        sync_appbuilder_roles(flask_app)

        init_jinja_globals(flask_app)
        init_xframe_protection(flask_app)
        init_airflow_session_interface(flask_app)
        init_check_user_active(flask_app)
    return flask_app


def cached_app(config=None, testing=False):
    """Return cached instance of Airflow WWW app."""
    global app
    if not app:
        app = create_app(config=config, testing=testing)
    return app


def purge_cached_app():
    """Removes the cached version of the app in global state."""
    global app
    app = None
