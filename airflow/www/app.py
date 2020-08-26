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
#

from datetime import timedelta
from typing import Optional

from flask import Flask
from flask_appbuilder import SQLA
from flask_caching import Cache
from flask_wtf.csrf import CSRFProtect

from airflow import settings
from airflow.configuration import conf
from airflow.logging_config import configure_logging
from airflow.utils.json import AirflowJsonEncoder
from airflow.www.extensions.init_appbuilder import init_appbuilder
from airflow.www.extensions.init_appbuilder_links import init_appbuilder_links
from airflow.www.extensions.init_dagbag import init_dagbag
from airflow.www.extensions.init_jinja_globals import init_jinja_globals
from airflow.www.extensions.init_manifest_files import configure_manifest_files
from airflow.www.extensions.init_security import init_api_experimental_auth, init_xframe_protection
from airflow.www.extensions.init_session import init_logout_timeout, init_permanent_session
from airflow.www.extensions.init_views import (
    init_api_connexion, init_api_experimental, init_appbuilder_views, init_error_handlers, init_flash_views,
    init_plugins,
)
from airflow.www.extensions.init_wsgi_middlewares import init_wsgi_middleware

app: Optional[Flask] = None

# Initializes at the module level, so plugins can access it.
# See: /docs/plugins.rst
csrf = CSRFProtect()


def sync_appbuilder_roles(flask_app):
    """Sync appbuilder roles to DB"""
    # Garbage collect old permissions/views after they have been modified.
    # Otherwise, when the name of a view or menu is changed, the framework
    # will add the new Views and Menus names to the backend, but will not
    # delete the old ones.
    if conf.getboolean('webserver', 'UPDATE_FAB_PERMS'):
        security_manager = flask_app.appbuilder.sm
        security_manager.sync_roles()


def create_app(config=None, testing=False, app_name="Airflow"):
    """Create a new instance of Airflow WWW app"""
    flask_app = Flask(__name__)
    flask_app.secret_key = conf.get('webserver', 'SECRET_KEY')

    session_lifetime_days = conf.getint('webserver', 'SESSION_LIFETIME_DAYS', fallback=30)
    flask_app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=session_lifetime_days)

    flask_app.config.from_pyfile(settings.WEBSERVER_CONFIG, silent=True)
    flask_app.config['APP_NAME'] = app_name
    flask_app.config['TESTING'] = testing
    flask_app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

    flask_app.config['SESSION_COOKIE_HTTPONLY'] = True
    flask_app.config['SESSION_COOKIE_SECURE'] = conf.getboolean('webserver', 'COOKIE_SECURE')
    flask_app.config['SESSION_COOKIE_SAMESITE'] = conf.get('webserver', 'COOKIE_SAMESITE')

    if config:
        flask_app.config.from_mapping(config)

    # Configure the JSON encoder used by `|tojson` filter from Flask
    flask_app.json_encoder = AirflowJsonEncoder

    csrf.init_app(flask_app)

    init_wsgi_middleware(flask_app)

    db = SQLA()
    db.session = settings.Session
    db.init_app(flask_app)

    init_dagbag(flask_app)

    init_api_experimental_auth(flask_app)

    Cache(app=flask_app, config={'CACHE_TYPE': 'filesystem', 'CACHE_DIR': '/tmp'})

    init_flash_views(flask_app)

    configure_logging()
    configure_manifest_files(flask_app)

    with flask_app.app_context():
        init_appbuilder(flask_app)

        init_appbuilder_views(flask_app)
        init_appbuilder_links(flask_app)
        init_plugins(flask_app)
        init_error_handlers(flask_app)
        init_api_connexion(flask_app)
        init_api_experimental(flask_app)

        sync_appbuilder_roles(flask_app)

        init_jinja_globals(flask_app)
        init_logout_timeout(flask_app)
        init_xframe_protection(flask_app)
        init_permanent_session(flask_app)

    return flask_app


def cached_app(config=None, testing=False):
    """Return cached instance of Airflow WWW app"""
    global app  # pylint: disable=global-statement
    if not app:
        app = create_app(config=config, testing=testing)
    return app
