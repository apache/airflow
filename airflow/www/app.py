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
from airflow.www.extensions.init_jinja_globals import init_jinja_globals
from airflow.www.extensions.init_manifest_files import configure_manifest_files
from airflow.www.extensions.init_security import init_api_experimental_auth, init_xframe_protection
from airflow.www.extensions.init_session import init_logout_timeout, init_permanent_session
from airflow.www.extensions.init_views import (
    init_api_connexion, init_api_experimental, init_appbuilder_views, init_error_handlers, init_flash_views,
    init_plugins,
)
from airflow.www.extensions.init_wsgi_middlewares import init_wsg_middleware

app: Optional[Flask] = None


def sync_appbuilder_roles(app):
    # Garbage collect old permissions/views after they have been modified.
    # Otherwise, when the name of a view or menu is changed, the framework
    # will add the new Views and Menus names to the backend, but will not
    # delete the old ones.
    if conf.getboolean('webserver', 'UPDATE_FAB_PERMS'):
        security_manager = app.appbuilder.sm
        security_manager.sync_roles()


def create_app(config=None, testing=False, app_name="Airflow"):
    app = Flask(__name__)
    app.secret_key = conf.get('webserver', 'SECRET_KEY')

    session_lifetime_days = conf.getint('webserver', 'SESSION_LIFETIME_DAYS', fallback=30)
    app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=session_lifetime_days)

    app.config.from_pyfile(settings.WEBSERVER_CONFIG, silent=True)
    app.config['APP_NAME'] = app_name
    app.config['TESTING'] = testing
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

    app.config['SESSION_COOKIE_HTTPONLY'] = True
    app.config['SESSION_COOKIE_SECURE'] = conf.getboolean('webserver', 'COOKIE_SECURE')
    app.config['SESSION_COOKIE_SAMESITE'] = conf.get('webserver', 'COOKIE_SAMESITE')

    if config:
        app.config.from_mapping(config)

    # Configure the JSON encoder used by `|tojson` filter from Flask
    app.json_encoder = AirflowJsonEncoder

    CSRFProtect(app)

    init_wsg_middleware(app)

    db = SQLA()
    db.session = settings.Session
    db.init_app(app)

    init_api_experimental_auth(app)

    Cache(app=app, config={'CACHE_TYPE': 'filesystem', 'CACHE_DIR': '/tmp'})

    init_flash_views(app)

    configure_logging()
    configure_manifest_files(app)

    with app.app_context():
        init_appbuilder(app)

        init_appbuilder_views(app)
        init_appbuilder_links(app)
        init_plugins(app)
        init_error_handlers(app)
        init_api_connexion(app)
        init_api_experimental(app)

        sync_appbuilder_roles(app)

        init_jinja_globals(app)
        init_logout_timeout(app)
        init_xframe_protection(app)
        init_permanent_session(app)

    return app


def cached_app(config=None, testing=False):
    global app
    if not app:
        app = create_app(config=config, testing=testing)
    return app
