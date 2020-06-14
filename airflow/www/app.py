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
import socket
from datetime import timedelta
from typing import Optional
from urllib.parse import urlparse

import pendulum
from flask import Flask
from flask_appbuilder import SQLA
from flask_caching import Cache
from flask_wtf.csrf import CSRFProtect
from werkzeug.middleware.dispatcher import DispatcherMiddleware
from werkzeug.middleware.proxy_fix import ProxyFix

from airflow import settings, version
from airflow.configuration import conf
from airflow.logging_config import configure_logging
from airflow.utils.json import AirflowJsonEncoder
from airflow.www.extensions.init_appbuilder import init_appbuilder
from airflow.www.extensions.init_session import init_logout_timeout, init_permanent_session
from airflow.www.extensions.init_views import (
    init_api_connexion, init_api_experimental, init_appbuilder_views, init_error_handlers, init_flash_views,
    init_plugins,
)
from airflow.www.static_config import configure_manifest_files

app: Optional[Flask] = None


def root_app(env, resp):
    resp(b'404 Not Found', [('Content-Type', 'text/plain')])
    return [b'Apache Airflow is not at this location']


def init_appbuilder_links(app):
    appbuilder = app.appbuilder
    if "dev" in version.version:
        airflow_doc_site = "https://airflow.readthedocs.io/en/latest"
    else:
        airflow_doc_site = 'https://airflow.apache.org/docs/{}'.format(version.version)

    appbuilder.add_link("Website",
                        href='https://airflow.apache.org',
                        category="Docs",
                        category_icon="fa-globe")
    appbuilder.add_link("Documentation",
                        href=airflow_doc_site,
                        category="Docs",
                        category_icon="fa-cube")
    appbuilder.add_link("GitHub",
                        href='https://github.com/apache/airflow',
                        category="Docs")
    appbuilder.add_link("REST API Reference",
                        href='/api/v1./api/v1_swagger_ui_index',
                        category="Docs")


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

    def apply_middlewares(flask_app: Flask):
        # Apply DispatcherMiddleware
        base_url = urlparse(conf.get('webserver', 'base_url'))[2]
        if not base_url or base_url == '/':
            base_url = ""
        if base_url:
            flask_app.wsgi_app = DispatcherMiddleware(  # type: ignore
                root_app,
                mounts={base_url: flask_app.wsgi_app}
            )

        # Apply ProxyFix middleware
        if conf.getboolean('webserver', 'ENABLE_PROXY_FIX'):
            flask_app.wsgi_app = ProxyFix(  # type: ignore
                flask_app.wsgi_app,
                x_for=conf.getint("webserver", "PROXY_FIX_X_FOR", fallback=1),
                x_proto=conf.getint("webserver", "PROXY_FIX_X_PROTO", fallback=1),
                x_host=conf.getint("webserver", "PROXY_FIX_X_HOST", fallback=1),
                x_port=conf.getint("webserver", "PROXY_FIX_X_PORT", fallback=1),
                x_prefix=conf.getint("webserver", "PROXY_FIX_X_PREFIX", fallback=1)
            )

    apply_middlewares(app)

    db = SQLA()
    db.session = settings.Session
    db.init_app(app)

    def init_api_experimental_auth():
        from airflow import api
        api.load_auth()
        api.API_AUTH.api_auth.init_app(app)
    init_api_experimental_auth()

    Cache(app=app, config={'CACHE_TYPE': 'filesystem', 'CACHE_DIR': '/tmp'})

    init_flash_views(app)

    configure_logging()
    configure_manifest_files(app)

    with app.app_context():
        def init_jinja_globals(app):
            server_timezone = conf.get('core', 'default_timezone')
            if server_timezone == "system":
                server_timezone = pendulum.local_timezone().name
            elif server_timezone == "utc":
                server_timezone = "UTC"

            default_ui_timezone = conf.get('webserver', 'default_ui_timezone')
            if default_ui_timezone == "system":
                default_ui_timezone = pendulum.local_timezone().name
            elif default_ui_timezone == "utc":
                default_ui_timezone = "UTC"
            if not default_ui_timezone:
                default_ui_timezone = server_timezone

            @app.context_processor
            def jinja_globals():  # pylint: disable=unused-variable

                globals = {
                    'server_timezone': server_timezone,
                    'default_ui_timezone': default_ui_timezone,
                    'hostname': socket.getfqdn() if conf.getboolean(
                        'webserver', 'EXPOSE_HOSTNAME', fallback=True) else 'redact',
                    'navbar_color': conf.get(
                        'webserver', 'NAVBAR_COLOR'),
                    'log_fetch_delay_sec': conf.getint(
                        'webserver', 'log_fetch_delay_sec', fallback=2),
                    'log_auto_tailing_offset': conf.getint(
                        'webserver', 'log_auto_tailing_offset', fallback=30),
                    'log_animation_speed': conf.getint(
                        'webserver', 'log_animation_speed', fallback=1000)
                }

                if 'analytics_tool' in conf.getsection('webserver'):
                    globals.update({
                        'analytics_tool': conf.get('webserver', 'ANALYTICS_TOOL'),
                        'analytics_id': conf.get('webserver', 'ANALYTICS_ID')
                    })

                return globals

        def init_xframe_protection(app):
            @app.after_request
            def apply_caching(response):
                _x_frame_enabled = conf.getboolean('webserver', 'X_FRAME_ENABLED', fallback=True)
                if not _x_frame_enabled:
                    response.headers["X-Frame-Options"] = "DENY"
                return response

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
