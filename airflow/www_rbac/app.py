# -*- coding: utf-8 -*-
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
import logging
import socket
import os
from datetime import timedelta
from typing import Any

import flask
import flask_login
import six
import pendulum
from flask import Flask, session as flask_session
from flask_appbuilder import AppBuilder, SQLA
from flask_caching import Cache
from flask_wtf.csrf import CSRFProtect
from six.moves.urllib.parse import urlparse
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.middleware.dispatcher import DispatcherMiddleware

from airflow import settings, version
from airflow.configuration import conf
from airflow.logging_config import configure_logging
from airflow.settings import STATE_COLORS
from airflow.www_rbac.static_config import configure_manifest_files

app = None  # type: Any
appbuilder = None
csrf = CSRFProtect()

log = logging.getLogger(__name__)


def create_app(config=None, session=None, testing=False, app_name="Airflow"):
    global app, appbuilder
    app = Flask(__name__)
    if conf.getboolean('webserver', 'ENABLE_PROXY_FIX'):
        app.wsgi_app = ProxyFix(
            app.wsgi_app,
            num_proxies=conf.get("webserver", "PROXY_FIX_NUM_PROXIES", fallback=None),
            x_for=conf.getint("webserver", "PROXY_FIX_X_FOR", fallback=1),
            x_proto=conf.getint("webserver", "PROXY_FIX_X_PROTO", fallback=1),
            x_host=conf.getint("webserver", "PROXY_FIX_X_HOST", fallback=1),
            x_port=conf.getint("webserver", "PROXY_FIX_X_PORT", fallback=1),
            x_prefix=conf.getint("webserver", "PROXY_FIX_X_PREFIX", fallback=1)
        )
    app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(minutes=settings.get_session_lifetime_config())

    app.secret_key = conf.get('webserver', 'SECRET_KEY')

    app.config.from_pyfile(settings.WEBSERVER_CONFIG, silent=True)
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['APP_NAME'] = app_name
    app.config['TESTING'] = testing

    app.config['SESSION_COOKIE_HTTPONLY'] = True
    app.config['SESSION_COOKIE_SECURE'] = conf.getboolean('webserver', 'COOKIE_SECURE')
    app.config['SESSION_COOKIE_SAMESITE'] = conf.get('webserver', 'COOKIE_SAMESITE')

    if config:
        app.config.from_mapping(config)

    if 'SQLALCHEMY_ENGINE_OPTIONS' not in app.config:
        app.config['SQLALCHEMY_ENGINE_OPTIONS'] = settings.prepare_engine_args()

    csrf.init_app(app)

    db = SQLA(app)
    from airflow.utils.sqlalchemy import setup_event_handlers
    setup_event_handlers(db.session.get_bind())

    from airflow import api
    api.load_auth()
    api.API_AUTH.api_auth.init_app(app)

    # flake8: noqa: F841
    cache = Cache(app=app, config={'CACHE_TYPE': 'filesystem', 'CACHE_DIR': '/tmp'})

    from airflow.www_rbac.blueprints import routes
    app.register_blueprint(routes)

    configure_logging()
    configure_manifest_files(app)

    with app.app_context():
        from airflow.www_rbac.security import AirflowSecurityManager
        security_manager_class = app.config.get('SECURITY_MANAGER_CLASS') or \
            AirflowSecurityManager

        if not issubclass(security_manager_class, AirflowSecurityManager):
            raise Exception(
                """Your CUSTOM_SECURITY_MANAGER must now extend AirflowSecurityManager,
                 not FAB's security manager.""")

        appbuilder = AppBuilder(
            app,
            db.session if not session else session,
            security_manager_class=security_manager_class,
            base_template='airflow/master.html',
            update_perms=conf.getboolean('webserver', 'UPDATE_FAB_PERMS'))

        def init_views(appbuilder):
            from airflow.www_rbac import views
            # Remove the session from scoped_session registry to avoid
            # reusing a session with a disconnected connection
            appbuilder.session.remove()
            appbuilder.add_view_no_menu(views.Airflow())
            appbuilder.add_view_no_menu(views.DagModelView())
            appbuilder.add_view(views.DagRunModelView,
                                "DAG Runs",
                                category="Browse",
                                category_icon="fa-globe")
            appbuilder.add_view(views.JobModelView,
                                "Jobs",
                                category="Browse")
            appbuilder.add_view(views.LogModelView,
                                "Logs",
                                category="Browse")
            appbuilder.add_view(views.SlaMissModelView,
                                "SLA Misses",
                                category="Browse")
            appbuilder.add_view(views.TaskInstanceModelView,
                                "Task Instances",
                                category="Browse")
            appbuilder.add_view(views.TaskRescheduleModelView,
                                "Task Reschedules",
                                category="Browse")
            appbuilder.add_view(views.ConfigurationView,
                                "Configurations",
                                category="Admin",
                                category_icon="fa-user")
            appbuilder.add_view(views.ConnectionModelView,
                                "Connections",
                                category="Admin")
            appbuilder.add_view(views.PoolModelView,
                                "Pools",
                                category="Admin")
            appbuilder.add_view(views.VariableModelView,
                                "Variables",
                                category="Admin")
            appbuilder.add_view(views.XComModelView,
                                "XComs",
                                category="Admin")

            if "dev" in version.version:
                airflow_doc_site = "https://s.apache.org/airflow-docs"
            else:
                airflow_doc_site = 'https://airflow.apache.org/docs/apache-airflow/{}'.format(version.version)

            appbuilder.add_link("Documentation",
                                href=airflow_doc_site,
                                category="Docs",
                                category_icon="fa-cube")
            appbuilder.add_link("GitHub",
                                href='https://github.com/apache/airflow',
                                category="Docs")
            appbuilder.add_view(views.VersionView,
                                'Version',
                                category='About',
                                category_icon='fa-th')

            def integrate_plugins():
                """Integrate plugins to the context"""
                from airflow.plugins_manager import (
                    flask_appbuilder_views, flask_appbuilder_menu_links
                )

                for v in flask_appbuilder_views:
                    log.debug("Adding view %s", v["name"])
                    appbuilder.add_view(v["view"],
                                        v["name"],
                                        category=v["category"])
                for ml in sorted(flask_appbuilder_menu_links, key=lambda x: x["name"]):
                    log.debug("Adding menu link %s", ml["name"])
                    appbuilder.add_link(ml["name"],
                                        href=ml["href"],
                                        category=ml["category"],
                                        category_icon=ml["category_icon"])

            integrate_plugins()
            # Garbage collect old permissions/views after they have been modified.
            # Otherwise, when the name of a view or menu is changed, the framework
            # will add the new Views and Menus names to the backend, but will not
            # delete the old ones.

        def init_plugin_blueprints(app):
            from airflow.plugins_manager import flask_blueprints

            for bp in flask_blueprints:
                log.debug("Adding blueprint %s:%s", bp["name"], bp["blueprint"].import_name)
                app.register_blueprint(bp["blueprint"])

        init_views(appbuilder)
        init_plugin_blueprints(app)

        if conf.getboolean('webserver', 'UPDATE_FAB_PERMS'):
            security_manager = appbuilder.sm
            security_manager.sync_roles()

        from airflow.www_rbac.api.experimental import endpoints as e
        # required for testing purposes otherwise the module retains
        # a link to the default_auth
        if app.config['TESTING']:
            if six.PY2:
                reload(e) # noqa
            else:
                import importlib
                importlib.reload(e)

        app.register_blueprint(e.api_experimental, url_prefix='/api/experimental')

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
                'navbar_color': conf.get('webserver', 'NAVBAR_COLOR'),
                'log_fetch_delay_sec': conf.getint(
                    'webserver', 'log_fetch_delay_sec', fallback=2),
                'log_auto_tailing_offset': conf.getint(
                    'webserver', 'log_auto_tailing_offset', fallback=30),
                'log_animation_speed': conf.getint(
                    'webserver', 'log_animation_speed', fallback=1000),
                'state_color_mapping': STATE_COLORS
            }

            if 'analytics_tool' in conf.getsection('webserver'):
                globals.update({
                    'analytics_tool': conf.get('webserver', 'ANALYTICS_TOOL'),
                    'analytics_id': conf.get('webserver', 'ANALYTICS_ID')
                })

            return globals

        @app.teardown_appcontext
        def shutdown_session(exception=None):
            settings.Session.remove()

        @app.after_request
        def apply_caching(response):
            _x_frame_enabled = conf.getboolean('webserver', 'X_FRAME_ENABLED', fallback=True)
            if not _x_frame_enabled:
                response.headers["X-Frame-Options"] = "DENY"
            return response

        @app.before_request
        def make_session_permanent():
            flask_session.permanent = True

    return app, appbuilder


def root_app(env, resp):
    resp('404 Not Found', [('Content-Type', 'text/plain')])
    return [b'Apache Airflow is not at this location']


def cached_app(config=None, session=None, testing=False):
    global app, appbuilder
    if not app or not appbuilder:
        base_url = urlparse(conf.get('webserver', 'base_url'))[2]
        if not base_url or base_url == '/':
            base_url = ""

        app, _ = create_app(config, session, testing)
        app = DispatcherMiddleware(root_app, {base_url: app})
    return app


def cached_appbuilder(config=None, testing=False):
    global appbuilder
    cached_app(config=config, testing=testing)
    return appbuilder
