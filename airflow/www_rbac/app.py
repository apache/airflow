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
import socket
import six

from flask import Flask
from flask_appbuilder import AppBuilder, SQLA
from flask_caching import Cache
from flask_wtf.csrf import CSRFProtect
from six.moves.urllib.parse import urlparse
from werkzeug.wsgi import DispatcherMiddleware
from werkzeug.contrib.fixers import ProxyFix

from airflow import settings
from airflow import configuration as conf
from airflow.logging_config import configure_logging
from airflow.www_rbac.static_config import configure_manifest_files

app = None
appbuilder = None
csrf = CSRFProtect()


def create_app(config=None, session=None, testing=False, app_name="Airflow"):
    global app, appbuilder
    app = Flask(__name__)
    if conf.getboolean('webserver', 'ENABLE_PROXY_FIX'):
        app.wsgi_app = ProxyFix(app.wsgi_app)
    app.secret_key = conf.get('webserver', 'SECRET_KEY')

    airflow_home_path = conf.get('core', 'AIRFLOW_HOME')
    webserver_config_path = airflow_home_path + '/webserver_config.py'
    app.config.from_pyfile(webserver_config_path, silent=True)
    app.config['APP_NAME'] = app_name
    app.config['TESTING'] = testing

    csrf.init_app(app)

    db = SQLA(app)

    from airflow import api
    api.load_auth()
    api.api_auth.init_app(app)

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
            base_template='appbuilder/baselayout.html')

        def init_views(appbuilder):
            from airflow.www_rbac import views
            appbuilder.add_view_no_menu(views.Airflow())
            appbuilder.add_view_no_menu(views.DagModelView())
            appbuilder.add_view_no_menu(views.ConfigurationView())
            appbuilder.add_view_no_menu(views.VersionView())
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
            appbuilder.add_link("Configurations",
                                href='/configuration',
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
            appbuilder.add_link("Documentation",
                                href='https://airflow.apache.org/',
                                category="Docs",
                                category_icon="fa-cube")
            appbuilder.add_link("Github",
                                href='https://github.com/apache/incubator-airflow',
                                category="Docs")
            appbuilder.add_link('Version',
                                href='/version',
                                category='About',
                                category_icon='fa-th')

            # Garbage collect old permissions/views after they have been modified.
            # Otherwise, when the name of a view or menu is changed, the framework
            # will add the new Views and Menus names to the backend, but will not
            # delete the old ones.

        init_views(appbuilder)

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

        @app.context_processor
        def jinja_globals():
            return {
                'hostname': socket.getfqdn(),
                'navbar_color': conf.get('webserver', 'NAVBAR_COLOR'),
            }

        @app.teardown_appcontext
        def shutdown_session(exception=None):
            settings.Session.remove()

    return app, appbuilder


def root_app(env, resp):
    resp(b'404 Not Found', [(b'Content-Type', b'text/plain')])
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
    cached_app(config, testing)
    return appbuilder
