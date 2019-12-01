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
"""Flask application for Airflow Webserver."""
import logging
import socket
from typing import Callable, Dict, List, Optional, Tuple, Type, Union
from urllib.parse import urlparse

from flask import Flask
from flask_appbuilder import SQLA, AppBuilder
from flask_caching import Cache
from flask_wtf.csrf import CSRFProtect
from sqlalchemy.orm import Session
from werkzeug.middleware.dispatcher import DispatcherMiddleware
from werkzeug.middleware.proxy_fix import ProxyFix

from airflow import AirflowException, settings, version
from airflow.configuration import conf
from airflow.logging_config import configure_logging
from airflow.utils.json import AirflowJsonEncoder
from airflow.www.static_config import configure_manifest_files

# noinspection PyTypeChecker
app: Flask = None  # type: ignore  # app will always be set when app is used
appbuilder: Optional[AppBuilder] = None
csrf = CSRFProtect()

log = logging.getLogger(__name__)


def set_app(_app: Flask):
    """Sets flask application as globally accessible variable."""
    global app  # pylint: disable = global-statement
    app = _app


def set_appbuilder(_appbuilder: AppBuilder):
    """Sets appbuilder as globally accessible variable."""
    global appbuilder  # pylint: disable = global-statement
    appbuilder = _appbuilder


def add_views():
    """Adds views to the appbuilder."""
    from airflow.www import views
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
        airflow_doc_site = "https://airflow.readthedocs.io/en/latest"
    else:
        airflow_doc_site = 'https://airflow.apache.org/docs/{}'.format(version.version)

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
    """Integrate plugins to the appbuilder"""

    from airflow.plugins_manager import flask_appbuilder_views, flask_appbuilder_menu_links
    for v in flask_appbuilder_views:
        log.debug("Adding view %s", v["name"])
        appbuilder.add_view(v["view"],
                            v["name"],
                            category=v["category"])
    for menu_link in sorted(flask_appbuilder_menu_links, key=lambda x: x["name"]):
        log.debug("Adding menu link %s", menu_link["name"])
        appbuilder.add_link(menu_link["name"],
                            href=menu_link["href"],
                            category=menu_link["category"],
                            category_icon=menu_link["category_icon"])


def init_plugin_blueprints():
    """Initializes blueprints from all plugins."""
    from airflow.plugins_manager import flask_blueprints
    for blueprint in flask_blueprints:
        log.debug("Adding blueprint %s:%s", blueprint["name"], blueprint["blueprint"].import_name)
        app.register_blueprint(blueprint["blueprint"])


def update_fab_permissions():
    """Updates FAB permissions for the security manager."""
    if conf.getboolean('webserver', 'UPDATE_FAB_PERMS'):
        security_manager = appbuilder.sm
        security_manager.sync_roles()


def create_app(config: Optional[Dict[str, Union[str, bool]]] = None,
               session: Optional[Session] = None,
               testing: bool = False,
               app_name: str = "Airflow"):
    """Creates Flask application based on configuration."""
    set_app(Flask(__name__))
    if not app:
        raise AirflowException("The app cannot be None.")
    configure_app(app_name, config, testing)

    # Configure the JSON encoder used by `|tojson` filter from Flask
    app.json_encoder = AirflowJsonEncoder

    csrf.init_app(app)

    db = SQLA(app)

    from airflow import api
    api.load_auth()
    api.API_AUTH.api_auth.init_app(app)

    Cache(app=app, config={'CACHE_TYPE': 'filesystem', 'CACHE_DIR': '/tmp'})

    from airflow.www.blueprints import routes
    app.register_blueprint(routes)

    configure_logging()
    configure_manifest_files(app)

    with app.app_context():
        @app.context_processor
        def jinja_globals() -> Dict[str, str]:  # pylint: disable=unused-variable
            """Returns global context variables for JINJA."""
            global_vars = {
                'hostname': socket.getfqdn(),
                'navbar_color': conf.get('webserver', 'NAVBAR_COLOR'),
            }

            if 'analytics_tool' in conf.getsection('webserver'):
                global_vars.update({
                    'analytics_tool': conf.get('webserver', 'ANALYTICS_TOOL'),
                    'analytics_id': conf.get('webserver', 'ANALYTICS_ID')
                })

            return global_vars

        @app.teardown_appcontext
        def shutdown_session(_):  # pylint: disable=unused-variable
            """Shuts down the Sqlalchemy session."""
            settings.Session.remove()

        from airflow.www.security import AirflowSecurityManager

        def get_security_manager() -> Type[AirflowSecurityManager]:
            """Retrieves security manager from configuration."""
            security_manager_class = app.config.get('SECURITY_MANAGER_CLASS') or AirflowSecurityManager

            if not issubclass(security_manager_class, AirflowSecurityManager):
                raise Exception(
                    f"Your custom security manager class {security_manager_class} "
                    "must extend AirflowSecurityManager, not just FAB's security manager.")
            return security_manager_class

        set_appbuilder(AppBuilder(
            app,
            db.session if not session else session,
            security_manager_class=get_security_manager(),
            base_template='appbuilder/baselayout.html',
            update_perms=conf.getboolean('webserver', 'UPDATE_FAB_PERMS'))
        )

        if not appbuilder:
            raise AirflowException("The appbuilder should not be None.")

        add_views()
        integrate_plugins()
        init_plugin_blueprints()
        update_fab_permissions()

        from airflow.www.api.experimental import endpoints

        # required for testing purposes otherwise the module retains
        # a link to the default_auth
        if app.config['TESTING']:
            import importlib
            importlib.reload(endpoints)

        app.register_blueprint(endpoints.api_experimental, url_prefix='/api/experimental')

    return app, appbuilder


def configure_app(app_name: str, config: Optional[Dict[str, Union[str, bool]]], testing: bool):
    """Configure the application."""
    if conf.getboolean('webserver', 'ENABLE_PROXY_FIX'):
        app.wsgi_app = ProxyFix(  # type: ignore
            app.wsgi_app,
            num_proxies=None,
            x_for=1,
            x_proto=1,
            x_host=1,
            x_port=1,
            x_prefix=1
        )
    app.secret_key = conf.get('webserver', 'SECRET_KEY')
    app.config.from_pyfile(settings.WEBSERVER_CONFIG, silent=True)
    app.config['APP_NAME'] = app_name
    app.config['TESTING'] = testing
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['SESSION_COOKIE_HTTPONLY'] = True
    app.config['SESSION_COOKIE_SECURE'] = conf.getboolean('webserver', 'COOKIE_SECURE')
    app.config['SESSION_COOKIE_SAMESITE'] = conf.get('webserver', 'COOKIE_SAMESITE')
    if config:
        app.config.from_mapping(config)


def root_app(_, resp: Callable[[bytes, List[Tuple[str, str]]], None]):
    """Root application function - returns 404 if the path is not found."""
    resp(b'404 Not Found', [('Content-Type', 'text/plain')])
    return [b'Apache Airflow is not at this location']


def cached_app(config: Optional[Dict[str, Union[str, bool]]] = None,
               session: Optional[Session] = None,
               testing: bool = False) -> Flask:
    """Returns cached application if it's there. Creates it otherwise."""
    if not app or not appbuilder:
        base_url = urlparse(conf.get('webserver', 'base_url'))[2]
        if not base_url or base_url == '/':
            base_url = ""

        an_app, _ = create_app(config, session, testing)
        set_app(DispatcherMiddleware(root_app, {base_url: an_app}))  # type: ignore
        if not app:
            raise AirflowException("The app cannot be None")
    return app


def cached_appbuilder(config: Optional[Dict[str, Union[str, bool]]] = None, testing=False) -> \
        AppBuilder:
    """Returns cached appbuilder."""
    cached_app(config=config, testing=testing)
    return appbuilder
