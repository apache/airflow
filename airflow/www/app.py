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
import six

from flask import Flask
from flask_admin import Admin, base
from flask_caching import Cache
from flask_wtf.csrf import CSRFProtect
from six.moves.urllib.parse import urlparse
from werkzeug.wsgi import DispatcherMiddleware
from werkzeug.contrib.fixers import ProxyFix

import airflow
from airflow import configuration as conf
from airflow import models, LoggingMixin
from airflow.settings import Session

from airflow.www.blueprints import routes
from airflow.logging_config import configure_logging
from airflow import jobs
from airflow import settings
from airflow import configuration
from airflow.utils.net import get_hostname

csrf = CSRFProtect()


def create_app(config=None, testing=False):

    log = LoggingMixin().log

    app = Flask(__name__)
    if configuration.conf.getboolean('webserver', 'ENABLE_PROXY_FIX'):
        app.wsgi_app = ProxyFix(app.wsgi_app)
    app.secret_key = configuration.conf.get('webserver', 'SECRET_KEY')
    app.config['LOGIN_DISABLED'] = not configuration.conf.getboolean(
        'webserver', 'AUTHENTICATE')

    csrf.init_app(app)

    app.config['TESTING'] = testing

    airflow.load_login()
    airflow.login.login_manager.init_app(app)

    from airflow import api
    api.load_auth()
    api.api_auth.init_app(app)

    # flake8: noqa: F841
    cache = Cache(app=app, config={'CACHE_TYPE': 'filesystem', 'CACHE_DIR': '/tmp'})

    app.register_blueprint(routes)

    configure_logging()

    with app.app_context():
        from airflow.www import views

        admin = Admin(
            app, name='Airflow',
            static_url_path='/admin',
            index_view=views.HomeView(endpoint='', url='/admin', name="DAGs"),
            template_mode='bootstrap3',
        )
        av = admin.add_view
        vs = views
        av(vs.Airflow(name='DAGs', category='DAGs'))

        if not conf.getboolean('core', 'secure_mode'):
            av(vs.QueryView(name='Ad Hoc Query', category="Data Profiling"))
            av(vs.ChartModelView(
                models.Chart, Session, name="Charts", category="Data Profiling"))
        av(vs.KnownEventView(
            models.KnownEvent,
            Session, name="Known Events", category="Data Profiling"))
        av(vs.SlaMissModelView(
            models.SlaMiss,
            Session, name="SLA Misses", category="Browse"))
        av(vs.TaskInstanceModelView(models.TaskInstance,
            Session, name="Task Instances", category="Browse"))
        av(vs.LogModelView(
            models.Log, Session, name="Logs", category="Browse"))
        av(vs.JobModelView(
            jobs.BaseJob, Session, name="Jobs", category="Browse"))
        av(vs.PoolModelView(
            models.Pool, Session, name="Pools", category="Admin"))
        av(vs.ConfigurationView(
            name='Configuration', category="Admin"))
        av(vs.UserModelView(
            models.User, Session, name="Users", category="Admin"))
        av(vs.ConnectionModelView(
            models.Connection, Session, name="Connections", category="Admin"))
        av(vs.VariableView(
            models.Variable, Session, name="Variables", category="Admin"))
        av(vs.XComView(
            models.XCom, Session, name="XComs", category="Admin"))

        admin.add_link(base.MenuLink(
            category='Docs', name='Documentation',
            url='https://airflow.incubator.apache.org/'))
        admin.add_link(
            base.MenuLink(category='Docs',
                          name='Github',
                          url='https://github.com/apache/incubator-airflow'))

        av(vs.VersionView(name='Version', category="About"))

        av(vs.DagRunModelView(
            models.DagRun, Session, name="DAG Runs", category="Browse"))
        av(vs.DagModelView(models.DagModel, Session, name=None))
        # Hack to not add this view to the menu
        admin._menu = admin._menu[:-1]

        def integrate_plugins():
            """Integrate plugins to the context"""
            from airflow.plugins_manager import (
                admin_views, flask_blueprints, menu_links)
            for v in admin_views:
                log.debug('Adding view %s', v.name)
                admin.add_view(v)
            for bp in flask_blueprints:
                log.debug('Adding blueprint %s', bp.name)
                app.register_blueprint(bp)
            for ml in sorted(menu_links, key=lambda x: x.name):
                log.debug('Adding menu link %s', ml.name)
                admin.add_link(ml)

        integrate_plugins()

        import airflow.www.api.experimental.endpoints as e
        # required for testing purposes otherwise the module retains
        # a link to the default_auth
        if app.config['TESTING']:
            six.moves.reload_module(e)

        app.register_blueprint(e.api_experimental, url_prefix='/api/experimental')

        @app.context_processor
        def jinja_globals():
            return {
                'hostname': get_hostname(),
                'navbar_color': configuration.get('webserver', 'NAVBAR_COLOR'),
            }

        @app.teardown_appcontext
        def shutdown_session(exception=None):
            settings.Session.remove()

        return app


app = None


def root_app(env, resp):
    resp(b'404 Not Found', [(b'Content-Type', b'text/plain')])
    return [b'Apache Airflow is not at this location']


def cached_app(config=None, testing=False):
    global app
    if not app:
        base_url = urlparse(configuration.conf.get('webserver', 'base_url'))[2]
        if not base_url or base_url == '/':
            base_url = ""

        app = create_app(config, testing)
        app = DispatcherMiddleware(root_app, {base_url: app})
    return app
