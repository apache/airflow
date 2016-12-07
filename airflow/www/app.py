# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import logging
import socket
import six

from flask import Flask
from flask_admin import Admin, base
from flask_cache import Cache
from flask_wtf.csrf import CsrfProtect

csrf = CsrfProtect()

import airflow
from airflow import models
from airflow.settings import Session

from airflow.www.blueprints import routes
from airflow import jobs
from airflow import settings
from airflow import configuration

def create_app(config=None, testing=False):
    app = Flask(__name__)
    app.secret_key = configuration.get('webserver', 'SECRET_KEY')
    app.config['LOGIN_DISABLED'] = not configuration.getboolean('webserver', 'AUTHENTICATE')

    csrf.init_app(app)

    app.config['TESTING'] = testing

    airflow.load_login()
    airflow.login.login_manager.init_app(app)

    from airflow import api
    api.load_auth()
    api.api_auth.init_app(app)

    cache = Cache(
        app=app, config={'CACHE_TYPE': 'filesystem', 'CACHE_DIR': '/tmp'})

    app.register_blueprint(routes)

    log_format = airflow.settings.LOG_FORMAT_WITH_PID
    airflow.settings.configure_logging(log_format=log_format)

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

        # DAGs required views
        av(vs.Airflow(name='DAGs', category='DAGs'))

        # Data Profiling required view
        av(vs.KnowEventView(
            models.KnownEvent, Session, name="Known Events", category="Data Profiling"))

        # Data Profiling optional views
        if not configuration.has_expected_value('suppressible_default_views', 'data_profiling.ad_hoc_query', 'F'):
            av(vs.QueryView(name='Ad Hoc Query', category="Data Profiling"))

        if not configuration.has_expected_value('suppressible_default_views', 'data_profiling.charts', 'F'):
            av(vs.ChartModelView(models.Chart, Session, name="Charts", category="Data Profiling"))

        # Browse required views
        av(vs.DagRunModelView(models.DagRun, Session, name="DAG Runs", category="Browse"))

        # Browse optional views
        if not configuration.has_expected_value('suppressible_default_views', 'browse.sla_misses', 'F'):
            av(vs.SlaMissModelView(models.SlaMiss, Session, name="SLA Misses", category="Browse"))

        if not configuration.has_expected_value('suppressible_default_views', 'browse.task_instances', 'F'):
            av(vs.TaskInstanceModelView(
                models.TaskInstance, Session, name="Task Instances", category="Browse"))

        if not configuration.has_expected_value('suppressible_default_views', 'browse.logs', 'F'):
            av(vs.LogModelView(models.Log, Session, name="Logs", category="Browse"))

        if not configuration.has_expected_value('suppressible_default_views', 'browse.jobs', 'F'):
            av(vs.JobModelView(jobs.BaseJob, Session, name="Jobs", category="Browse"))

        # Admin optional views
        if not configuration.has_expected_value('suppressible_default_views', 'browse.pools', 'F'):
            av(vs.PoolModelView(models.Pool, Session, name="Pools", category="Admin"))

        if not configuration.has_expected_value('suppressible_default_views', 'admin.configuration', 'F'):
            av(vs.ConfigurationView(name='Configuration', category="Admin"))

        if not configuration.has_expected_value('suppressible_default_views', 'admin.users', 'F'):
            av(vs.UserModelView(models.User, Session, name="Users", category="Admin"))

        if not configuration.has_expected_value('suppressible_default_views', 'admin.connections', 'F'):
            av(vs.ConnectionModelView(models.Connection, Session, name="Connections", category="Admin"))

        if not configuration.has_expected_value('suppressible_default_views', 'admin.variables', 'F'):
            av(vs.VariableView(models.Variable, Session, name="Variables", category="Admin"))

        if not configuration.has_expected_value('suppressible_default_views', 'admin.xcoms', 'F'):
            av(vs.XComView(models.XCom, Session, name="XComs", category="Admin"))

        # About optional views
        if not configuration.has_expected_value('suppressible_default_views', 'about.versions', 'F'):
            av(vs.VersionView(name='Version', category="About"))

        av(vs.DagModelView(models.DagModel, Session, name=None))
        # Hack to not add this view to the menu
        admin._menu = admin._menu[:-1]

        # Menu links
        # Docs required views
        admin.add_link(base.MenuLink(category='Docs', name='Documentation',
                                     url='http://pythonhosted.org/airflow/'))
        admin.add_link(base.MenuLink(category='Docs', name='Github',
                                     url='https://github.com/airbnb/airflow'))

        # Add custom links
        for key in configuration.as_dict()['custom_external_links']:
            # Don't care about the key itself
            # Take value as <category_name>::<tab_name>::<tab_link>
            v = configuration.get('custom_external_links', key).split("::")
            admin.add_link(base.MenuLink(category=v[0], name=v[1], url=v[2]))

        def integrate_plugins():
            """Integrate plugins to the context"""
            from airflow.plugins_manager import (
                admin_views, flask_blueprints, menu_links)
            for v in admin_views:
                logging.info('Adding view ' + v.name)
                admin.add_view(v)
            for bp in flask_blueprints:
                logging.info('Adding blueprint ' + bp.name)
                app.register_blueprint(bp)
            for ml in sorted(menu_links, key=lambda x: x.name):
                logging.info('Adding menu link ' + ml.name)
                admin.add_link(ml)

        integrate_plugins()

        import airflow.www.api.experimental.endpoints as e
        # required for testing purposes otherwise the module retains
        # a link to the default_auth
        if app.config['TESTING']:
            if six.PY2:
                reload(e)
            else:
                import importlib
                importlib.reload(e)

        app.register_blueprint(e.api_experimental, url_prefix='/api/experimental')

        @app.context_processor
        def jinja_globals():
            return {
                'hostname': socket.getfqdn(),
            }

        @app.teardown_appcontext
        def shutdown_session(exception=None):
            settings.Session.remove()

        return app


app = None


def cached_app(config=None):
    global app
    if not app:
        app = create_app(config)
    return app
