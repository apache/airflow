import socket

from flask import Flask
from flask.ext.admin import Admin, base
from flask.ext.cache import Cache

from airflow import login
from airflow import models
from airflow.settings import Session

from airflow.www.blueprints import ck, routes
from airflow import jobs
from airflow import settings
from airflow.configuration import conf


def create_app(config=None):
    app = Flask(__name__)
    app.secret_key = conf.get('webserver', 'SECRET_KEY')
    #app.config = config
    login.login_manager.init_app(app)

    cache = Cache(
        app=app, config={'CACHE_TYPE': 'filesystem', 'CACHE_DIR': '/tmp'})

    app.register_blueprint(ck, url_prefix='/ck')
    app.register_blueprint(routes)
    app.jinja_env.add_extension("chartkick.ext.charts")

    with app.app_context():
        from airflow.www.views import HomeView
        admin = Admin(
            app, name='Airflow',
            static_url_path='/admin',
            index_view=HomeView(endpoint='', url='/admin'),
            template_mode='bootstrap3',
        )

        from airflow.www import views
        admin.add_view(views.Airflow(name='DAGs'))

        admin.add_view(views.SlaMissModelView(models.SlaMiss, Session, name="SLA Misses", category="Browse"))
        admin.add_view(
            views.TaskInstanceModelView(models.TaskInstance, Session, name="Task Instances", category="Browse")
        )

        admin.add_view(views.LogModelView(models.Log, Session, name="Logs", category="Browse"))
        admin.add_view(views.JobModelView(jobs.BaseJob, Session, name="Jobs", category="Browse"))

        admin.add_view(views.QueryView(name='Ad Hoc Query', category="Data Profiling"))
        admin.add_view(views.ChartModelView(models.Chart, Session, name="Charts", category="Data Profiling"))
        admin.add_view(views.KnowEventView(models.KnownEvent, Session, name="Known Events", category="Data Profiling"))

        admin.add_view(views.PoolModelView(models.Pool, Session, name="Pools", category="Admin"))
        admin.add_view(views.ConfigurationView(name='Configuration', category="Admin"))
        admin.add_view(views.UserModelView(models.User, Session, name="Users", category="Admin"))
        admin.add_view(views.ConnectionModelView(models.Connection, Session, name="Connections", category="Admin"))
        admin.add_view(views.VariableView(models.Variable, Session, name="Variables", category="Admin"))

        admin.add_link(base.MenuLink(category='Docs', name='Documentation', url='http://pythonhosted.org/airflow/'))
        admin.add_link(base.MenuLink(category='Docs',name='Github',url='https://github.com/airbnb/airflow'))

        admin.add_view(views.DagModelView(models.DagModel, Session, name=None))
        # Hack to not add this view to the menu
        admin._menu = admin._menu[:-1]

        def integrate_plugins():
            """Integrate plugins to the context"""
            from airflow.plugins_manager import (
                admin_views, flask_blueprints, menu_links)
            for v in admin_views:
                admin.add_view(v)
            for bp in flask_blueprints:
                print(bp)
                app.register_blueprint(bp)
            for ml in menu_links:
                admin.add_link(ml)

        integrate_plugins()

        @app.context_processor
        def jinja_globals():
            return {
                'hostname': socket.gethostname(),
            }

        @app.teardown_appcontext
        def shutdown_session(exception=None):
            settings.Session.remove()

        return app
