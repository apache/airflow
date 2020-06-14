import logging
from os import path

import connexion
from connexion import ProblemException
from flask import Flask

log = logging.getLogger(__name__)

# airflow/www/extesions/init_views.py => airflow/
ROOT_APP_DIR = path.abspath(path.join(path.dirname(__file__), path.pardir, path.pardir))


def init_flash_views(app):
    from airflow.www.blueprints import routes
    app.register_blueprint(routes)


def init_appbuilder_views(app):
    appbuilder = app.appbuilder
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
    appbuilder.add_view(views.VersionView,
                        'Version',
                        category='About',
                        category_icon='fa-th')


def init_plugins(app):
    from airflow import plugins_manager

    plugins_manager.initialize_web_ui_plugins()

    appbuilder = app.appbuilder

    for v in plugins_manager.flask_appbuilder_views:
        log.debug("Adding view %s", v["name"])
        appbuilder.add_view(v["view"],
                            v["name"],
                            category=v["category"])

    for ml in sorted(plugins_manager.flask_appbuilder_menu_links, key=lambda x: x["name"]):
        log.debug("Adding menu link %s", ml["name"])
        appbuilder.add_link(ml["name"],
                            href=ml["href"],
                            category=ml["category"],
                            category_icon=ml["category_icon"])

    for bp in plugins_manager.flask_blueprints:
        log.debug("Adding blueprint %s:%s", bp["name"], bp["blueprint"].import_name)
        app.register_blueprint(bp["blueprint"])


def init_error_handlers(app: Flask):
    from airflow.www import views
    app.register_error_handler(500, views.show_traceback)
    app.register_error_handler(404, views.circles)


def init_api_connexion(app: Flask):
    spec_dir = path.join(ROOT_APP_DIR, 'api_connexion', 'openapi')
    connexion_app = connexion.App(__name__, specification_dir=spec_dir, skip_error_handlers=True)
    connexion_app.app = app
    connexion_app.add_api(
        specification='v1.yaml',
        base_path='/api/v1',
        validate_responses=True,
        strict_validation=False
    )
    app.register_error_handler(ProblemException, connexion_app.common_error_handler)


def init_api_experimental(app):
    from airflow.www.api.experimental import endpoints as e
    # required for testing purposes otherwise the module retains
    # a link to the default_auth
    if app.config['TESTING']:
        import importlib
        importlib.reload(e)

    app.register_blueprint(e.api_experimental, url_prefix='/api/experimental')
    app.extensions['csrf'].exempt(e.api_experimental)
