from flask import Blueprint
from airflow.plugins_manager import AirflowPlugin

# Creating a flask blueprint to intergrate the templates and static folder
bp = Blueprint(
    "test_plugin", __name__,
    template_folder='templates',
)

bp_css = Blueprint(
    "test_plugin_css", __name__,
    static_folder='static/css',
    static_url_path='/static/plugin/css'
)

bp_js = Blueprint(
    "test_plugin_js", __name__,
    static_folder='static/js',
    static_url_path='/static/plugin/js'
)

bp_img = Blueprint(
    "test_plugin_img", __name__,
    static_folder='static/img',
    static_url_path='/static/plugin/img'
)


class ViewsPlugin(AirflowPlugin):
    name = "views_plugin"
    flask_blueprints = [bp, bp_css, bp_js, bp_img]
