from flask import Blueprint
from airflow.plugins_manager import AirflowPlugin

# Creating a flask blueprint to intergrate the templates and static folder
bp = Blueprint(
    "test_plugin", __name__,
    template_folder='templates'
)


class ViewsPlugin(AirflowPlugin):
    name = "views_plugin"
    flask_blueprints = [bp]
