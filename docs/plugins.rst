Plugins
=======

Airflow has a simple plugin manager built-in that can integrate external
features to its core by simply dropping files in your
``$AIRFLOW_HOME/plugins`` folder.

The python modules in the ``plugins`` folder get imported,
and **hooks**, **operators**, **sensors**, **macros**, **executors** and web **views**
get integrated to Airflow's main collections and become available for use.

What for?
---------

Airflow offers a generic toolbox for working with data. Different
organizations have different stacks and different needs. Using Airflow
plugins can be a way for companies to customize their Airflow installation
to reflect their ecosystem.

Plugins can be used as an easy way to write, share and activate new sets of
features.

There's also a need for a set of more complex applications to interact with
different flavors of data and metadata.

Examples:

* A set of tools to parse Hive logs and expose Hive metadata (CPU /IO / phases/ skew /...)
* An anomaly detection framework, allowing people to collect metrics, set thresholds and alerts
* An auditing tool, helping understand who accesses what
* A config-driven SLA monitoring tool, allowing you to set monitored tables and at what time
  they should land, alert people, and expose visualizations of outages
* ...

Why build on top of Airflow?
----------------------------

Airflow has many components that can be reused when building an application:

* A web server you can use to render your views
* A metadata database to store your models
* Access to your databases, and knowledge of how to connect to them
* An array of workers that your application can push workload to
* Airflow is deployed, you can just piggy back on its deployment logistics
* Basic charting capabilities, underlying libraries and abstractions


Interface
---------

To create a plugin you will need to derive the
``airflow.plugins_manager.AirflowPlugin`` class and reference the objects
you want to plug into Airflow. Here's what the class you need to derive
looks like:


.. code:: python

    class AirflowPlugin(object):
        # The name of your plugin (str)
        name = None
        # A list of class(es) derived from BaseOperator
        operators = []
        # A list of class(es) derived from BaseSensorOperator
        sensors = []
        # A list of class(es) derived from BaseHook
        hooks = []
        # A list of class(es) derived from BaseExecutor
        executors = []
        # A list of references to inject into the macros namespace
        macros = []
        # A list of objects created from a class derived
        # from flask_admin.BaseView
        admin_views = []
        # A list of Blueprint object created from flask.Blueprint
        flask_blueprints = []
        # A list of menu links (flask_admin.base.MenuLink)
        menu_links = []


Example
-------

The code below defines a plugin that injects a set of dummy object
definitions in Airflow.

.. code:: python

    # This is the class you derive to create a plugin
    from airflow.plugins_manager import AirflowPlugin

    from flask import Blueprint
    from flask_admin import BaseView, expose
    from flask_admin.base import MenuLink

    # Importing base classes that we need to derive
    from airflow.hooks.base_hook import BaseHook
    from airflow.models import BaseOperator
    from airflow.sensors.base_sensor_operator import BaseSensorOperator
    from airflow.executors.base_executor import BaseExecutor

    # Will show up under airflow.hooks.test_plugin.PluginHook
    class PluginHook(BaseHook):
        pass

    # Will show up under airflow.operators.test_plugin.PluginOperator
    class PluginOperator(BaseOperator):
        pass

    # Will show up under airflow.sensors.test_plugin.PluginSensorOperator
    class PluginSensorOperator(BaseSensorOperator):
        pass

    # Will show up under airflow.executors.test_plugin.PluginExecutor
    class PluginExecutor(BaseExecutor):
        pass

    # Will show up under airflow.macros.test_plugin.plugin_macro
    def plugin_macro():
        pass

    # Creating a flask admin BaseView
    class TestView(BaseView):
        @expose('/')
        def test(self):
            # in this example, put your test_plugin/test.html template at airflow/plugins/templates/test_plugin/test.html
            return self.render("test_plugin/test.html", content="Hello galaxy!")
    v = TestView(category="Test Plugin", name="Test View")

    # Creating a flask blueprint to intergrate the templates and static folder
    bp = Blueprint(
        "test_plugin", __name__,
        template_folder='templates', # registers airflow/plugins/templates as a Jinja template folder
        static_folder='static',
        static_url_path='/static/test_plugin')

    ml = MenuLink(
        category='Test Plugin',
        name='Test Menu Link',
        url='https://airflow.incubator.apache.org/')

    # Defining the plugin class
    class AirflowTestPlugin(AirflowPlugin):
        name = "test_plugin"
        operators = [PluginOperator]
        sensors = [PluginSensorOperator]
        hooks = [PluginHook]
        executors = [PluginExecutor]
        macros = [plugin_macro]
        admin_views = [v]
        flask_blueprints = [bp]
        menu_links = [ml]
