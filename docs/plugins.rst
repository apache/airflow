Plugins
=======

Airflow has a simple plugin manager built-in that can integrate external
features at its core by simply dropping files in your 
``$AIRFLOW_HOME/plugins`` folder.

The python modules in the ``plugins`` folder get imported, 
and **hooks**, **operators**, **macros**, **executors** and web **views** 
get integrated to Airflow's main collections and become available for use.

What for?
---------

Airflow offers a generic toolbox for working with data. Different 
organizations have different stacks and different needs. Using Airflow
plugins can be a way for companies to customize their Airflow installation
to reflect their ecosystem.

Plugins can be used as an easy way to write, share and activate new sets of 
features.

There's also a need for a set of more complex application to interact with 
different flavors of data and metadata.

Examples:

* A set of tools to parse Hive logs and expose Hive metadata (CPU /IO / phases/ skew /...)
* An anomaly detection framework, allowing people to collect metrics, set thresholds and alerts
* An auditing tool, helping understand who accesses what
* A config-driven SLA monitoring tool, allowing you to set monitored tables and at what time
  they should land, alert people and exposes visualization of outages
* ...

Why build on top Airflow?
-------------------------

Airflow has many components that can be reused when building an application:

* A web server you can use to render your views
* A metadata database to store your models
* Access to your database, and knowledge of how to connect to them
* An array of workers that can allow your application to push workload to
* Airflow is deployed, you can just piggy back on it's deployment logistics
* Basic charting capabilities, underlying libraries and abstractions


Interface
---------

To create a plugin you will need to derive the 
``airflow.plugins_manager.AirflowPlugin`` class and reference the objects
you want to plug into Airflow. Here's what the class you need to derive
look like:


.. code:: python

    class AirflowPlugin(object):
        # The name of your plugin (str)
        name = None
        # A list of class(es) derived from BaseOperator
        operators = []
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
        # A list of menu links (flask.ext.admin.base.MenuLink)
        menu_links = []


Example
-------

The code bellow defines a plugin that injects a set of dummy object
definitions in Airflow. 

.. code:: python
    
    # This is the class you derive to create a plugin
    from airflow.plugins_manager import AirflowPlugin

    from flask import Blueprint
    from flask.ext.admin import BaseView
    from flask.ext.admin.base import MenuLink

    # Importing base classes that we need to derive
    from airflow.hooks.base_hook import BaseHook
    from airflow.models import  BaseOperator
    from airflow.executors.base_executor import BaseExecutor

    # Will show up under airflow.hooks.PluginHook
    class PluginHook(BaseHook):
        pass

    # Will show up under airflow.operators.PluginOperator
    class PluginOperator(BaseOperator):
        pass

    # Will show up under airflow.executors.PluginExecutor
    class PluginExecutor(BaseExecutor):
        pass

    # Creating a flask admin BaseView
    class TestView(BaseView):
        @expose('/')
        def test(self):
            return self.render("test_plugin/test.html", content="Hello galaxy!")
    v = TestView(category="Test Plugin", name="Test View")

    # Creating a flask blueprint to intergrate the templates and static folder
    bp = Blueprint(
        "test_plugin", __name__,
        template_folder='templates',
        static_folder='static',
        static_url_path='/static/test_plugin')


    ml = MenuLink(
        category='Test Plugin',
        name='Test Menu Link',
        url='http://pythonhosted.org/airflow/')

    # Defining the plugin class
    class AirflowTestPlugin(AirflowPlugin):
        name = "test_plugin"
        operators = [PluginOperator]
        flask_blueprints = [bp]
        hooks = [PluginHook]
        executors = [PluginExecutor]
        admin_views = [v]
        menu_links = [ml]
