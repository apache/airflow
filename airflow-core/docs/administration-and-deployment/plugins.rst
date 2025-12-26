 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.



Plugins
========

Airflow has a simple plugin manager built-in that can integrate external
features to its core by simply dropping files in your
``$AIRFLOW_HOME/plugins`` folder.

Since Airflow 3.1, the plugin system supports new features such as React apps, FastAPI endpoints,
and middleware, making it easier to extend Airflow and build rich custom integrations.

The python modules in the ``plugins`` folder get imported, and **macros** and web **views**
get integrated to Airflow's main collections and become available for use.

To troubleshoot issues with plugins, you can use the ``airflow plugins`` command.
This command dumps information about loaded plugins.

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
* An auditing tool, helping to understand who accesses what
* A config-driven SLA monitoring tool, allowing you to set monitored tables and at what time
  they should land, alert people, and expose visualizations of outages

Why build on top of Airflow?
----------------------------

Airflow has many components that can be reused when building an application:

* A web server you can use to render your views
* A metadata database to store your models
* Access to your databases, and knowledge of how to connect to them
* An array of workers that your application can push workload to
* Airflow is deployed, you can just piggyback on its deployment logistics
* Basic charting capabilities, underlying libraries and abstractions

.. _plugins:loading:

Available Building Blocks
-------------------------

Airflow plugins can register the following components:

* *External Views* – Add buttons/tabs linking to new pages in the UI.
* *React Apps* – Embed custom React apps inside the Airflow UI (new in Airflow 3.1).
* *FastAPI Apps* – Add custom API endpoints.
* *FastAPI Middlewares* – Intercept and modify API requests/responses.
* *Macros* – Define reusable Python functions available in DAG templates.
* *Operator Extra Links* – Add custom buttons in the task details view.
* *Timetables & Listeners* – Implement custom scheduling logic and event hooks.

When are plugins (re)loaded?
----------------------------

Plugins are by default lazily loaded and once loaded, they are never reloaded (except the UI plugins are
automatically loaded in Webserver). To load them at the
start of each Airflow process, set ``[core] lazy_load_plugins = False`` in ``airflow.cfg``.

This means that if you make any changes to plugins, and you want the webserver or scheduler to use that new
code you will need to restart those processes. However, it will not be reflected in new running tasks until after the scheduler boots.

By default, task execution uses forking. This avoids the slowdown associated with creating a new Python interpreter
and re-parsing all of Airflow's code and startup routines. This approach offers significant benefits, especially for shorter tasks.
This does mean that if you use plugins in your tasks, and want them to update you will either
need to restart the worker (if using CeleryExecutor) or scheduler (LocalExecutor). The other
option is you can accept the speed hit at start up set the ``core.execute_tasks_new_python_interpreter``
config setting to True, resulting in launching a whole new python interpreter for tasks.

(Modules only imported by Dag files on the other hand do not suffer this problem, as Dag files are not
loaded/parsed in any long-running Airflow process.)

.. _plugins-interface:

Interface
---------

To create a plugin you will need to derive the
``airflow.plugins_manager.AirflowPlugin`` class and reference the objects
you want to plug into Airflow. Here's what the class you need to derive
looks like:


.. code-block:: python

    class AirflowPlugin:
        # The name of your plugin (str)
        name = None
        # A list of references to inject into the macros namespace
        macros = []
        # A list of dictionaries containing FastAPI app objects and some metadata. See the example below.
        fastapi_apps = []
        # A list of dictionaries containing FastAPI middleware factory objects and some metadata. See the example below.
        fastapi_root_middlewares = []
        # A list of dictionaries containing external views and some metadata. See the example below.
        external_views = []
        # A list of dictionaries containing react apps and some metadata. See the example below.
        # Note: React apps are only supported in Airflow 3.1 and later.
        # Note: The React app integration is experimental and interfaces might change in future versions. Particularly, dependency and state interactions between the UI and plugins may need to be refactored for more complex plugin apps.
        react_apps = []

        # A callback to perform actions when Airflow starts and the plugin is loaded.
        # NOTE: Ensure your plugin has *args, and **kwargs in the method definition
        #   to protect against extra parameters injected into the on_load(...)
        #   function in future changes
        def on_load(*args, **kwargs):
            # ... perform Plugin boot actions
            pass

        # A list of global operator extra links that can redirect users to
        # external systems. These extra links will be available on the
        # task page in the form of buttons.
        #
        # Note: the global operator extra link can be overridden at each
        # operator level.
        global_operator_extra_links = []

        # A list of operator extra links to override or add operator links
        # to existing Airflow Operators.
        # These extra links will be available on the task page in form of
        # buttons.
        operator_extra_links = []

        # A list of timetable classes to register so they can be used in Dags.
        timetables = []

        # A list of Listeners that plugin provides. Listeners can register to
        # listen to particular events that happen in Airflow, like
        # TaskInstance state changes. Listeners are python modules.
        listeners = []

You can derive it by inheritance (please refer to the example below). In the example, all options have been
defined as class attributes, but you can also define them as properties if you need to perform
additional initialization. Please note ``name`` inside this class must be specified.

Make sure you restart the webserver and scheduler after making changes to plugins so that they take effect.

Plugin Management Interface
---------------------------

Airflow 3.1 introduces a Plugin Management Interface, available under *Admin → Plugins* in the Airflow UI.
This page allows you to view installed plugins.

...

External Views
--------------

External views can also be embedded directly into the Airflow UI using iframes by providing a ``url_route`` value.
This allows you to render the view inline instead of opening it in a new browser tab.

.. _plugin-example:

Example
-------

The code below defines a plugin that injects a set of illustrative object
definitions in Airflow.

.. code-block:: python

    # This is the class you derive to create a plugin
    from airflow.plugins_manager import AirflowPlugin

    from fastapi import FastAPI
    from fastapi.middleware.trustedhost import TrustedHostMiddleware

    # Importing base classes that we need to derive
    from airflow.hooks.base import BaseHook
    from airflow.providers.amazon.aws.transfers.gcs_to_s3 import GCSToS3Operator


    # Will show up in templates through {{ macros.test_plugin.plugin_macro }}
    def plugin_macro():
        pass


    # Creating a FastAPI application to integrate in Airflow Rest API.
    app = FastAPI()


    @app.get("/")
    async def root():
        return {"message": "Hello World from FastAPI plugin"}


    app_with_metadata = {"app": app, "url_prefix": "/some_prefix", "name": "Name of the App"}


    # Creating a FastAPI middleware that will operates on all the server api requests.
    middleware_with_metadata = {
        "middleware": TrustedHostMiddleware,
        "args": [],
        "kwargs": {"allowed_hosts": ["example.com", "*.example.com"]},
        "name": "Name of the Middleware",
    }

    # Creating an external view that will be rendered in the Airflow UI.
    external_view_with_metadata = {
        # Name of the external view, this will be displayed in the UI.
        "name": "Name of the External View",
        # Source URL of the external view. This URL can be templated using context variables, depending on the location where the external view is rendered
        # the context variables available will be different, i.e a subset of (DAG_ID, RUN_ID, TASK_ID, MAP_INDEX).
        "href": "https://example.com/{DAG_ID}/{RUN_ID}/{TASK_ID}/{MAP_INDEX}",
        # Destination of the external view. This is used to determine where the view will be loaded in the UI.
        # Supported locations are Literal["nav", "dag", "dag_run", "task", "task_instance"], default to "nav".
        "destination": "dag_run",
        # Optional icon, url to an svg file.
        "icon": "https://example.com/icon.svg",
        # Optional dark icon for the dark theme, url to an svg file. If not provided, "icon" will be used for both light and dark themes.
        "icon_dark_mode": "https://example.com/dark_icon.svg",
        # Optional parameters, relative URL location for the External View rendering. If not provided, external view will be rendered as an external link. If provided
        # will be rendered inside an Iframe in the UI. Should not contain a leading slash.
        "url_route": "my_external_view",
        # Optional category, only relevant for destination "nav". This is used to group the external links in the navigation bar.  We will match the existing
        # menus of ["browse", "docs", "admin", "user"] and if there's no match then create a new menu.
        "category": "browse",
    }

    # Note: The React app integration is experimental and interfaces might change in future versions.
    react_app_with_metadata = {
        # Name of the React app, this will be displayed in the UI.
        "name": "Name of the React App",
        # Bundle URL of the React app. This is the URL where the React app is served from. It can be a static file or a CDN.
        # This URL can be templated using context variables, depending on the location where the external view is rendered
        # the context variables available will be different, i.e a subset of (DAG_ID, RUN_ID, TASK_ID, MAP_INDEX).
        "bundle_url": "https://example.com/static/js/my_react_app.js",
        # Destination of the react app. This is used to determine where the app will be loaded in the UI.
        # Supported locations are Literal["nav", "dag", "dag_run", "task", "task_instance"], default to "nav".
        # It can also be put inside of an existing page, the supported views are ["dashboard", "dag_overview", "task_overview"]. You can position
        # element in the existing page via the css `order` rule which will determine the flex order.
        "destination": "dag_run",
        # Optional icon, url to an svg file.
        "icon": "https://example.com/icon.svg",
        # Optional dark icon for the dark theme, url to an svg file. If not provided, "icon" will be used for both light and dark themes.
        "icon_dark_mode": "https://example.com/dark_icon.svg",
        # URL route for the React app, relative to the Airflow UI base URL. Should not contain a leading slash.
        "url_route": "my_react_app",
        # Optional category, only relevant for destination "nav". This is used to group the react apps in the navigation bar. We will match the existing
        # menus of ["browse", "docs", "admin", "user"] and if there's no match then create a new menu.
        "category": "browse",
    }


    # Defining the plugin class
    class AirflowTestPlugin(AirflowPlugin):
        name = "test_plugin"
        macros = [plugin_macro]
        fastapi_apps = [app_with_metadata]
        fastapi_root_middlewares = [middleware_with_metadata]
        external_views = [external_view_with_metadata]
        react_apps = [react_app_with_metadata]

.. seealso:: :doc:`/howto/define-extra-link`

Exclude views from CSRF protection
----------------------------------

We strongly suggest that you should protect all your views with CSRF. But if needed, you can exclude
some views using a decorator.

.. code-block:: python

    from airflow.www.app import csrf


    @csrf.exempt
    def my_handler():
        # ...
        return "ok"

Plugins as Python packages
--------------------------

It is possible to load plugins via `setuptools entrypoint <https://packaging.python.org/guides/creating-and-discovering-plugins/#using-package-metadata>`_ mechanism. To do this link
your plugin using an entrypoint in your package. If the package is installed, Airflow
will automatically load the registered plugins from the entrypoint list.

.. note::
    Neither the entrypoint name (eg, ``my_plugin``) nor the name of the
    plugin class will contribute towards the module and class name of the plugin
    itself.

.. code-block:: python

    # my_package/my_plugin.py
    from airflow.plugins_manager import AirflowPlugin


    class MyAirflowPlugin(AirflowPlugin):
        name = "my_namespace"

Then inside pyproject.toml:

.. code-block:: toml

    [project.entry-points."airflow.plugins"]
    my_plugin = "my_package.my_plugin:MyAirflowPlugin"

Flask Appbuilder and Flask Blueprints in Airflow 3
--------------------------------------------------

Airflow 2 supported Flask Appbuilder views (``appbuilder_views``), Flask AppBuilder menu items (``appbuilder_menu_items``),
and Flask Blueprints (``flask_blueprints``) in plugins. These have been superseded in Airflow 3 by External Views (``external_views``), Fast API apps (``fastapi_apps``),
FastAPI middlewares (``fastapi_root_middlewares``) and React apps (``react_apps``) that allow extended functionality and better integration with the Airflow UI.

All new plugins should use the new interfaces.

However, a compatibility layer is provided for Flask and FAB plugins to ease the transition to Airflow 3 - simply install the FAB provider and tweak the code
following Airflow 3 migration guide. This compatibility layer allows you to continue using your existing Flask Appbuilder views, Flask Blueprints and Flask Appbuilder menu items.

Troubleshooting
---------------

You can use `the Flask CLI <https://flask.palletsprojects.com/en/1.1.x/cli/>`__ to troubleshoot problems. To run this, you need to set the variable :envvar:`FLASK_APP` to ``airflow.www.app:create_app``.

For example, to print all routes, run:

.. code-block:: bash

    FLASK_APP=airflow.www.app:create_app flask routes
