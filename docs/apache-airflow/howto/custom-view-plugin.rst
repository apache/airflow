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


Customize view of Apache Hive Metastore from Airflow web UI
===========================================================

Airflow has feature that allows to integrate a custom UI along with its
core UI using the Plugin manager

This is an example plugin for Airflow that allows to create custom view of
Apache Hive metastore from the web UI of Airflow. Showing Metastore information
like list of the tables in database, finding the table object/ ddl information
for given table, retrieving partition information, retrieving data from the table,
retrieving table objects from Hive Metastore are some of the custom views shown
in this example.

In this plugin, two object reference are derived from the base class
``airflow.plugins_manager.AirflowPlugin``. They are flask_blueprints and
appbuilder_views

Using flask_blueprints in Airflow plugin, the core application can be extended
to support the application that is customized to view Apache Hive Metastore.
In this object reference, the list of Blueprint object with the static template for
rendering the Metastore information is passed on.

Using appbuilder_views in Airflow plugin, a class that represents a concept is
added and presented with views and methods to implement it.
In this object reference, the list of dictionaries with FlaskAppBuilder BaseView object
and metadata information like name and category is passed on.


Custom view Registration
------------------------

A custom view with object reference to flask_appbuilder and Blueprint from flask
and be registered as a part of a :doc:`plugin </authoring-and-scheduling/plugins>`. The following is a
skeleton for us to implement a new custom view:

.. code-block:: python

    from airflow.plugins_manager import AirflowPlugin
    from flask import Blueprint
    from flask_appbuilder import BaseView


    class MetastoreBrowserView(BaseView):
        pass


    # Creating a flask blueprint to integrate the templates and static folder
    bp = Blueprint(
        "metastore_browser",
        __name__,
        template_folder="templates",
        static_folder="static",
        static_url_path="/static/metastore_browser",
    )


    class MetastoreBrowserPlugin(AirflowPlugin):
        name = "metastore_browser"
        flask_blueprints = [bp]
        appbuilder_views = [
            {
                "category": "Plugins",  # name of the tab in Airflow UI
                "name": "Hive Metadata Browser",  # name of link under the tab
                "view": MetastoreBrowserView(),
            }
        ]

``Plugins`` specified in the ``category`` key of ``appbuilder_views`` dictionary is
the name of the tab in the navigation bar of the Airflow UI. ``Hive Metastore Browser``
is the name of the link under the tab ``Plugins``, which will launch the plugin

We have to add Blueprint for generating the part of the application
that needs to be rendered in Airflow web UI. We can define templates, static files
and this blueprint will be registered as part of the Airflow application when the
plugin gets loaded.

Next, we can add code into ``MetastoreBrowserView`` with views and implementing
methods for each of those views. After the implementation, the custom view
created becomes part of the Airflow web UI.

For reference, here's the plugin code within ``MetastoreBrowserView`` class that shows list of tables in the database:

.. exampleinclude:: ../../../metastore_browser/hive_metastore.py
    :language: python
    :start-after: [START howto_customview_show_database_table]
    :end-before: [END howto_customview_show_database_table]

The ``$AIRFLOW_HOME/plugins`` folder with custom view UI have the following folder structure.

::

    plugins
    ├── hive_metastore.py
    ├── templates
    |   └── metastore_browser
    |       ├── base.html
    |       ├── db.html
    |       ├── dbs.html
    │       └── table.html
    └── README.md

The HTML files required to render the views built is added as part of the
Airflow plugin into ``$AIRFLOW_HOME/plugins/templates`` folder and defined in the
blueprint.
