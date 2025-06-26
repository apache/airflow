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


Customize view of Apache from Airflow web UI
============================================

Airflow has feature that allows to integrate a custom UI along with its
core UI using the Plugin manager.

Plugins integrate with the Airflow core RestAPI. In this plugin,
three object references are derived from the base class ``airflow.plugins_manager.AirflowPlugin``.
They are fastapi_apps, fastapi_root_middlewares and external_views.

Using fastapi_apps in Airflow plugin, the core RestAPI can be extended
to support extra endpoints to serve custom static file or any other json/application responses.
In this object reference, the list of dictionaries with FastAPI application and metadata information
like the name and the url prefix are passed on.

Using fastapi_root_middlewares in Airflow plugin, allows to register custom middleware at the root of
the FastAPI application. This middleware can be used to add custom headers, logging, or any other
functionality to the entire FastAPI application, including core endpoints.
In this object reference, the list of dictionaries with Middleware factories object,
initialization parameters and some metadata information like the name are passed on.

Using external_views in Airflow plugin, allows to register custom views that are rendered in iframes or external link
in the Airflow UI. This is useful for integrating external applications or custom dashboards into the Airflow UI.
In this object reference, the list of dictionaries with the view name, iframe src (templatable), destination and
optional parameters like the icon and url_route are passed on.

Information and code samples to register ``fastapi_apps``, ``fastapi_root_middlewares`` and ``external_views`` are
available in :doc:`plugin </administration-and-deployment/plugins>`.

Support for Airflow 2 plugins
=============================

Airflow 2 plugins are still supported with some limitations. More information on such
plugins can be found in the Airflow 2 documentation.

Adding Rest endpoints through the blueprints is still supported, those endpoints will
be integrated in the FastAPI application via the WSGI Middleware and accessible
under ``/pluginsv2``.

It is not possible to extend the core UI, for instance by extending the base template, nonetheless extra menu items
of the auth managers are added to the core UI security tab and their ``href`` are rendered in iframes.
This is how the fab provider integrates users, roles, actions, resources and permissions custom views in the Airflow 3 UI.


Airflow 3 plugins will be improved to allow UI customization for the entire react app, it is recommended
to upgrade your plugins to Airflow 3 plugins when possible. Until then for a temporary or custom needs
it is possible to use a Middleware to inject custom javascript or css to the core UI index request.
