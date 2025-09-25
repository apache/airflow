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
They are fastapi_apps, fastapi_root_middlewares, external_views and react_apps.

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
In this object reference, the list of dictionaries with the view name, href (templatable), destination and
optional parameters like the icon and url_route are passed on.

Using react_apps  in Airflow plugin, allows to register custom React applications that can be rendered
in the Airflow UI. This is useful for integrating custom React components or applications into the Airflow UI.
In this object reference, the list of dictionaries with the app name, bundle_url (where to load the js assets, templatable), destination and
optional parameters like the icon and url_route are passed on.


Information and code samples to register ``fastapi_apps``, ``fastapi_root_middlewares``, ``external_views`` and ``react_apps`` are
available in :doc:`plugin </administration-and-deployment/plugins>`.

Developing React Applications with the Bootstrap Tool
=====================================================

.. warning::
  React applications are new in Airflow 3.1 and should be considered experimental. The feature may be
  subject to changes in future versions without warning based on user feedback and errors reported.
  Dependency and state interactions between the UI and plugins may need to be refactored, which will also change the bootstrapped example project provided.

|experimental|

Airflow provides a React plugin bootstrap tool to help developers quickly create, develop, and integrate external React applications into the core UI. This is the most flexible
and recommended way to customize the Airflow UI.
This tool generates a complete React project structure that builds as a library compatible with dynamic imports and shares React instances with the host Airflow application.

Creating a New React Plugin Project
-----------------------------------

The bootstrap tool is located in ``dev/react-plugin-tools/`` and provides a simple CLI to generate new React plugin projects:

.. code-block:: bash

    # Navigate to the bootstrap tool directory
    cd dev/react-plugin-tools

    # Create a new plugin project
    python bootstrap.py my-awesome-plugin

    # Or specify a custom directory
    python bootstrap.py my-awesome-plugin --dir /path/to/my-projects/my-awesome-plugin

This generates a complete React project with Vite, TypeScript, Chakra UI integration, and proper configuration for building as a library that integrates with Airflow's UI.

.. warning:: It is highly recommended to use the bootstrap tool to create a new React Plugin project. There are specific bundling configurations required
   to ensure compatibility with Airflow's Core UI and manually setting up a project may lead to integration issues. If you already have an existing React project that
   you want to integrate, you can take a look at the bootstrap tool code and the generated build configuration files for reference.
   React and React-DOM are shared dependencies with the host application and need to have compatible versions.

React Development Workflow
---------------------------

Once your project is generated, refer to the ``README.md`` file in your project directory for complete development instructions, including:

- Available development scripts (``pnpm dev``, ``pnpm build``, etc.)
- Project structure explanation
- Development workflow with hot reload
- Building for production
- Troubleshooting common React development issues

The generated project is pre-configured with all necessary tools and follows Airflow's UI development patterns.

Integrating with Airflow
-------------------------

To integrate your React application with Airflow, you need to:

1. **Serve the built assets** you can do that on your own infrastructure or directly within Airflow using ``fastapi_apps``
2. **Register the React app** using ``react_apps`` plugin configuration

Example Plugin Implementation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create an Airflow plugin that serves your React application:

.. code-block:: python

    from pathlib import Path
    from fastapi import FastAPI
    from starlette.staticfiles import StaticFiles
    import mimetypes

    from airflow.plugins_manager import AirflowPlugin

    # Ensure proper MIME types for cjs files
    mimetypes.add_type("application/javascript", ".cjs")

    # Create FastAPI app to serve static files
    app = FastAPI()

    # Mount your React app's dist folder
    react_app_directory = Path(__file__).parent.joinpath("my-awesome-plugin", "dist")
    app.mount(
        "/my-react-app",
        StaticFiles(directory=react_app_directory, html=True),
        name="my_react_app_static",
    )


    class MyReactPlugin(AirflowPlugin):
        name = "My React Plugin"

        # Serve static files
        fastapi_apps = [
            {
                "app": app,
                "url_prefix": "/my-plugin",
                "name": "My Plugin Static Server",
            }
        ]

        # Register React application
        react_apps = [
            {
                "name": "My Awesome React App",
                "url_route": "my-awesome-app",
                "bundle_url": "https://airflow-domain/my-plugin/my-react-app/main.umd.cjs",
                "destination": "nav",
            }
        ]

Plugin Configuration Options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

React apps support several configuration options, you can take a look at :doc:`plugin </administration-and-deployment/plugins>` for more details.


Integration Best Practices
---------------------------

The generated template follows these best practices for Airflow integration:

1. **External Dependencies**: React and common libraries are marked as external to avoid conflicts with the host application
2. **Global Naming**: Uses standardized global name (``AirflowPlugin``) for consistency
3. **Library Build**: Configured as UMD library with proper externalization for dynamic imports
4. **MIME Types**: Proper JavaScript MIME type handling for ``.cjs`` files because FastAPI serves them as plain text by default

Deployment Strategies
---------------------

External Hosting
~~~~~~~~~~~~~~~~

You can also host assets on external infrastructure:

.. code-block:: python

    react_apps = [
        {
            "name": "My External App",
            "url_route": "my-external-app",
            "bundle_url": "https://my-cdn.com/main.umd.cjs",
            "destination": "nav",
        }
    ]

Troubleshooting Integration Issues
-----------------------------------

Common integration issues and solutions:

**MIME type issues**
    Ensure ``.js`` and ``.cjs`` files are served with correct MIME type using ``mimetypes.add_type("application/javascript", ".cjs")``.

**Component not loading**
    Check that the bundle URL is accessible and matches the expected format.

**React development issues**
    Refer to the ``README.md`` file generated with your project for detailed troubleshooting of React-specific development issues.

Support for Airflow 2 plugins
=============================

Airflow 2 plugins are still supported with some limitations. More information on such
plugins can be found in the Airflow 2 documentation.

Adding Rest endpoints through the blueprints is still supported, those endpoints will
be integrated in the FastAPI application via the WSGI Middleware and accessible
under ``/pluginsv2``.

Adding Flask-AppBuilder views ( ``appbuilder_views`` ) via the Airflow 2 is still supported in its own iframe.

It is not possible to extend the AF3 core UI, for instance by extending the base template, nonetheless extra menu items
of the auth managers are added to the core UI security tab and their ``href`` are rendered in iframes.
This is how the fab provider integrates users, roles, actions, resources and permissions custom views in the Airflow 3 UI.


Airflow 3 plugins will be improved to allow UI customization for the entire react app, it is recommended
to upgrade your plugins to Airflow 3 plugins when possible. Until then for a temporary or custom needs
it is possible to use a Middleware to inject custom javascript or css to the core UI index request.
