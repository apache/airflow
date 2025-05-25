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
two object references are derived from the base class ``airflow.plugins_manager.AirflowPlugin``.
They are fastapi_apps and fastapi_root_middlewares.

Using fastapi_apps in Airflow plugin, the core RestAPI can be extended
to support extra endpoints to serve custom static file or any other json/application responses.
In this object reference, the list of dictionaries with FastAPI application and metadata information
like the name and the url prefix are passed on.

Using fastapi_root_middlewares in Airflow plugin, allows to register custom middleware at the root of
the FastAPI application. This middleware can be used to add custom headers, logging, or any other
functionality to the entire FastAPI application, including core endpoints.
In this object reference, the list of dictionaries with Middleware factories object,
initialization parameters and some metadata information like the name are passed on.

Information and code samples to register ``fastapi_apps`` and ``fastapi_root_middlewares`` are
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


Airflow 3 now provides native support for React-based UI plugins through the ``ui`` field in plugin definitions.
This allows plugins to integrate seamlessly with the main Airflow web interface while maintaining the sidebar
navigation and overall layout. It is recommended to upgrade your plugins to use the new UI plugin system
when possible. For legacy needs, it is still possible to use middleware to inject custom JavaScript or CSS.

React-based UI Plugins
======================

Airflow 3 introduces native support for UI plugins that integrate with the React-based web interface.
UI plugins appear as menu items in the sidebar navigation and render their content within the main
Airflow layout while maintaining the navigation bar and overall user experience.

Plugin Types
------------

Airflow 3 supports two plugin rendering types:

**Iframe Type (Recommended)**

The ``"iframe"`` type renders your plugin's UI inside an iframe within the Airflow layout.
This is the **currently supported and recommended approach** for all plugin development.

**Module Type (Not Yet Functional)**

The ``"module"`` type is designed for Module Federation integration but is **not currently implemented**.
While the backend accepts module type plugins, the frontend Module Federation infrastructure is incomplete.

.. warning::
    Module type plugins are not functional in the current Airflow 3 release. The Module Federation
    implementation is planned for a future release. Use iframe type for all plugin development.

Quick Start
-----------

To create a UI plugin, you need to:

1. **Define a FastAPI app** to serve your plugin's UI and API endpoints
2. **Register the FastAPI app** in your plugin's ``fastapi_apps`` field
3. **Define UI configuration** in your plugin's ``ui`` field with ``"type": "iframe"``
4. **Create your frontend** (HTML/CSS/JS or React)

Here's a minimal working example:

.. code-block:: python

    from airflow.plugins_manager import AirflowPlugin
    from fastapi import FastAPI
    from fastapi.staticfiles import StaticFiles
    from pathlib import Path

    # Create FastAPI app
    app = FastAPI()

    # Serve static files
    static_dir = Path(__file__).parent / "static"
    app.mount("/ui", StaticFiles(directory=static_dir, html=True), name="ui")


    class MyPlugin(AirflowPlugin):
        name = "my_plugin"

        fastapi_apps = [{"app": app, "url_prefix": "/my_plugin", "name": "my_plugin_api"}]

        ui = [
            {
                "slug": "my-plugin",
                "label": "My Plugin",
                "icon": "FiZap",
                "entry": "/my_plugin/ui",
                "type": "iframe",  # Always use iframe type
                "permissions": ["plugins.can_read"],
            }
        ]

Step-by-Step Development Tutorial
=================================

This tutorial walks you through creating a complete UI plugin from scratch using the iframe approach.

Project Structure
-----------------

Create the following directory structure in your ``$AIRFLOW_HOME/plugins`` folder:

.. code-block:: text

    plugins/
    └── my_custom_plugin/
        ├── __init__.py
        ├── plugin.py
        ├── static/
        │   ├── index.html
        │   ├── style.css
        │   └── airflow-plugin-base.js
        ├── pyproject.toml
        ├── README.md
        └── LICENSE

Step 1: Create the Plugin Class
-------------------------------

Create ``plugin.py``:

.. code-block:: python

    from airflow.plugins_manager import AirflowPlugin
    from fastapi import FastAPI
    from fastapi.staticfiles import StaticFiles
    from pathlib import Path

    # Create FastAPI app
    app = FastAPI(title="Custom Plugin API")

    # Get plugin directory
    plugin_dir = Path(__file__).parent

    # Serve static files (this serves your HTML/CSS/JS)
    app.mount("/ui", StaticFiles(directory=plugin_dir / "static", html=True), name="ui")


    # Optional: Add API endpoints for your plugin
    @app.get("/api/status")
    async def get_status():
        """Simple status endpoint"""
        return {"status": "healthy", "plugin": "my_custom_plugin"}


    # Define the plugin
    class CustomPlugin(AirflowPlugin):
        name = "custom_plugin"

        # Register the FastAPI app
        fastapi_apps = [
            {"app": app, "url_prefix": "/custom", "name": "custom_api"}  # Plugin will be available at /custom/*
        ]

        # Register the UI plugin (appears in sidebar)
        ui = [
            {
                "slug": "custom",  # URL: /plugins/custom
                "label": "Custom Plugin",  # Text in sidebar
                "icon": "FiZap",  # Icon from react-icons/fi
                "entry": "/custom/ui",  # Points to FastAPI static mount
                "type": "iframe",  # Always use iframe
                "permissions": ["plugins.can_read"],  # Required permissions
            }
        ]

Step 2: Create the Frontend (HTML/CSS/JS Approach)
--------------------------------------------------

Create ``static/index.html``:

.. code-block:: html

    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>My Plugin</title>

        <!-- Include Airflow theme integration FIRST -->
        <script src="airflow-plugin-base.js"></script>

        <link rel="stylesheet" href="style.css">
    </head>
    <body>
        <div class="container">
            <h1>My Custom Plugin</h1>
            <p>This plugin integrates with Airflow's theme system.</p>

            <div class="card">
                <h2>Plugin Content</h2>
                <p>Add your plugin's functionality here.</p>
            </div>
        </div>

        <!-- Listen for theme changes -->
        <script>
            document.addEventListener('airflow-theme-change', function(event) {
                console.log('Theme changed to:', event.detail.theme);
                // Add any custom theme-specific logic here
            });
        </script>
    </body>
    </html>

Create ``static/style.css``:

.. code-block:: css

    /* Light mode styles (default) */
    body {
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
        margin: 0;
        padding: 20px;
        background-color: #f5f5f5;
        color: #333;
        transition: background-color 0.3s, color 0.3s;
    }

    .container {
        max-width: 800px;
        margin: 0 auto;
        background-color: white;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        padding: 20px;
    }

    .card {
        border: 1px solid #e0e0e0;
        border-radius: 4px;
        padding: 16px;
        margin-top: 20px;
    }

    /* Dark mode styles - automatically applied when Airflow switches to dark mode */
    .dark-mode {
        background-color: #1a1a1a;
        color: #e0e0e0;
    }

    .dark-mode .container {
        background-color: #2a2a2a;
        box-shadow: 0 2px 4px rgba(0,0,0,0.3);
    }

    .dark-mode .card {
        border-color: #444;
        background-color: #333;
    }



Step 3: Advanced Theme Integration
-----------------------------------

One of the key advantages of Airflow 3's React-based UI plugins is **automatic theme synchronization**.
When users switch between light and dark mode in Airflow, your plugin can automatically adapt to match
the main interface, providing a seamless user experience.

**Why Theme Synchronization Matters:**

- **Consistent User Experience**: Your plugin looks native to Airflow
- **Professional Appearance**: No jarring theme mismatches when switching modes
- **Accessibility**: Respects user's theme preferences automatically
- **Modern Integration**: Leverages Airflow 3's React architecture

Create ``static/airflow-plugin-base.js`` for automatic theme synchronization:

.. code-block:: javascript

    /*!
     * Airflow 3 Plugin Base Integration
     *
     * This file provides the core integration between Airflow 3 and iframe-based plugins.
     * It handles theme synchronization, postMessage communication, and other Airflow-specific features.
     *
     * Plugin developers should NOT modify this file. Instead, focus on customizing your plugin's
     * HTML, CSS, and JavaScript in the main index.html file.
     *
     * Features provided:
     * - Automatic theme synchronization with Airflow's dark/light mode
     * - URL parameter parsing for initial theme detection
     * - PostMessage communication with parent Airflow window
     * - Ready state notification to parent window
     */

    (function() {
        'use strict';

        // Airflow Plugin Base Integration
        const AirflowPluginBase = {

            /**
             * Initialize the plugin integration with Airflow
             */
            init: function() {
                this.setupThemeSync();
                this.notifyParentReady();
            },

            /**
             * Set up theme synchronization with Airflow
             */
            setupThemeSync: function() {
                // Apply initial theme from URL parameter
                const initialTheme = this.getInitialTheme();
                this.applyTheme(initialTheme);

                // Listen for theme updates from parent Airflow window
                window.addEventListener('message', (event) => {
                    if (event.data && event.data.type === 'AIRFLOW_THEME_UPDATE') {
                        this.applyTheme(event.data.theme);
                    }
                });
            },

            /**
             * Get the initial theme from URL parameters
             * @returns {string} 'dark' or 'light'
             */
            getInitialTheme: function() {
                const urlParams = new URLSearchParams(window.location.search);
                const themeParam = urlParams.get('theme');
                return themeParam || 'light';
            },

            /**
             * Apply the specified theme to the document
             * @param {string} theme - 'dark' or 'light'
             */
            applyTheme: function(theme) {
                if (theme === 'dark') {
                    document.body.classList.add('dark-mode');
                } else {
                    document.body.classList.remove('dark-mode');
                }

                // Trigger custom event for plugin-specific theme handling
                const themeEvent = new CustomEvent('airflow-theme-change', {
                    detail: { theme: theme }
                });
                document.dispatchEvent(themeEvent);
            },

            /**
             * Notify the parent Airflow window that the plugin is ready
             */
            notifyParentReady: function() {
                if (window.parent !== window) {
                    window.parent.postMessage({
                        type: 'PLUGIN_READY',
                        theme: this.getInitialTheme()
                    }, '*');
                }
            },

            /**
             * Get the current theme
             * @returns {string} 'dark' or 'light'
             */
            getCurrentTheme: function() {
                return document.body.classList.contains('dark-mode') ? 'dark' : 'light';
            }
        };

        // Auto-initialize when DOM is ready
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', function() {
                AirflowPluginBase.init();
            });
        } else {
            AirflowPluginBase.init();
        }

        // Also initialize on window load for additional safety
        window.addEventListener('load', function() {
            AirflowPluginBase.notifyParentReady();
        });

        // Expose the API globally for plugin developers who need advanced features
        window.AirflowPluginBase = AirflowPluginBase;

    })();

The HTML example above already includes the theme integration. The key points are:

1. **Include ``airflow-plugin-base.js`` first** - before any other scripts
2. **Add theme change listener** - to handle custom theme logic
3. **Use ``.dark-mode`` CSS classes** - automatically applied by the base script

Step 4: Testing Your Plugin
---------------------------

1. **Restart Airflow API server** to load the new plugin:

   .. code-block:: bash

       airflow api-server

2. **Check plugin loading** with the CLI:

   .. code-block:: bash

       airflow plugins

   You should see your plugin listed in the output.

3. **Access your plugin** by navigating to the Airflow UI and looking for "Custom Plugin" in the sidebar menu.

4. **Test API endpoints** (optional):

   .. code-block:: bash

       curl http://localhost:8080/custom/api/status

5. **Test theme synchronization**:

   - Switch between light and dark mode in Airflow's UI
   - Your plugin should automatically adapt to match the theme
   - Check browser console for theme change events

Step 5: Python Packaging with Hatch
------------------------------------

To distribute your plugin as a Python package, create a ``pyproject.toml`` file in your plugin's root directory.
This example uses **hatch** as the build backend, which is modern and efficient for Python packaging.

Create ``pyproject.toml``:

.. code-block:: toml

    [project]
    name = "airflow-my-custom-plugin"
    version = "0.1.0"
    description = "Custom plugin for Apache Airflow 3"
    readme = "README.md"
    authors = [
        {name = "Your Name", email = "your.email@example.com"}
    ]
    license = {text = "Your License Choice"}  # e.g., "MIT", "Apache-2.0", "BSD-3-Clause"
    classifiers = [
        "Development Status :: 3 - Alpha",
        "Environment :: Web Environment",
        "Framework :: Apache Airflow",
        "Intended Audience :: Developers",
        "License :: OSI Approved",  # Update based on your license choice
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ]
    requires-python = ">=3.8"
    dependencies = [
        "apache-airflow>=3.1.0",
    ]

    [project.urls]
    "Homepage" = "https://github.com/yourusername/airflow-my-custom-plugin"
    "Bug Tracker" = "https://github.com/yourusername/airflow-my-custom-plugin/issues"

    [project.optional-dependencies]
    dev = [
        "build>=1.2.2",
        "pre-commit>=4.0.1",
        "ruff>=0.9.2"
    ]

    # This entry point tells Airflow how to discover your plugin
    [project.entry-points."airflow.plugins"]
    my_custom_plugin = "my_custom_plugin:CustomPlugin"

    [build-system]
    requires = ["hatchling"]
    build-backend = "hatchling.build"

    # Configure what gets included in source distribution
    [tool.hatch.build.targets.sdist]
    exclude = [
        "*",
        "!my_custom_plugin/**",
        "!pyproject.toml",
        "!README.md"
    ]

    # Configure what gets included in wheel distribution
    [tool.hatch.build.targets.wheel]
    packages = ["my_custom_plugin"]

**Key Configuration Points:**

* **Entry Points**: The ``[project.entry-points."airflow.plugins"]`` section tells Airflow how to discover your plugin
* **Dependencies**: Include ``apache-airflow>=3.1.0`` as a minimum requirement
* **Build Targets**: Configure ``sdist`` to exclude everything except your plugin code and essential files
* **Wheel Packages**: Specify which Python packages to include in the wheel

**Building and Installing:**

1. **Install build tools**:

   .. code-block:: bash

       pip install build hatch

2. **Build the package**:

   .. code-block:: bash

       python -m build

   This creates both source distribution (``.tar.gz``) and wheel (``.whl``) files in the ``dist/`` directory.

3. **Install locally for testing**:

   .. code-block:: bash

       pip install dist/airflow_my_custom_plugin-0.1.0-py3-none-any.whl

4. **Install in development mode**:

   .. code-block:: bash

       pip install -e .

**Directory Structure After Packaging:**

.. code-block:: text

    my_custom_plugin/
    ├── my_custom_plugin/
    │   ├── __init__.py
    │   ├── plugin.py
    │   └── static/
    │       ├── index.html
    │       ├── style.css
    │       └── airflow-plugin-base.js
    ├── pyproject.toml
    ├── README.md
    ├── LICENSE
    └── dist/  # Created after building
        ├── airflow_my_custom_plugin-0.1.0.tar.gz
        └── airflow_my_custom_plugin-0.1.0-py3-none-any.whl

Troubleshooting
===============

Quick Debugging
---------------

**Check plugin loading:**

.. code-block:: bash

    # Verify plugin is loaded
    curl -H "Authorization: Bearer JWT_TOKEN" http://localhost:8080/api/v2/plugins/ui-plugins

**Sample successful response:**

.. code-block:: json

    {
        "plugins": [{
            "slug": "custom",
            "label": "Custom Plugin",
            "icon": "FiZap",
            "entry": "/custom/ui",
            "type": "iframe",
            "permissions": ["plugins.can_read"],
            "plugin_name": "custom_plugin"
        }],
        "total_entries": 1
    }

Common Issues
-------------

- **Plugin not in sidebar**: Check API response includes your plugin with correct ``type: "iframe"`` and restart API server.
- **Static files 404**: Verify ``StaticFiles`` mount path matches your ``entry`` URL in plugin configuration.
- **Permission denied**: Add ``"permissions": ["plugins.can_read"]`` to your ``ui`` configuration.
- **Theme not syncing**: Include ``airflow-plugin-base.js`` script first in your HTML.

Best Practices
==============

- **Security**: Always define ``permissions`` in your UI configuration and validate API inputs.
- **Theme Integration**: Include ``airflow-plugin-base.js`` for automatic light/dark mode synchronization.
- **Performance**: Minimize static asset sizes and use efficient API endpoints.
- **User Experience**: Test in both light and dark themes, provide loading states and error handling.

Future Enhancements
===================

Module Federation Support
-------------------------

Module Federation (``"type": "module"``) is planned for a future Airflow release.
This will enable:

- Better integration with the main Airflow React application
- Shared dependencies to reduce bundle sizes
- Native React component rendering without iframes
- Access to Airflow's React context and state

.. note::
    Module Federation support is currently in the planning phase (Phase 9 of the implementation plan).
    The backend infrastructure exists but the frontend Module Federation setup is not yet implemented.

Until Module Federation is implemented, continue using the iframe approach for all plugin development.

For updates on Module Federation support, monitor the Airflow project's development progress.
