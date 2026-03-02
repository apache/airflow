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

Configuring Flask Application for Airflow Webserver
===================================================

``FabAuthManager`` and Airflow 2 plugins uses Flask to render the web UI.When initialized, predefined configuration
is used, based on the ``webserver`` section of the ``airflow.cfg`` file. You can override these settings
and add any extra settings by adding flask configuration to ``webserver_config.py`` file specified in ``[fab] config_file`` (by default it is ``$AIRFLOW_HOME/webserver_config.py``). This file is automatically loaded by the webserver.

For example if you would like to change rate limit strategy to "moving window", you can set the
``RATELIMIT_STRATEGY`` to ``moving-window``.

You could also enhance / modify the underlying flask app directly,
as the `app context <https://flask.palletsprojects.com/en/2.3.x/appcontext/>`_ is pushed to ``webserver_config.py``:

.. code-block:: python

    from flask import current_app as app


    @app.before_request
    def print_custom_message() -> None:
        print("Executing before every request")
