
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

HTTP Event Trigger
==================

.. _howto/trigger:HttpEventTrigger:

The ``HttpEventTrigger`` is an event-based trigger that monitors whether responses
from an API meet the conditions set by the user in the ``response_check`` callable.

It is designed for **Airflow 3.0+** to be used in combination with the ``AssetWatcher`` system,
enabling event-driven DAGs based on API responses.

How It Works
------------

1. Sends requests to an API every ``poll_interval`` seconds (default 60).
2. Uses the callable at ``response_check_path`` to evaluate the API response.
3. If the callable returns ``True``, a ``TriggerEvent`` is emitted. This will trigger DAGs using this ``AssetWatcher`` for scheduling.

.. note::
   This trigger requires **Airflow >= 3.0** due to dependencies on ``AssetWatcher`` and event-driven scheduling infrastructure.

Usage Example with AssetWatcher
-------------------------------

Here's an example of using the HttpEventTrigger in an AssetWatcher to monitor the GitHub API for new Airflow releases.

.. code-block:: python


    import datetime
    import os

    from asgiref.sync import sync_to_async

    from airflow.providers.http.triggers.http import HttpEventTrigger
    from airflow.sdk import Asset, AssetWatcher, Variable, dag, task

    # This token must be generated through GitHub and added as an environment variable
    token = os.getenv("GITHUB_TOKEN")

    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
    }


    async def check_github_api_response(response):
        data = response.json()
        release_id = str(data["id"])
        get_variable_sync = sync_to_async(Variable.get)
        previous_release_id = await get_variable_sync(key="release_id_var", default=None)
        if release_id == previous_release_id:
            return False
        release_name = data["name"]
        release_html_url = data["html_url"]
        set_variable_sync = sync_to_async(Variable.set)
        await set_variable_sync(key="release_id_var", value=str(release_id))
        await set_variable_sync(key="release_name_var", value=release_name)
        await set_variable_sync(key="release_html_url_var", value=release_html_url)
        return True


    trigger = HttpEventTrigger(
        endpoint="repos/apache/airflow/releases/latest",
        method="GET",
        http_conn_id="http_default",  # HTTP connection with https://api.github.com/ as the Host
        headers=headers,
        response_check_path="dags.check_airflow_releases.check_github_api_response",  # Path to the check_github_api_response callable
        poll_interval=600,  # Poll API every 600 seconds
    )

    asset = Asset(
        "airflow_releases_asset", watchers=[AssetWatcher(name="airflow_releases_watcher", trigger=trigger)]
    )


    @dag(start_date=datetime.datetime(2024, 10, 1), schedule=asset, catchup=False)
    def check_airflow_releases():
        @task()
        def print_airflow_release_info():
            release_name = Variable.get("release_name_var")
            release_html_url = Variable.get("release_html_url_var")
            print(f"{release_name} has been released. Check it out at {release_html_url}")

        print_airflow_release_info()


    check_airflow_releases()

Parameters
----------

``http_conn_id``
    http connection id that has the base API url i.e https://www.google.com/ and optional authentication credentials.
    Default headers can also be specified in the Extra field in json format.

``auth_type``
    The auth type for the service

``method``
    the API method to be called

``endpoint``
    Endpoint to be called, i.e. ``resource/v1/query?``

``headers``
    Additional headers to be passed through as a dict

``data``
    Payload to be uploaded or request parameters

``extra_options``
    Additional kwargs to pass when creating a request.

``response_check_path``
    Path to callable that evaluates whether the API response passes the conditions set by the user to trigger DAGs

``poll_interval``
    How often, in seconds, the trigger should send a request to the API.


Important Notes
---------------

1. A ``response_check_path`` value is required.
2. The ``response_check_path`` must contain the path to an asynchronous callable. Synchronous callables will raise an exception.
3. The ``poll_interval`` defaults to 60 seconds. This may be changed to avoid hitting API rate limits.
4. This trigger does not automatically record the previous API response.
5. The previous response may have to be persisted manually though ``Variable.set()`` in the ``response_check_path`` callable to prevent the trigger from emitting events repeatedly for the same API response.
