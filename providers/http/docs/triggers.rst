
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

The ``HttpEventTrigger`` is an event-based trigger that monitors whether responses from an API meet the conditions set
by the user in the ``response_check_path`` callable. If the condition is met, then the trigger "fires". Otherwise, it waits
a certain amount of time before repeating the request and check.

The ``HttpEventTrigger`` is designed for **Airflow 3.0+** to be used in combination with the ``AssetWatcher`` system,
enabling event-driven DAGs based on API responses.

How It Works
------------

1. Sends requests to an API every ``poll_interval`` seconds (default 60).
2. Uses the callable at ``response_check_path`` to evaluate the API response.
3. If the callable returns ``True``, a ``TriggerEvent`` is emitted. This will trigger DAGs using this ``AssetWatcher`` for scheduling.
4. If the request fails or the callable raises, the error is logged and the trigger polls again after ``poll_interval`` seconds.
5. After ``max_consecutive_failures`` consecutive failed polls (default 10) the trigger stops absorbing the error and raises, so the failure and its traceback reach the trigger log. The triggerer then restarts the watcher.

.. note::
   This trigger requires **Airflow >= 3.0** due to dependencies on ``AssetWatcher`` and event-driven scheduling infrastructure.

Usage Example with AssetWatcher
-------------------------------

Here's an example of using the ``HttpEventTrigger`` in an ``AssetWatcher`` to monitor the GitHub API for new Airflow releases.

.. code-block:: python


    import asyncio
    import datetime
    import os

    from airflow.providers.http.triggers.http import HttpEventTrigger
    from airflow.sdk import Asset, AssetWatcher, dag, task

    # This token must be generated through GitHub and added as an environment variable
    token = os.getenv("GITHUB_TOKEN")

    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
    }


    async def check_github_api_response(response, asset_state_store=None):
        """Determine if a new version of Airflow has been released."""
        data = response.json()
        release_id = str(data["id"])

        if asset_state_store is not None:
            # aget/aset landed in Airflow 3.3.2; 3.3.0 and 3.3.1 expose the blocking API.
            # Use aget/aset when available, or fall back to asyncio.to_thread to avoid blocking
            # the triggerer's shared event loop.
            if hasattr(asset_state_store, "aget"):
                previous_release_id = await asset_state_store.aget("release_id", None)
            else:
                previous_release_id = await asyncio.to_thread(asset_state_store.get, "release_id", None)

            if release_id == previous_release_id:
                return False

            release_name = data.get("name", "Unknown")
            release_html_url = data.get("html_url", "Unknown")
            if hasattr(asset_state_store, "aset"):
                await asset_state_store.aset("release_id", release_id)
                await asset_state_store.aset("release_name", release_name)
                await asset_state_store.aset("release_html_url", release_html_url)
            else:
                await asyncio.to_thread(asset_state_store.set, "release_id", release_id)
                await asyncio.to_thread(asset_state_store.set, "release_name", release_name)
                await asyncio.to_thread(asset_state_store.set, "release_html_url", release_html_url)

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
        name="airflow_releases_asset", watchers=[AssetWatcher(name="airflow_releases_watcher", trigger=trigger)]
    )


    @dag(start_date=datetime.datetime(2024, 10, 1), schedule=asset, catchup=False)
    def check_airflow_releases():
        @task()
        def print_airflow_release_info(**context):
            store = context.get("asset_state_store")
            release_name = store.get("release_name") if store else "Unknown"
            release_html_url = store.get("release_html_url") if store else "Unknown"
            print(f"{release_name} has been released. Check it out at {release_html_url}")

        print_airflow_release_info()


    check_airflow_releases()

Parameters
----------

``http_conn_id``
    HTTP Connection ID that has the base API URL, e.g. ``https://www.google.com/``, and optional authentication credentials. Default headers can also be specified in the ``extra`` field in JSON format.

``auth_type``
    The auth type for the service

``method``
    The API method to be called (``GET``, ``POST``, etc.)

``endpoint``
    Endpoint to be called, i.e. ``resource/v1/query?``

``headers``
    Additional headers to be passed through as a ``dict``

``data``
    Payload to be uploaded or request parameters

``extra_options``
    Additional keyword arguments to pass when creating a request

``response_check_path``
    Path to callable that evaluates whether the API response passes the conditions set by the user to trigger DAGs.
    If the callable accepts an ``asset_state_store`` keyword argument (or ``**kwargs``), the trigger provides the
    ``AssetStateStoreAccessors`` instance (on Airflow >= 3.3.0) or ``None`` (on older Airflow versions or when the trigger
    is not running in an asset watcher context).

``poll_interval``
    How often, in seconds, the trigger should send a request to the API

``max_consecutive_failures``
    Maximum number of consecutive polling failures before the trigger raises.
    The effective failure tolerance is approximately
    ``max_consecutive_failures`` × ``poll_interval``.
    Any poll that completes resets the count, including one where ``response_check`` returns ``False``.


Important Notes
---------------

1. A ``response_check_path`` value is required.
2. The ``response_check_path`` must contain the path to an asynchronous callable. Synchronous callables will raise an exception.
3. The ``poll_interval`` defaults to 60 seconds. This may be changed to avoid hitting API rate limits.
4. On Airflow >= 3.3.0, the ``response_check_path`` callable can accept ``asset_state_store`` (e.g. ``async def check(response, asset_state_store=None)``) to read and persist state (like cursors or seen IDs) across polls without needing external variables.
5. On Airflow versions earlier than 3.3.0, or when not running in an asset watcher context, ``asset_state_store`` is ``None``.
