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
* *Deadline References* – Register custom :doc:`Deadline Alert </howto/deadline-alerts>` reference classes.

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
        # The team owning this plugin (str), when multi-team mode is enabled. None means the
        # plugin is global. Ignored when multi-team mode is off. See "Multi-team deployments" below.
        team_name = None
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
        # A list of UI translation sources to add languages to, or override translations in, the UI.
        # Each entry is a path to a ``<language>/<namespace>.json`` directory tree or an inline
        # ``{language: {namespace: {key: value}}}`` mapping. See the example below.
        ui_translations = []

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

        # A list of deadline reference classes that can be used as custom deadlines in Dags.
        # Custom deadline reference classes must be registered here in order to be
        # resolvable at scheduler-side deserialization time; classes that are not
        # registered will raise ``DeadlineReferenceNotRegistered`` when a Dag attempts
        # to use them.
        deadline_references = []

        # A list of Listeners that plugin provides. Listeners can register to
        # listen to particular events that happen in Airflow, like
        # TaskInstance state changes. Listeners are python modules.
        listeners = []

You can derive it by inheritance (please refer to the example below). In the example, all options have been
defined as class attributes, but you can also define them as properties if you need to perform
additional initialization. Please note ``name`` inside this class must be specified.

``name`` must also be unique across all plugins. Airflow registers the first plugin it discovers under a
given name and skips any later plugin that reuses it, so two plugins sharing a name means one of them is
not loaded.

Make sure you restart the webserver and scheduler after making changes to plugins so that they take effect.

Plugin Management Interface
---------------------------

Airflow 3.1 introduces a Plugin Management Interface, available under *Admin → Plugins* in the Airflow UI.
This page allows you to view installed plugins.

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


.. warning::

    **Airflow does not authenticate plugin FastAPI apps. Authenticating them is the
    plugin author's responsibility.**

    Airflow authenticates the core API with authentication dependencies, declared at the
    router level and, for some endpoints, per route. A plugin app is attached with
    ``app.mount()``, and a Starlette mount has its own route table and inherits none of the
    parent's dependencies, so those dependencies never reach a plugin's routes. No
    middleware in the API server authenticates them either.

    Every route a plugin exposes is therefore reachable by **anonymous callers** unless
    the plugin authenticates it itself. The minimal ``app`` above is a structural
    illustration, not a template to deploy as-is.

    Depend on ``GetUserDep`` to require a caller Airflow has authenticated:

    .. code-block:: python

        from fastapi import FastAPI

        from airflow.api_fastapi.core_api.security import GetUserDep

        app = FastAPI()


        @app.get("/dashboard")
        def dashboard(user: GetUserDep):
            return {"user": user.get_name()}

    Prefer attaching the dependency once, at the application or router level, so that a
    route added later does not silently ship unauthenticated:

    .. code-block:: python

        from fastapi import Depends, FastAPI

        from airflow.api_fastapi.core_api.security import get_user

        app = FastAPI(dependencies=[Depends(get_user)])

    Authentication is not authorization. ``GetUserDep`` establishes *who* is calling;
    whether that user may perform a given action remains the plugin's own decision. This
    applies to team scoping too and a global plugin that does not check the caller's team
    serves every team's users the same data. The one exception is a plugin that declares a
    ``team_name`` in a deployment with ``[core] multi_team`` enabled: Airflow then
    authenticates its app and restricts it to that team's users, as described in
    :ref:`plugins-multi-team`. With multi-team mode off, that plugin's app is mounted like
    any other (unauthenticated).

    The core API's access helpers can enforce that decision for you. For example,
    ``requires_access_dag`` restricts a route to callers allowed the requested action on a
    Dag; it authenticates the caller and reads the ``dag_id`` from the request:

    .. code-block:: python

        from fastapi import Depends, FastAPI

        from airflow.api_fastapi.core_api.security import requires_access_dag

        app = FastAPI()


        @app.get("/dags/{dag_id}", dependencies=[Depends(requires_access_dag(method="GET"))])
        def dag_detail(dag_id: str):
            return {"dag_id": dag_id}

.. code-block:: python

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
        # the context variables available will be different, i.e a subset of (DAG_ID, RUN_ID, TASK_ID, MAP_INDEX, ASSET_ID, ASSET_URI).
        "href": "https://example.com/{DAG_ID}/{RUN_ID}/{TASK_ID}/{MAP_INDEX}",
        # Destination of the external view. This is used to determine where the view will be loaded in the UI.
        # Supported locations are Literal["nav", "dag", "dag_run", "task", "task_instance", "asset", "base"], default to "nav".
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
        # Optional flag, only relevant for destination "nav". When True, this item is always rendered directly on the
        # navigation toolbar instead of inside the "Plugins" submenu. When two or more non-promoted items remain they
        # are still grouped into the submenu; a single remaining non-promoted item is also shown on the toolbar.
        # Defaults to False.
        "nav_top_level": True,
        # Optional scoping, limiting where this view is shown. Keys are dotted field paths into
        # the records the page has. Omit it entirely to show the view everywhere (the default).
        # See "Scoping a view to specific Dags and tasks" below.
        "applies_to": {
            "dag.tags.name": ["production", "ml"],
            "dag.dag_id": ["my_dag", "my_other_dag"],
        },
    }

    # Note: The React app integration is experimental and interfaces might change in future versions.
    react_app_with_metadata = {
        # Name of the React app, this will be displayed in the UI.
        "name": "Name of the React App",
        # Bundle URL of the React app. This is the URL where the React app is served from. It can be a static file or a CDN.
        # This URL can be templated using context variables, depending on the location where the external view is rendered
        # the context variables available will be different, i.e a subset of (DAG_ID, RUN_ID, TASK_ID, MAP_INDEX, ASSET_ID, ASSET_URI).
        "bundle_url": "https://example.com/static/js/my_react_app.js",
        # Destination of the react app. This is used to determine where the app will be loaded in the UI.
        # Supported locations are Literal["nav", "dag", "dag_run", "task", "task_instance", "asset", "base"], default to "nav".
        # It can also be put inside of an existing page, the supported views are ["dashboard", "dag_overview", "task_overview"]. You can position
        # element in the existing page via the css `order` rule which will determine the flex order.
        # Use "base" to mount the app in the base layout (e.g. a toolbar strip); the host uses a flex container so you can set ``order`` in your root JSX to control position.
        "destination": "task",
        # Optional icon, url to an svg file.
        "icon": "https://example.com/icon.svg",
        # Optional dark icon for the dark theme, url to an svg file. If not provided, "icon" will be used for both light and dark themes.
        "icon_dark_mode": "https://example.com/dark_icon.svg",
        # URL route for the React app, relative to the Airflow UI base URL. Should not contain a leading slash.
        "url_route": "my_react_app",
        # Optional category, only relevant for destination "nav". This is used to group the react apps in the navigation bar. We will match the existing
        # menus of ["browse", "docs", "admin", "user"] and if there's no match then create a new menu.
        "category": "browse",
        # Optional flag, only relevant for destination "nav". When True, this item is always rendered directly on the
        # navigation toolbar instead of inside the "Plugins" submenu. When two or more non-promoted items remain they
        # are still grouped into the submenu; a single remaining non-promoted item is also shown on the toolbar.
        # Defaults to False.
        "nav_top_level": True,
        # Optional scoping, limiting where this app is shown. Keys are dotted field paths into
        # the records the page has. Omit it entirely to show the app everywhere (the default).
        # See "Scoping a view to specific Dags and tasks" below.
        "applies_to": {
            "dag.tags.name": ["production", "ml"],
            "operator_name": ["KubernetesPodOperator"],
        },
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

Scoping a view to specific Dags and tasks
-----------------------------------------

By default an external view or React app is shown on every page matching its ``destination``.
The optional ``applies_to`` block narrows that down, so a tab is only offered where it is
relevant instead of appearing on every Dag:

.. code-block:: python

    "applies_to": {
        "state": ["failed", "upstream_failed"],  # the entity's own field
        "dag.tags.name": ["ml"],  # a related record, array-aware
        "dag.dag_id": ["train_pipeline"],
    }

Keys are **dotted field paths** into the records the page has, so any field the REST API
returns for an entity is addressable. Use the names the API returns, which are not always the
Python attribute names: a task instance's run is ``dag_run_id``, not ``run_id``, and computed
fields such as a Dag's ``is_backfillable`` work like any other. Values are matched for equality
and compared as strings, so numbers and booleans need no quoting rules of their own
(``"try_number": ["2"]``, ``"is_paused": ["false"]``).

An **unqualified path is rooted at the entity the destination is about** — on ``dag_run``,
``state`` is the run's state; on ``task_instance``, the task instance's. A path may instead
name a related record as its first segment: ``dag``, ``dag_run``, ``task`` or
``task_instance``. Traversing a list fans out across it, so ``dag.tags.name`` collects every
tag name and matches if any of them is listed.

Paths combine like Kubernetes label selectors — **OR within a path, AND across paths** — but
the AND applies only across paths the page can evaluate. A ``task_instance.*`` path cannot be
judged on a Dag-level page, so it is skipped there rather than failing the match, which is what
lets one block be shared across a plugin's destinations. Which records each destination
resolves:

.. list-table::
   :header-rows: 1

   * - Destination
     - Unqualified path is rooted at
     - Records a qualified path can reach
   * - ``dag``, ``dag_overview``
     - ``dag``
     - ``dag``
   * - ``dag_run``
     - ``dag_run``
     - ``dag``, ``dag_run``
   * - ``task``, ``task_overview``
     - ``task``
     - ``dag``, ``task``
   * - ``task_instance``
     - ``task_instance``
     - ``dag``, ``dag_run``, ``task``, ``task_instance``
   * - ``nav``, ``base``, ``dashboard``, ``asset``
     - —
     - none, so every path is skipped

If none of the configured paths can be evaluated on a given page, the view is shown. On task
group pages the task-level records are absent, since a group is not a task.

A path is also skipped when the record exists but has no such field, which on its own would mean
a **typo widens the scope rather than narrowing it**. So paths are checked against the API
response models when plugins load, and one that names a field *no* record has is treated as a
misconfiguration: the view is **not loaded at all**, and the reason is logged as an error with a
suggested correction.

.. code-block:: text

    Plugin 'acme' has an external view 'Incidents' with an 'applies_to' path that matches no
    field: 'dag.tags.nme' names no field 'nme' on DagTagResponse (did you mean 'name'?). It
    could never scope the way it asks to, so the view will not be loaded.

Withholding it is deliberate: the author asked to narrow by something that cannot exist, so the
view can never appear for the reason they intended, and a view that disappears gets noticed
while one on every page looks deliberate.

A path naming a field some *other* record has is a different matter — that is the shared-block
pattern, so it is reported as skipped here and the view loads. Neither check can see past a
field the models do not describe — the dict behind ``class_ref``, or a Dag Run's ``conf`` — so a
typo below one of those still widens silently.

If a view appears in more places than you expect, run ``airflow plugins --verbose``, then check
the path against the REST API response for that entity.

An empty list is different from a missing field: a Dag with no tags has definitively answered
``dag.tags.name``, so the view is not shown. A path that stops short of a leaf is likewise a
decided answer rather than a skip: ``dag.tags`` resolves to a list of objects, which have no
comparable value, so the view is hidden. Address the field you mean to compare
(``dag.tags.name``).

Targeting operators
^^^^^^^^^^^^^^^^^^^

``operator_name`` is spelled the same way on a task and on a task instance, so **one
unqualified path targets an operator on either page**:

.. code-block:: python

    "applies_to": {"operator_name": ["KubernetesPodOperator"]}

``operator_name`` is the display name the UI shows (an operator's ``custom_operator_name``).
It equals the class name for a plain operator, but not for a decorated one: a ``@task.bash``
task is named ``@task.bash`` and classed ``_BashDecoratedOperator``.

It is also the only operator field both records carry. The *class* name is spelled differently
on each — a task instance has ``operator``, a task has ``class_ref.class_name`` — so targeting
it takes one view per destination, each scoped on its own record:

.. code-block:: python

    external_views = [
        {
            # ... name, href, and a url_route of its own
            "destination": "task_instance",
            "applies_to": {"operator": ["KubernetesPodOperator"]},
        },
        {
            # ...
            "destination": "task",
            "applies_to": {"class_ref.class_name": ["KubernetesPodOperator"]},
        },
    ]

One block naming both also works, since whichever field this page's record lacks is skipped:

.. code-block:: python

    "applies_to": {
        # On a task page the first is skipped and the second decides; on a task instance
        # page, the reverse. Prefer `operator_name` unless you need the private class name.
        "operator": ["KubernetesPodOperator"],
        "class_ref.class_name": ["KubernetesPodOperator"],
    }

Qualifying them — ``task_instance.operator`` and ``task.class_ref.class_name`` — works too, but
says the same thing twice: a task instance page resolves both records, so both are evaluated and
AND-ed. The task record is read at the version that instance ran, so the two agree; naming one
of them is enough.

The general rule where records overlap: **prefer an unqualified path**, which always reads the
entity the page is about, and qualify one only when you genuinely mean a different record —
``dag.tags.name`` from a task instance, say.

A malformed ``applies_to`` — one that is not a dictionary, has a non-string or empty path, or
gives a path something other than a list of strings — is reported as a warning when plugins
are loaded, and ignored, so the view still loads unscoped. Configuring a path whose root the
``destination`` cannot resolve (for example ``task_instance.state`` on a ``dag`` view) is also
warned about, since it has no effect there.

All of these are logged when the UI plugins are first collected, which happens in the API
server the first time anything requests ``/api/v2/plugins`` — and only once per process, so
reloading the page will not log them again. To see them on demand instead of searching the API
server log, run:

.. code-block:: bash

    airflow plugins --verbose

Each invocation is a fresh process, so every warning for every plugin is reported. The
``--verbose`` flag is required: without it the command suppresses log output.

.. note::
    ``applies_to`` is a display convenience, not an authorization boundary. It controls
    whether the UI offers the tab, not whether the underlying view can be reached — a user
    who knows the ``url_route`` can still navigate to it directly. Use access control to
    restrict who may view a plugin's data.

React app context props
-----------------------

.. note::
    The React app integration is experimental and these props may change in future versions.

Unlike an external view, which only receives context through ``{DAG_ID}``-style tokens in its
``bundle_url``, a React app is rendered as a component and receives context directly as props.
The props available depend on where the app is mounted (its ``destination`` and route):

- ``dagId``, ``runId``, ``taskId``, ``mapIndex``, ``assetId`` — the identifiers from the current
  route (strings), when present.
- ``assetUri`` — the URI of the current asset, when mounted on an asset route.
- ``dag``, ``dagRun``, ``taskInstance``, ``asset`` — the full records for the current route,
  matching the corresponding REST API response schemas
  (``DAGDetailsResponse``, ``DAGRunResponse``, ``TaskInstanceResponse``, ``AssetResponse``).
  Each object is only provided once the identifiers it depends on are present in the route, and
  is served from the UI's query cache the details page has already populated (no extra request).
  On routes or ``destination`` values without those identifiers (e.g. ``nav``, ``base``,
  ``dashboard``), the corresponding objects are ``undefined``.

Adding or overriding UI translations
------------------------------------

The ``ui_translations`` attribute lets a plugin add a language the Airflow UI does not ship, or
override individual strings in a language it does. Each entry is either a path to a
``<language>/<namespace>.json`` directory tree (mirroring Airflow's own
``airflow/ui/public/i18n/locales`` layout) or an inline
``{language: {namespace: {key: value}}}`` mapping. Both forms can be mixed in the same list.

.. code-block:: python

    from pathlib import Path

    from airflow.plugins_manager import AirflowPlugin


    class TranslationsPlugin(AirflowPlugin):
        name = "translations"
        ui_translations = [
            # A directory tree, e.g. locales/eo/common.json, adding Esperanto as a new language.
            Path(__file__).parent / "locales",
            # Override individual keys in a language Airflow already ships.
            {"en": {"dags": {"dag_one": "Pipeline"}}},
        ]

The plugin's values are deep-merged on top of the built-in translations, so an override replaces
only the keys it names (at any nesting depth) and leaves the rest untouched. Keys a plugin does not
provide fall back to the built-in language, and ultimately to English.

Because translations are not versioned in lockstep with Airflow, robustness is built in:

- A malformed or unreadable translation source is skipped with a warning in the API server log; it
  never stops the API server from starting or keeps other plugins from loading.
- English is the reference for which keys exist. When translations are consolidated at startup, any
  plugin key that is **not** present in the English file for its namespace (likely renamed or removed
  upstream) is logged as a warning in the API server log and otherwise ignored.

Right-to-left languages are handled automatically: the UI derives text direction from the language
code (via the browser's locale data, e.g. Persian ``fa`` or Urdu ``ur``), so a custom RTL language
flips the whole UI to right-to-left without any extra configuration.

.. _plugins-multi-team:

Multi-team deployments
----------------------

.. versionadded:: 3.4.0

A plugin can name the team that owns it by setting ``team_name``. Airflow then offers what the
plugin contributes to that team only, instead of to the whole deployment:

.. code-block:: python

    from airflow.plugins_manager import AirflowPlugin

    from my_package.payments import PaymentWindowTimetable, settlement_date


    class PaymentsPlugin(AirflowPlugin):
        name = "payments"
        # Only this team's Dags, tasks and users get the pieces below.
        team_name = "payments"
        macros = [settlement_date]
        timetables = [PaymentWindowTimetable]

``team_name`` is part of the plugin's code, so the plugin author decides it; there is no
deployment-time override. Leaving it unset (the default) makes the plugin **global**: everything
it contributes is available to every team, which is how plugins written before multi-team support
behaved.

``team_name`` only takes effect when :doc:`multi-team mode </core-concepts/multi-team>` is
enabled. With ``[core] multi_team = False`` it is ignored and every plugin is global.

The team must already exist in the metadata database (``airflow teams create <team_name>``). The
API server checks this when it loads plugins for the API: a plugin naming an unknown team is
recorded as a plugin import error (surfaced under *Admin → Plugins* and at
``GET /api/v2/plugins/importErrors``) and logged as a warning. It is deliberately not raised, so
one misconfigured plugin does not stop the API server, or the other plugins, from starting. This is
cached for the life of the process, so creating the team afterwards does not clear
the error until the API server restarts. The plugin still loads, and everything it scopes to the
nonexistent team is unusable in the meantime. Its scheduling classes are refused for every Dag,
its macros resolve for no task, and its extra links are shown nowhere.

Team validation of plugins runs in the API server. Plugin loading in the other components
does no team lookup at all, and the subprocess that imports Dag files has no database session.

Plugin *discovery* is unchanged: every Airflow component still loads every installed plugin, and
``team_name`` decides who is offered what. ``airflow plugins`` lists each plugin's ``team_name``.

.. list-table::
   :header-rows: 1
   :widths: 34 66

   * - Plugin attribute
     - Effect of ``team_name``
   * - ``fastapi_apps``
     - App is mounted behind a team check; only that team's users may call it.
   * - ``fastapi_root_middlewares``
     - Skipped, with a warning. A root middleware cannot be scoped to one team.
   * - ``external_views``, ``react_apps``
     - Only offered in the UI to that team's users.
   * - ``macros``
     - Only resolvable when rendering templates for that team's tasks.
   * - ``global_operator_extra_links``, ``operator_extra_links``
     - Only shown on task instances of that team's Dags.
   * - ``timetables``, ``partition_mappers``, ``windows``, ``deadline_references``,
       ``priority_weight_strategies``
     - Only usable by that team's Dags.
   * - ``listeners``
     - None. Every listener receives events for all teams.
   * - ``ui_translations``, ``hook_lineage_readers``, ``flask_blueprints``,
       ``appbuilder_views``, ``appbuilder_menu_items``, ``admin_views``, ``menu_links``
     - None. These stay global.

Listeners are deployment-wide by design: every listener, including one a team plugin registers,
receives events for every team's Dags. Their hooks fire in shared components such as the scheduler
and the API server, so choosing which listeners run is the Deployment Manager's responsibility.

API endpoints
^^^^^^^^^^^^^

A team plugin's ``fastapi_apps`` are mounted with a middleware that resolves the caller (bearer
token or UI session cookie, exactly as the core API does) and asks the auth manager whether that
user is authorized for the team. A caller the middleware cannot authenticate gets the same
``401``/``403`` the core API returns for that token; an authenticated caller who is not authorized
for the team gets ``403``. The request's HTTP method is mapped onto one of Airflow's resource
methods (``GET``/``HEAD``/``OPTIONS`` → ``GET``, ``POST`` → ``POST``, ``PUT``/``PATCH`` → ``PUT``,
``DELETE`` → ``DELETE``) before it is passed on, so an auth manager that distinguishes methods can
grant a team read-only access to its own plugin. Auth managers that only check membership can
ignore it.

This is the only authorization Airflow adds to a plugin app. Whether the caller may perform a
given action *within* the team is still the plugin's decision, and a **global** plugin's app gets
no authentication and no team check at all — see the warning in :ref:`the example above
<plugin-example>`.

``fastapi_root_middlewares`` are not scoped. A root middleware wraps every request to the API
server, including core routes and other teams' plugins, so one declared by a team plugin is
skipped and a warning is logged. A team plugin that needs middleware should apply it inside its
own FastAPI app, where it only sees that app's requests.

UI elements
^^^^^^^^^^^

The UI builds its navigation items, external views and React apps from ``GET /api/v2/plugins``,
which returns global plugins plus the plugins of teams the caller is authorized for. A team
plugin's UI pieces are therefore only offered to that team's users. The endpoint still requires
the existing *Plugins* view permission; team scoping narrows what that permission returns rather
than introducing a separate one.

``GET /api/v2/plugins/importErrors`` is **not** filtered by team: anyone with the *Plugins* view
permission sees the import errors of all plugins, including their source paths and error text. A
plugin load failure is deployment-level information that the person debugging it needs, so it is
reported the same way to everyone who may see the plugins page at all.

Macros
^^^^^^

``{{ macros.<plugin_name>.<macro> }}`` resolves only for that team's tasks. A task of another
team, or of a teamless Dag, gets an ``AttributeError`` naming the owning team. Built-in macros and
global plugins' macros are unaffected.

This is logical scoping not an isolation boundary. The plugin's macro module is imported into
the worker process like any other, so task code that goes looking for it — through
``sys.modules``, say — will still find it. The scoping keeps one team's macros out of another
team's templates; it does not stop a task author who sets out to reach them.

Operator extra links
^^^^^^^^^^^^^^^^^^^^

Extra links a team plugin registers, through either ``global_operator_extra_links`` or
``operator_extra_links``, are shown only on task instances of that team's Dags (not on another
team's Dags and not on teamless ones). Filtering is by the team of the Dag the link would be
rendered for.

Links that a global plugin registers as well as links that an operator defines itself are
untouched. A link class registered by both a team plugin and a global plugin stays visible
everywhere: the global registration wins, so a team plugin cannot withdraw a link from the
rest of the deployment.

Scheduling classes
^^^^^^^^^^^^^^^^^^

Timetables, partition mappers, windows, deadline references and priority weight strategies are
named by a Dag directly, so scoping them means deciding which Dags may name them. A Dag's team is
the team that owns the bundle it was parsed from (see :ref:`multi-team-dag-bundles`). If only
team-scoped plugins register a class, a Dag that names it and does not belong to one of those
teams is **not stored**, and gets an import error naming the class, its owning team and the two
ways out: move the Dag into a bundle owned by that team, or have the plugin provide the class
globally. The rest of the bundle is stored normally, so one such Dag does not take its neighbours
down with it.

A class that any global plugin also registers stays available to every Dag. Airflow's own
timetables, partition mappers and windows (anything under ``airflow.timetables.`` or
``airflow.partition_mappers.``) are never team-owned, even if a team plugin lists them:
deserialization imports those paths directly and never consults plugins.

One gap remains: a partition mapper that a timetable picks inside ``get_partition_mapper()`` is
not covered, because nothing names it until the timetable runs. As with macros, this is logical
scoping, it decides which Dags Airflow will schedule with a class, not what Dag code is able to
import.

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
