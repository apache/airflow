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


Add your own actions to Dags, Dag Runs and Task Instances
=========================================================

Airflow renders a fixed set of actions for each entity — pause, trigger, clear, mark as,
delete. ``AirflowPlugin.ui_actions`` lets you add your own beside them, everywhere Airflow already
offers actions for that entity:

* the trailing cell of its **table rows**,
* its **card**, on lists that have a card view, and
* the action row of its **header card** on the detail page.

An action either opens a URL or mounts a React component of your own. Unlike
``external_views`` and ``react_apps``, which each take over a whole tab, an action is a single
button — the right size for "open the runbook for this Dag" or "show me this task's payload".

Your first action
-----------------

Drop this in a file under your :doc:`plugins folder </administration-and-deployment/plugins>`:

.. code-block:: python

    from airflow.plugins_manager import AirflowPlugin


    class RunbookPlugin(AirflowPlugin):
        name = "runbook_plugin"

        ui_actions = [
            {
                "action_type": "link",
                "name": "Open runbook",
                "destination": "dag",
                "href": "https://runbooks.example.com/{DAG_ID}",
            }
        ]

Restart the API server. Every Dag now carries an "Open runbook" button — on each row and card
of the Dags list, and in the header of each Dag's detail page — pointing at that Dag's
runbook.

Four keys are required: ``action_type``, ``name``, ``destination``, and the target the kind
needs (``href`` for ``link``, ``bundle_url`` for ``react``). ``name`` is the button's tooltip
and its accessible label, so write it as a label rather than an identifier.

``destination`` picks the entity, and with it the surfaces the button appears on:

.. list-table::
   :header-rows: 1

   * - ``destination``
     - Appears on
     - Template variables
   * - ``dag``
     - Dags list rows and cards, Dag detail header
     - ``{DAG_ID}``
   * - ``dag_run``
     - Dag Runs rows, Dag Run detail header
     - ``{DAG_ID}``, ``{RUN_ID}``
   * - ``task_instance``
     - Task Instances rows, Task Instance detail header
     - ``{DAG_ID}``, ``{RUN_ID}``, ``{TASK_ID}``, ``{MAP_INDEX}``,
       ``{REGION_ID}``, ``{REGION_INDEX}``

A variable with no value on the current page is left in the URL as-is rather than replaced
with an empty string, so a half-resolved link fails visibly instead of quietly pointing
somewhere else. Values are URL-encoded for you.

``{REGION_ID}`` and ``{REGION_INDEX}`` identify one iteration of a looping task, and resolve
only on rows that have them; on every other task instance they are left in the URL like any
other variable with no value.

Internal and external links
---------------------------

A link that matches an Airflow UI route navigates inside the app, with no page reload:

.. code-block:: python

    {
        "action_type": "link",
        "name": "Jump to runs",
        "destination": "dag",
        "href": "/dags/{DAG_ID}/runs",
    }

A path is resolved against the deployment's base path, so the same plugin works whether
Airflow is served from the root or from under a prefix like ``http://host/airflow`` -- both
``/dags/{DAG_ID}/runs`` and ``dags/{DAG_ID}/runs`` reach the Dag's runs. You do not need to
read ``[api] base_url`` to build an internal link.

Anything else is external: it opens in a new tab, and a small arrow is badged onto the corner
of its button so people can tell before they click.

"External" is wider than "a different host". A page served by your own ``fastapi_apps``
shares Airflow's origin but is not a UI route, so it counts as external too — following it in
the same tab would drop the user out of the UI and lose their place in whatever list they were
looking at. You do not configure any of this; it is decided from the URL.

Limiting an action to the Dags that need it
-------------------------------------------

An action with no scoping appears on every entity of its kind, which gets noisy fast. The
optional ``applies_to`` block is the same one external views and React apps use:

.. code-block:: python

    {
        "action_type": "link",
        "name": "Open runbook",
        "destination": "dag",
        "href": "https://runbooks.example.com/{DAG_ID}",
        "applies_to": {"dag.tags.name": ["production"]},
    }

Keys are dotted field paths into the records the surface has, so anything the REST API returns
for the entity is addressable. The case this is most often wanted for is state:

.. code-block:: python

    {
        "action_type": "link",
        "name": "Investigate failure",
        "destination": "task_instance",
        "href": "https://ai.example.com/investigate?dag={DAG_ID}&task={TASK_ID}&run={RUN_ID}",
        # Offered on failed task instances and nowhere else.
        "applies_to": {"state": ["failed", "upstream_failed"]},
    }

Values are OR-ed within a path and AND-ed across paths, and a path whose root record the
current surface cannot resolve is skipped rather than failing the match. See
:ref:`Scoping a view to specific Dags and tasks <plugin-scoping>` for the full semantics.

One consequence is worth knowing before you rely on it. On the deployment-wide **Dag Runs**
and **Task Instances** lists, each row is its own record but Airflow has not loaded the parent
Dag, so a ``dag.*`` path cannot be judged there and is skipped — the action shows on every
row. Paths rooted at the row itself, like ``state`` or ``dag_id``, are evaluated normally, so
pair them if that matters:

.. code-block:: python

    "applies_to": {"dag.tags.name": ["production"], "dag_id": ["checkout_etl"]}

Everywhere else — the Dags list and all three header cards — the Dag is available.

Actions that open a surface
---------------------------

.. warning::
  React plugins are experimental. The feature may change in future versions without warning
  based on user feedback and errors reported.

|experimental|

A ``react`` action mounts a component you supply. Airflow renders **only the button** — your
component renders its own surface, so it can be a popover, a modal, a side drawer or anything
else, and it owns its own appearance and accessibility.

.. code-block:: python

    {
        "action_type": "react",
        "name": "Inspect payload",
        "destination": "task_instance",
        "bundle_url": "https://example.com/static/js/inspect_payload.js",
    }

Alongside the entity context props — the same ones documented in
:ref:`React app context props <plugin-react-props>` — your component receives three props for
driving its surface:

``open``
    Whether the button is currently toggled on. Your component is mounted the first time the
    button is clicked and stays mounted afterwards, so it can animate out and keep its state;
    follow ``open`` rather than assuming you are unmounted when closed.

``onClose``
    Call it to toggle the button back off — after submitting, or when your surface is
    dismissed by a backdrop click or Escape.

``triggerRef``
    A ref to the button element. A popover needs it to anchor against, since the button
    belongs to Airflow rather than to you.

Note the context comes from the **entity whose button was clicked**, not from the page's URL.
A row's action lives on a list page whose URL says nothing about that row, so read ``dagId``,
``runId`` and ``taskId`` from props rather than from the router.

A minimal component, with no build step, borrowing React and Chakra from the host — enough to
see the contract working before you reach for tooling:

.. code-block:: javascript

    const React = globalThis.React;
    const { Popover, Portal, Text } = globalThis.ChakraUI;
    const h = React.createElement;

    globalThis["Inspect payload"] = ({ onClose, open, taskId, triggerRef }) =>
      h(
        Popover.Root,
        {
          onOpenChange: (event) => { if (!event.open) onClose(); },
          open: Boolean(open),
          positioning: {
            getAnchorRect: () => triggerRef?.current?.getBoundingClientRect() ?? null,
            placement: "bottom-end",
          },
        },
        h(Portal, null,
          h(Popover.Positioner, null,
            h(Popover.Content, null,
              h(Popover.Body, null, h(Text, null, `Task: ${taskId}`)),
            ),
          ),
        ),
      );

The bundle must register a function component on ``globalThis`` under the action's ``name``.
Airflow also accepts the shared ``globalThis.AirflowPlugin`` global, which is what the
bootstrap tool below produces.

For anything beyond a demo, use the React plugin bootstrap tool described in
:doc:`custom-view-plugin` — it generates a Vite project with the bundling configuration
Airflow needs, and shares React with the host rather than shipping its own copy.

Two rules for React actions
^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Give every React action a distinct name.** The component is registered on the page under
its ``name``, so two React actions sharing one — or a React action sharing a name with a
``react_apps`` entry — would render each other's component. Airflow warns about this at load
time and skips the action rather than letting it happen. A ``react_apps`` entry always keeps
the name; between two actions, the plugin that loads first keeps it. The name also cannot be
blank or ``AirflowPlugin``, which is the shared slot Airflow clears before each bundle loads.

.. warning::

    **Do not name a React action after a browser global.** The component is looked up at
    ``globalThis[name]``, and the browser has already put plenty of ordinary words there --
    ``Comment``, ``File``, ``History``, ``Image``, ``Notification``, ``Plugin``, ``Request``
    and ``Text`` among them. Because ``name`` is also the button's label, these are exactly
    the words a label reaches for.

    The lookup finds the browser's constructor before your bundle's registration, so the
    button does nothing and the reason only appears in the console. A bundle that registers
    under that name instead replaces the browser's global for the whole page, which can break
    Airflow itself. Airflow cannot reject these for you: the set differs by browser and
    version. Prefix the name, or make it a phrase -- ``"Inspect payload"`` rather than
    ``"Image"``.

    ``react_apps`` are looked up the same way and have the same hazard.

**Give every React action its own bundle_url.** Airflow clears the shared global before each
load, and a browser will not re-execute a module it has already imported, so a second action
pointing at a bundle that is already loaded finds nothing registered under its name. If you
genuinely want one bundle to back several actions, register each name explicitly from it.

Serving a bundle from your own plugin
-------------------------------------

A ``bundle_url`` can point anywhere the browser can reach, including a CDN. To serve it from
Airflow itself, mount a FastAPI app in the same plugin:

.. code-block:: python

    from fastapi import FastAPI
    from fastapi.staticfiles import StaticFiles

    from airflow.plugins_manager import AirflowPlugin

    app = FastAPI()
    app.mount("/static", StaticFiles(directory="/path/to/dist"), name="inspect_static")


    class InspectPlugin(AirflowPlugin):
        name = "inspect_plugin"

        fastapi_apps = [{"name": "Inspect", "app": app, "url_prefix": "/inspect"}]

        ui_actions = [
            {
                "action_type": "react",
                "name": "Inspect payload",
                "destination": "task_instance",
                "bundle_url": "inspect/static/main.umd.cjs",
            }
        ]

A ``bundle_url`` is resolved the same way a link's ``href`` is, so write it relative --
``inspect/static/main.umd.cjs`` -- and it loads whether Airflow is served from the root or
from under a prefix. A leading slash ties the plugin to a root deployment.

When an action does not appear
------------------------------

Airflow skips an action it cannot render and logs why, rather than failing the plugin. Check
the API server log at startup for a line naming your plugin. An action is skipped when it:

* is not a dictionary;
* names an unknown ``action_type`` or ``destination``;
* omits the ``href`` or ``bundle_url`` its kind requires, or has an empty ``name``;
* is a ``react`` action whose ``name`` collides with another React component.

``applies_to`` is reported separately, and what happens depends on what is wrong with it. A
malformed block — not a dictionary, a non-string path, a value that is not a list of strings —
is stripped, and the action loads unscoped. A path the ``destination`` cannot evaluate is
warned about and skipped, which is what lets one block be shared across destinations. But a
path naming a field *no* record has is a misconfiguration rather than a filter that never
matches, so the action is not loaded at all and the reason is logged as an error: it could
never scope the way it asks to, and showing it everywhere would be the opposite of the intent.
See :doc:`/administration-and-deployment/plugins` for the path syntax.

The rest of the plugin is unaffected by any of this: one bad action does not cost you the
plugin's other actions, its views, or its macros.

Security
--------

.. note::
    ``applies_to`` decides whether a button is drawn, not what its target will do. Anyone who
    can read ``GET /plugins`` sees every action's ``href`` and ``bundle_url`` and can request
    them directly, for any entity — including ones the scoping would have hidden the button
    for. A link action's target must authenticate and authorize every request itself, and
    validate the identifiers it is handed rather than trusting them.

.. seealso::

    - :doc:`/administration-and-deployment/plugins` — the full field reference for
      ``ui_actions``, ``external_views`` and ``react_apps``.
    - :doc:`custom-view-plugin` — plugin tabs and pages, and the React bootstrap tool.
