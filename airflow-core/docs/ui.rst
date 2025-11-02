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



UI Overview
===========
The Airflow UI provides a powerful way to monitor, manage, and troubleshoot your data pipelines and data assets. As of
Airflow 3, the UI has been refreshed with a modern look, support for dark and light themes, and a redesigned navigation
experience.

This guide offers a reference-style walkthrough of key UI components, with annotated screenshots to help new and
experienced users alike.

.. note::
   Screenshots in this guide use **dark theme** by default. Select views are also shown in **light theme** for comparison. You can toggle themes from user settings located at the bottom corner of the Airflow UI.

.. _ui-home:

Home Page
---------
The Home Page provides a high-level overview of the system state and recent activity. It is the default landing page in
Airflow 3 and includes:

- **Health indicators** for system components such as the MetaDatabase, Scheduler, Triggerer, and Dag Processor
- **Quick links** to Dags filtered by status (e.g., Failed Dags, Running Dags, Active Dags)
- **Dag and Task Instance history**, showing counts and success/failure rates over a selectable time range
- **Recent asset events**, including materializations and triggered Dags

This page is useful for quickly assessing the health of your environment and identifying recent issues or
asset-triggered events.

.. image:: img/ui-dark/home_dark.png
   :alt: Airflow Home Page showing system health, Dag/task stats, and asset events (Dark Mode)

|

.. image:: img/ui-light/home_light.png
   :alt: Airflow Home Page showing system health, Dag/task stats, and asset events (Light Mode)

.. _ui-dag-list:

Dag List View
-------------

The Dag List View appears when you click the **Dags** tab in the main navigation bar. It displays all Dags available in
your environment, with a clear summary of their status, recent runs, and configuration.

Each row includes:

- **Dag ID**
- **Schedule** and next run time
- **Status of the latest Dag run**
- **Bar chart of recent runs**
- **Tags**, which can be used for grouping or filtering Dags (e.g., ``example``, ``produces``)
- **Pause/resume toggle**
- Links to access Dag-level views

At the top of the view, you can:

- Use **filters** for Dag status, schedule state, and tags
- Use **search** or **advanced search (⌘+K)** to find specific Dags
- Sort the list using the dropdown (e.g., Latest Run Start Date)

.. image:: img/ui-dark/dag_list.png
   :alt: Dag List View in dark mode showing search, filters, and Dag-level controls

|

.. image:: img/ui-light/dag_list.png
   :alt: Dag List View in light mode showing the same Dags and actions for comparison

|

Some Dags in this list may interact with data assets. For example, Dags that are triggered by asset conditions may
display popups showing upstream asset inputs.

.. image:: img/ui-dark/dag_list_asset_condition_popup.png
   :alt: Dag List View showing asset condition popup (Dark Mode)

|

.. image:: img/ui-light/dag_list_asset_condition_popup.png
   :alt: Dag List View showing asset condition popup (Light Mode)

.. _ui-dag-details:

Dag Details Page
----------------

Clicking a Dag from the list opens the Dag Details Page. This view offers centralized access to a Dag's metadata, recent
activity, and task-level diagnostics.

Key elements include:

- **Dag metadata**, including ID, owner, tags, schedule, and latest Dag version
- **Action buttons** to trigger the Dag, reparse it, or pause/resume
- **Tabbed interface**: Overview (recent failures, run counts, task logs); Grid View (status heatmap); Graph View (task dependencies); Runs (full run history); Tasks (aggregated stats); Events (system- or asset-triggered); Code (Dag source); and Details (extended metadata)

This page also includes a visual **timeline of recent Dag runs** and a **log preview for failures**, helping users quickly identify issues across runs.

.. image:: img/ui-dark/dag_overview_dashboard.png
   :alt: Dag Details Page in dark mode showing overview dashboard and failure diagnostics

|

.. image:: img/ui-light/dag_overview_dashboard.png
   :alt: Dag Details Page in light mode showing overview dashboard and failure diagnostics

.. _ui-grid-view:

Grid View
'''''''''

The Grid View is the primary interface for inspecting Dag runs and task states. It offers an interactive way to debug,
retry, or monitor workflows over time.

Use Grid View to:

- **Understand the status of recent Dag runs** at a glance
- **Identify failed or retried tasks** by color and tooltip
- **Take action** by clicking a task cell to view logs or mark task instances as successful, failed, or cleared
- **Filter tasks** by name or partial ID
- **Select a run range**, like "last 25 runs" using the dropdown above the grid

Each row represents a task, and each column represents a Dag run. You can hover over any task instance for more detail,
or click to drill down into logs and metadata.

.. image:: img/ui-dark/dag_overview_grid.png
   :alt: Grid View showing Dag run status matrix with varied task states (Dark Mode)

|

.. image:: img/ui-light/dag_overview_grid.png
   :alt: Grid View showing Dag run status matrix with varied task states (Light Mode)

.. _ui-graph-view:

Graph View
''''''''''

The Graph View shows the logical structure of your Dag - how tasks are connected, what order they run in, and how
branching or retries are configured.

This view is helpful when:

- **Debugging why a task didn't run** (e.g., skipped due to a trigger rule)
- **Understanding task dependencies** across complex pipelines
- **Inspecting run-specific task states** (e.g., success, failed, upstream failed)

Each node represents a task. Edges show the dependencies between them. You can click any task to view its metadata and
recent run history.

Use the dropdown at the top to switch between Dag runs and see how task states changed across executions.

.. image:: img/ui-dark/dag_overview_graph.png
   :alt: Graph View showing Dag structure with no Dag run selected (Dark Mode)

|

.. image:: img/ui-light/dag_overview_graph.png
   :alt: Graph View showing Dag structure with no Dag run selected (Light Mode)

.. _ui-dag-tabs:

Dag Tabs
--------
In addition to the interactive views like Grid and Graph, the Dag Details page includes several other tabs that provide
deeper insights and metadata:

Runs Tab
''''''''
The **Runs** tab displays a sortable table of all Dag runs, along with their status, execution duration, run type, and Dag version.

.. image:: img/ui-dark/dag_overview_runs.png
   :alt: Dag Runs Tab (Dark Mode)

|

.. image:: img/ui-light/dag_overview_runs.png
   :alt: Dag Runs Tab (Light Mode)

|

Tasks Tab
'''''''''

The **Tasks** tab shows metadata for each task in the Dag, including operator type, trigger rule, most recent run status, and run history.

.. image:: img/ui-dark/dag_overview_tasks.png
   :alt: Dag Tasks Tab (Dark Mode)

|

.. image:: img/ui-light/dag_overview_tasks.png
   :alt: Dag Tasks Tab (Light Mode)

|

Events Tab
''''''''''

The **Events** tab surfaces structured events related to the Dag, such as Dag triggers and version patches. This tab is especially useful for Dag versioning and troubleshooting changes.

.. image:: img/ui-dark/dag_overview_events.png
   :alt: Dag Events Tab (Dark Mode)

|

.. image:: img/ui-light/dag_overview_events.png
   :alt: Dag Events Tab (Light Mode)

Code Tab
''''''''

The **Code** tab displays the current version of the Dag definition, including the timestamp of the last parse. Users can view the code for any specific Dag version.

.. image:: img/ui-dark/dag_overview_code.png
   :alt: Dag Code Tab (Dark Mode)

|

.. image:: img/ui-light/dag_overview_code.png
   :alt: Dag Code Tab (Light Mode)

|

Details Tab
'''''''''''

The **Details** tab provides configuration details and metadata for the Dag, including schedule, file location, concurrency limits, and version identifiers.

.. image:: img/ui-dark/dag_overview_details.png
   :alt: Dag Details Tab (Dark Mode)

|

.. image:: img/ui-light/dag_overview_details.png
   :alt: Dag Details Tab (Light Mode)

.. _ui-dag-runs:

Dag Run View
------------
Each Dag Run has its own view, accessible by selecting a specific row in the Dag's **Runs** tab. The Dag Run view
displays metadata about the selected run, as well as task-level details, rendered code, and more.

.. image:: img/ui-dark/dag_run_task_instances.png
  :alt: Dag Run - Task Instances tab (dark mode)

|


Key elements include:

- **Dag Run metadata**, including logical date, run type, duration, Dag version, and parsed time
- **Action buttons** to clear or mark the run, or add a note
- A persistent **Grid View sidebar**, which shows task durations and states across recent Dag runs. This helps spot recurring issues or performance trends at a glance.

Dag Run Tabs
------------

Task Instances
''''''''''''''

Displays the status and metadata for each task instance within the Dag Run. Columns include:

- Task ID
- State
- Start and End Dates
- Try Number
- Operator Type
- Duration
- Dag Version

Each row also includes a mini Gantt-style timeline that visually represents the task's duration.

.. image:: img/ui-light/dag_run_task_instances.png
   :alt: Dag Run - Task Instances (light mode)

Events
''''''

If available, this tab lists system-level or asset-triggered events that contributed to this Dag Run's execution.

Code
''''

Displays the Dag source code as it was at the time this run was parsed. This view is helpful for debugging version drift
or comparing behavior across Dag Runs that used different code.

Dag Run code for ``hello >> airflow()``:

.. image:: img/ui-dark/dag_run_code_hello_airflow.png
   :alt: Dag Run Code Snapshot - airflow() (dark mode)

|

.. image:: img/ui-light/dag_run_code_hello_airflow.png
   :alt: Dag Run Code Snapshot - airflow() (light mode)

|

Dag Run code for ``hello >> world()``:

.. image:: img/ui-dark/dag_run_code_hello_world.png
   :alt: Dag Run Code Snapshot - world() (dark mode)

|

.. image:: img/ui-light/dag_run_code_hello_world.png
   :alt: Dag Run Code Snapshot - world() (light mode)

Details
'''''''

Provides extended metadata for the Dag Run, including:

- Run ID and Trigger Type
- Queued At, Start and End Time, and Duration
- Data Interval boundaries
- Trigger Source and Run Config
- Dag Version ID and Bundle Name

.. image:: img/ui-dark/dag_run_details.png
   :alt: Dag Run - Details tab (dark mode)

|

.. image:: img/ui-light/dag_run_details.png
   :alt: Dag Run - Details tab (light mode)

Graph View
''''''''''

Shows the Dag's task dependency structure overlaid with the status of each task in this specific run. This is useful for visual debugging of task failure paths or identifying downstream blockers.

Each node includes a visual indicator of task duration.

.. image:: img/ui-dark/dag_run_graph.png
   :alt: Dag Run - Graph View (dark mode)

|

.. image:: img/ui-light/dag_run_graph.png
   :alt: Dag Run - Graph View (light mode)

.. _ui-ti-view:

Dag Trigger Window
------------------

Single Run
''''''''''

The Single Run window allows you to trigger a Dag run.

.. image:: img/ui-light/dag_trigger_window_single_run.png
   :alt: Dag Trigger Window - Single Run (Light Mode)

|

.. image:: img/ui-dark/dag_trigger_window_single_run.png
   :alt: Dag Trigger Window - Single Run (Dark Mode)

Backfill
''''''''

The Backfill window allows you to trigger a Dag run for past dates.

.. image:: img/ui-light/backfill.png
   :alt: Dag Trigger Window - Backfill (Light Mode)

|

.. image:: img/ui-dark/backfill.png
   :alt: Dag Trigger Window - Backfill (Dark Mode)

Task Instance View
------------------

When you click on a specific task from the Dag Run view, you're brought to the **Task Instance View**, which shows
detailed logs and metadata for that individual task execution.

.. image:: img/ui-dark/dag_task_instance_logs.png
  :alt: Task Logs (dark mode)

.. _ui-ti-tabs:

Task Instance Tabs
------------------

Each task instance has a tabbed view providing access to logs, rendered templates, XComs, and execution metadata.

Logs
''''
The default tab shows the task logs, which include system output, error messages, and traceback information. This is the first place to look when a task fails.

.. image:: img/ui-light/dag_task_instance_logs.png
  :alt: Task Logs (light mode)

Rendered Templates
''''''''''''''''''
Displays the rendered version of templated fields in your task. Useful for debugging context variables or verifying
dynamic content.

XCom
''''
Shows any values pushed via ``XCom.push()`` or returned from Python functions when using TaskFlow.

.. image:: img/ui-dark/dag_run_task_instance_xcom.png
  :alt: Task Instance - XCom tab (dark mode)

|

.. image:: img/ui-light/dag_run_task_instance_xcom.png
  :alt: Task Instance - XCom tab (light mode)

Events
''''''
If present, displays relevant events related to this specific task instance execution.

Code
''''
Shows the Dag source code parsed at the time of execution. This helps verify what version of the Dag the task ran with.

Details
'''''''
Displays runtime metadata about the task instance, including:

- Task ID and State
- Dag Run ID, Dag Version, and Bundle Name
- Operator used and runtime duration
- Pool and slot usage
- Executor and configuration

.. image:: img/ui-dark/dag_task_instance_details.png
  :alt: Task Instance - Details tab (dark mode)

|

.. image:: img/ui-light/dag_task_instance_details.png
  :alt: Task Instance - Details tab (light mode)

.. _ui-asset-views:

Asset Views
-----------

The **Assets** section provides a dedicated interface to monitor and debug asset-centric workflows. Assets represent
logical data units—such as files, tables, or models—that tasks can produce or consume. Airflow tracks these dependencies
and provides visualizations to better understand their orchestration.

Asset List
''''''''''

The Asset List shows all known assets, grouped by name. For each asset, you can see:

- The group the asset belongs to (if any)
- The Dags that consume the asset
- The tasks that produce the asset

Hovering over a count of Dags or tasks shows a tooltip with the full list of producers or consumers.

.. image:: img/ui-dark/asset_list_consuming_dags.png
  :alt: Asset Graph View (dark mode)

|

.. image:: img/ui-light/asset_list_consuming_dags.png
  :alt: Asset Graph View (light mode)

Clicking on the link takes you to the Asset Graph View.

Asset Graph View
''''''''''''''''

The Asset Graph View shows the asset in context, including upstream producers and downstream consumers. You can use this view to:

- Understand asset lineage and the Dags involved
- Trigger asset events manually
- View recent asset events and the Dag runs they triggered

.. image:: img/ui-dark/asset_view.png
  :alt: Asset Graph View (dark mode)

|

.. image:: img/ui-light/asset_view.png
  :alt: Asset Graph View (light mode)


Graph Overlays in Dag View
''''''''''''''''''''''''''

When a Dag contains asset-producing or asset-consuming tasks, you can enable asset overlays on the Dag Graph view. Toggle the switches next to each asset to:

- See how assets flow between Dags
- Inspect asset-triggered dependencies

Two graph modes are available:

- **All Dag Dependencies**: Shows all Dag-to-Dag and task-level connections

  .. image:: img/ui-dark/dag_graph_all_dependencies.png
    :alt: Dag Graph View - All Dependencies (dark mode)

  |

  .. image:: img/ui-light/dag_graph_all_dependencies.png
    :alt: Dag Graph View - All Dependencies (light mode)

  |

- **External Conditions**: Shows only Dags triggered via asset events

  .. image:: img/ui-dark/dag_graph_external_conditions.png
    :alt: Dag Graph View - External Conditions Only (dark mode)

  |

  .. image:: img/ui-light/dag_graph_external_conditions.png
    :alt: Dag Graph View - External Conditions Only (light mode)

.. _ui-admin-views:

Admin Views
-----------

The **Admin** tab provides system-level tools for configuring and extending Airflow. These views are primarily intended for administrators and platform operators responsible for deployment, integration, and performance tuning.

Key pages include:

- **Variables** – Store key-value pairs accessible from Dags. Variables can be used to manage environment-specific parameters or secrets.
- **Connections** – Define connection URIs to external systems such as databases, cloud services, or APIs. These are consumed by Airflow operators and hooks.
- **Pools** – Control resource allocation by limiting the number of concurrently running tasks assigned to a named pool. Useful for managing contention or quota-constrained systems.
- **Providers** – View installed provider packages (e.g., ``apache-airflow-providers-google``), including available hooks, sensors, and operators. This is helpful for verifying provider versions or troubleshooting import errors.
- **Plugins** – Inspect registered Airflow plugins that extend the platform via custom operators, macros, or UI elements.
- **Config** – View the full effective Airflow configuration as parsed from ``airflow.cfg``, environment variables, or overridden defaults. This can help debug issues related to scheduler behavior, secrets backends, and more.

.. note::
   The Admin tab is only visible to users with appropriate RBAC permissions.

------------

.. image:: img/ui-dark/variable_hidden.png

------------

.. image:: img/ui-dark/admin_connections.png

------------

.. image:: img/ui-dark/admin_connections_add.png
