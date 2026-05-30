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

.. _concepts-dag-versioning:

Dag Versioning
==============

.. versionadded:: 3.0

Airflow automatically tracks changes to your Dag definitions over time through Dag versioning. Each time
you modify a Dag's structure and the scheduler parses the updated file, Airflow creates a new Dag version
that captures the serialized Dag structure and code at that point in time.

This means that when you view a historical Dag run in the UI, you see the exact Dag structure and code as
it existed when that run executed --- not the current version of the Dag. This is essential for debugging,
auditing, and understanding the behavior of past runs, especially in production environments where Dags
evolve frequently.

.. note::

    Dag versioning is primarily a **UI and history feature**. When Airflow executes a task, it always uses
    the **latest** Dag code and structure, not the version from the original run. The one exception is when
    using a versioned Dag bundle (such as ``GitDagBundle``), which can check out and run code from a
    specific commit. See :ref:`Bundle Version Tracking <bundle-version-tracking>` below for details.

Each Dag version is identified by a version number that auto-increments per Dag (e.g., ``my_dag-1``,
``my_dag-2``). Versions are immutable once created; modifying your Dag always produces a new version rather
than updating an existing one.

How Dag Versions Are Created
----------------------------

A new Dag version is created when the scheduler parses a Dag file and detects that the Dag's structure has
changed since the last version. Structural changes include:

- Tasks added or removed
- Task dependencies changed
- Task parameters modified (e.g., ``bash_command``, ``trigger_rule``)
- Dag-level parameters modified (e.g., ``schedule``, ``default_args``)

Each Dag version stores:

- The **serialized Dag structure** (tasks, dependencies, and configuration)
- The **Dag code** (the Python source of the Dag file)
- The **bundle version** (e.g., a git commit hash, if using a versioned Dag bundle)

.. note::

    Cosmetic changes to your Dag file that do not affect the serialized structure (such as adding comments
    or reformatting code) may not trigger a new version, since Airflow compares the serialized representation
    rather than the raw source text.

**Example:** Consider a simple Dag with two tasks:

.. code-block:: python

    from airflow.sdk import dag, task


    @dag(schedule="@daily")
    def etl_pipeline():
        @task.bash
        def extract():
            return "echo extract"

        @task.bash
        def transform():
            return "echo transform"

        extract() >> transform()


    etl_pipeline()

This creates version 1 (``etl_pipeline-1``). Now you add a ``load`` task:

.. code-block:: python

    from airflow.sdk import dag, task


    @dag(schedule="@daily")
    def etl_pipeline():
        @task.bash
        def extract():
            return "echo extract"

        @task.bash
        def transform():
            return "echo transform"

        @task.bash
        def load():
            return "echo load"

        extract() >> transform() >> load()


    etl_pipeline()

When the scheduler next parses this file, it detects the new task and dependency, and creates version 2
(``etl_pipeline-2``). Any new Dag runs will use version 2, while existing runs retain their association
with version 1.


Scenarios
---------

The following scenarios illustrate how Dag versioning works in practice. They cover the most common
situations you will encounter when working with evolving Dags.

Scenario 1: Dag Changes Between Runs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This is the most common case. You update your Dag, and new runs use the new version while old runs
retain the version they were created with.

**Example:** Your Dag initially has two tasks (``extract`` and ``transform``). You deploy an update that
adds a third task (``load``). Runs created before the update show two tasks; runs created after show three.

In the UI:

- **Grid view:** Tasks that were removed in a newer version still appear in the grid but with empty status
  cells for runs that used the newer version.
- **Graph view:** Use the Dag Version dropdown to switch between versions. Selecting an older version
  displays the two-task graph; selecting a newer version displays the three-task graph.
- **Code tab:** Use the Dag Version dropdown to view the code as it existed for any version.

.. image:: /img/ui-light/dag_version_grid.png
   :alt: Grid view showing runs across two Dag versions with empty status cells for tasks removed in a newer version (Light Mode)


Scenario 2: Dag Changes During a Run
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If a Dag changes while a run is in progress, some task instances may execute with one version while others
execute with a different version. Airflow tracks the Dag version per task instance, so you can always see
which version each task actually ran with.

There are two sub-cases to consider:

**Non-structural changes (code only)**

If the change modifies task logic (e.g., a different ``bash_command``) but does not alter the set of task
IDs or their dependency relationships, the Dag structure remains the same. The grid view still shows the
same tasks, but the code tab reveals the updated code for task instances that ran after the change.

**Example:** While ``extract`` is running, you deploy a change that modifies the ``transform`` task's
``bash_command`` from ``"echo transform"`` to ``"python transform.py"``, creating version 2. The task IDs
and dependencies are unchanged, but ``transform`` runs with version 2's code.

**Structural changes (tasks or dependencies changed)**

If the change adds, removes, or renames tasks, or modifies task dependencies, the Dag structure itself
changes. This is the more visible case --- new tasks may appear, removed tasks show empty cells, and the
graph view looks different between versions.

**Example:** A Dag run starts with version 1 (tasks ``extract`` and ``transform``). While ``extract`` is
running, you deploy a Dag update that adds a ``load`` task, creating version 2. When ``transform`` and
``load`` are scheduled, they run with version 2.

In the UI for both sub-cases:

- **Dag run details:** The details panel lists all Dag versions used during the run, so you can see that
  multiple versions were involved.
- **Graph view:** Use the Dag Version dropdown to switch between the versions used within that run to
  compare the original and updated Dag structures.
- **Code tab:** Use the Dag Version dropdown to switch between versions and inspect the code for each.

.. image:: /img/ui-light/dag_version_scenario2_run_details.png
   :alt: Dag run details panel listing two Dag versions used within a single run (Light Mode)

.. note::

    This scenario is uncommon in most deployments, but it is important to understand for long-running Dags
    or environments with frequent deployments.


Scenario 3: Task Instance Reruns Across Versions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When a task instance is rerun after a Dag change --- whether through an automatic retry or a manual clear
--- each attempt may use a different Dag version. Airflow tracks the version for each attempt, so you can
see exactly which code and structure were used for every run.

**Example:** Task ``transform`` fails on its first attempt under version 1. Before the retry, you deploy a
Dag update (version 2) that changes the ``transform`` task's ``bash_command``. The retry executes with
version 2's code.

In the UI:

- **Task instance details:** Shows the Dag version for the task instance, and you can use the try number
  selector to switch between attempts.
- **Code tab:** Use the Dag Version dropdown to view the code that was active for each attempt.
- **Graph view:** Use the Dag Version dropdown to compare the Dag structure across different versions.
- **Logs:** Each attempt's logs are associated with the Dag version that was active during that attempt,
  so you can correlate log output with the specific code that produced it.

.. TODO: Add screenshot — Task instance details panel showing the Dag version for each try number.
..
..   Setup to reproduce:
..   1. Create a Dag with a task that intentionally fails on attempt 1 (e.g., ``BashOperator`` running
..      ``exit 1``) and set ``retries=1``.
..   2. Trigger a run and let the task fail (attempt 1 → version 1).
..   3. Before the retry fires, update the task's ``bash_command`` to ``exit 0`` (non-structural change)
..      so a new version is created.
..   4. Let the retry succeed (attempt 2 → version 2).
..   5. Open the task instance in the UI → Task instance details panel.
..   Expected: Try 1 shows version 1, try 2 shows version 2 in the details panel.
..   Capture: The task instance details panel with the try-number selector and Dag version field visible.


Scenario 4: Clearing and Rerunning Tasks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Clearing tasks for rerun has specific interactions with Dag versioning:

- **Clearing an entire Dag run:** The run reruns with the **latest** Dag version and structure. All task
  instances use the newest version when re-executed.
- **Clearing individual task instances:** The task reruns using the **latest** Dag version code.
- **Removed tasks:** If a task was removed in a newer Dag version, the clear button is disabled for that
  task in old runs. A tooltip explains that the task no longer exists in the current Dag version. You must
  re-add the task to the Dag before you can clear and rerun it.
- **Downstream dependencies:** When you clear a task, its downstream tasks are queued using the new Dag
  structure. If the dependency graph changed between versions, the downstream behavior reflects the latest
  version.

**Example:** Your Dag had tasks ``extract`` → ``transform`` → ``load``. You remove ``transform`` in a newer
version. When viewing old runs, you cannot clear ``transform`` --- the button is disabled with a tooltip
explaining that the task was removed. You can still clear ``extract`` or ``load`` from those old runs, and
they will rerun with the latest Dag code.

.. TODO: Add screenshot — Grid view showing a removed task with its clear button disabled and tooltip visible.
..
..   Setup to reproduce:
..   1. Create a Dag with tasks ``extract``, ``transform``, and ``load`` (version 1). Trigger a run and
..      let it complete successfully.
..   2. Remove ``transform`` from the Dag and let the scheduler parse it (version 2 is created).
..   3. Navigate to the completed run from step 1 in the grid view.
..   4. Click on the ``transform`` task cell to open the task instance panel.
..   Expected: The clear button is disabled; hovering shows a tooltip explaining the task no longer exists.
..   Capture: The task instance panel with the disabled clear button and tooltip visible.

.. warning::

    When clearing a task that has downstream dependencies, be aware that the downstream tasks will use the
    latest Dag structure. If the dependency graph has changed significantly, the rerun behavior may differ
    from the original run.


Viewing Dag Versions in the UI
------------------------------

Dag versioning information is surfaced throughout the Airflow UI:

**Graph view**

- Shows the Dag structure for the selected version
- Use the **Dag Version** dropdown (in the settings popover) to switch between versions and see how the
  Dag structure changed over time

.. image:: /img/ui-light/dag_version_graph.png
   :alt: Graph view showing version-specific Dag structure with the Dag Version dropdown visible (Light Mode)

**Code tab**

- Displays the source code for the selected Dag version
- Use the **Dag Version** dropdown to select any historical version and view its code
- Each version shows the timestamp of when it was created

**Details panels**

- **Dag details:** Shows the latest Dag version, including the version ID, bundle name, bundle version
  (with a link if available), and creation timestamp
- **Dag run details:** Shows all Dag versions used during the run. If the Dag changed mid-run, multiple
  versions are listed
- **Task instance details:** Shows the Dag version for that specific task instance, including bundle name
  and version

**Events tab**

- Shows an audit log of events related to the Dag, which can include version changes
- Filterable by Dag ID, run ID, task ID, and try number

.. seealso::

    See :doc:`/ui` for a full reference of the Airflow UI.


.. _bundle-version-tracking:

Bundle Version Tracking
-----------------------

Dag versions also record which :doc:`Dag bundle </administration-and-deployment/dag-bundles>` and bundle
version they came from. This is particularly useful when using version-controlled bundles:

- **GitDagBundle:** Records the git commit hash as the bundle version. This allows Airflow to check out
  and run tasks with the exact code from that commit, ensuring reproducibility even if the repository has
  moved forward.
- **LocalDagBundle:** Does not support bundle versioning. The bundle version is always ``None``, and tasks
  always run using the latest code on disk.
- **S3 and GCS bundles:** Do not support bundle versioning. Tasks always run using the latest code.

.. seealso::

    See :doc:`/administration-and-deployment/dag-bundles` for full documentation on configuring and using
    Dag bundles.


Key Behaviors Summary
---------------------

The following table summarizes how Airflow behaves in common Dag versioning scenarios:

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Action
     - Airflow Behavior
   * - Modify Dag structure
     - New Dag version created on next scheduler parse
   * - View an old Dag run
     - UI shows the Dag structure and code from that run's version
   * - Clear a task instance
     - Task reruns with the latest Dag version code
   * - Clear a removed task
     - Not possible --- clear button is disabled
   * - Dag changes mid-run
     - Mixed versions tracked per task instance
   * - Rerun a task after a Dag change (retry or clear)
     - Rerun uses the latest Dag version
   * - Clear an entire Dag run
     - All tasks rerun with the latest Dag version and structure

.. tip::

    You can use the **Events** tab in the Dag details view to track exactly when each version change
    occurred, which is helpful for correlating Dag changes with run outcomes.
