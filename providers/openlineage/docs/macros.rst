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

.. _howto/macros:openlineage:

OpenLineage Job Hierarchy & Macros
===================================

Macros included in OpenLineage plugin get integrated to Airflow's main collections and become available for use.

Job Hierarchy in OpenLineage
-----------------------------

When you need to establish relationships between different jobs (e.g., between DAGs, or between Airflow tasks and external systems),
you may need to explicitly pass parent job information. The following sections describe different scenarios and whether user action is required.

DAG to Task Hierarchy
^^^^^^^^^^^^^^^^^^^^^^

Apache Airflow features an inherent job hierarchy: DAGs, large and independently schedulable units, comprise smaller,
executable tasks. OpenLineage reflects this structure in its Job Hierarchy model. Within a single DAG, OpenLineage
automatically tracks the hierarchy between the DAG and its tasks - TaskInstance events automatically include
a ``ParentRunFacet`` that references the originating DAG run as parent job.

**User Action Required:** None. OpenLineage automatically establishes the parent-child relationship between DAG runs and their task instances.


TriggerDagRunOperator
^^^^^^^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.standard.operators.trigger_dagrun.TriggerDagRunOperator` triggers a DAG run for a specified Dag ID.

**OpenLineage Behavior:**

Since ``apache-airflow-providers-standard==1.10.0``, by default, the operator automatically injects OpenLineage parent
job information into the triggered DAG run's configuration. This creates a parent-child relationship between the
triggering task and the triggered DAG run - the triggered DAG Run events will have a ``ParentRunFacet`` referencing the
triggering task.

Apart from the above, OpenLineage COMPLETE event for the triggering task include the following operator-specific
attributes in the ``AirflowRunFacet``:

- ``trigger_dag_id`` - The Dag ID of the DAG being triggered
- ``trigger_run_id`` - The Dag Run ID of the DagRun being triggered

**User Action Required:** None. The operator automatically handles parent information injection.

**To disable automatic injection** pass ``openlineage_inject_parent_info=False``:

.. code-block:: python

    TriggerDagRunOperator(
        task_id="trigger_downstream",
        trigger_dag_id="downstream_dag",
        openlineage_inject_parent_info=False,  # Disable automatic injection
    )


Triggering DAGs via API
^^^^^^^^^^^^^^^^^^^^^^^^

When triggering a DAG run via the Airflow REST API, you can manually pass parent and root job information through the
DAG run's ``conf`` parameter. When a DAG run configuration includes an ``openlineage`` section with valid metadata,
this information is automatically parsed and converted into the ``parentRunFacet`` in DAG run's events, from which the
root information is also propagated to all tasks within that DAG run.

If no DAG run ``openlineage`` configuration is provided, the DAG run will not contain a ``parentRunFacet``,
and the root of all tasks will default to the DAG run itself.

The ``openlineage`` dictionary in the DAG run configuration should contain the following keys:

**Parent job information** (all three values must be included to create a parent reference):

- **parentRunId** — the unique run ID (UUID) of the direct parent job
- **parentJobName** — the name of the parent job
- **parentJobNamespace** — the namespace of the parent job

**Root job information** (all three values must be included to create a root reference; otherwise, parent will be used as root):

- **rootParentRunId** — the run ID (UUID) of the top-level (root) job
- **rootParentJobName** — the name of the top-level (root) job
- **rootParentJobNamespace** — the namespace of the top-level (root) job

.. note::

    We highly recommend providing all six OpenLineage identifiers (parent and root) to ensure complete lineage tracking.
    If the root information is missing, the parent set will be used as the root. If any of the three parent fields are missing,
    no parent facet will be created. Partial or mixed configurations are not supported—either all three parent values or all three
    root values must be provided together.

Example:

.. code-block:: shell

    curl -X POST "http://<AIRFLOW_HOST>/api/v2/dags/my_dag_name/dagRuns" \
    -H "Content-Type: application/json" \
    -d '{
      "logical_date": "2019-08-24T14:15:22Z",
      "conf": {
        "openlineage": {
          "parentRunId": "3bb703d1-09c1-4a42-8da5-35a0b3216072",
          "parentJobNamespace": "prod_biz",
          "parentJobName": "get_files",
          "rootParentRunId": "9d3b14f7-de91-40b6-aeef-e887e2c7673e",
          "rootParentJobNamespace": "prod_analytics",
          "rootParentJobName": "generate_report_sales_e2e"
        }
      }
    }'

**User Action Required:** Yes - you must manually include the parent and root job information in the DAG run ``conf``.

ExternalTaskSensor
^^^^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.standard.sensors.external_task.ExternalTaskSensor` waits for a task(s) in another DAG.

**OpenLineage Behavior:**

OpenLineage events for the sensor task include the following operator-specific attributes in the ``AirflowRunFacet``:

- ``external_dag_id`` - The DAG ID of the external task being waited for
- ``external_task_id`` - The task ID of the external task being waited for (when waiting for a single task)
- ``external_task_ids`` - List of task IDs being waited for (when waiting for multiple tasks)
- ``external_task_group_id`` - The task group ID being waited for (when waiting for a task group)
- ``external_dates_filter`` - The date filter applied when checking for external task completion

These attributes provide visibility into cross-DAG dependencies but do not create a parent-child job relationship

**User Action Required:** No automatic parent relationship is created. If you need to track this relationship in
OpenLineage, consider using TriggerDagRunOperator, manually passing parent information via API or using the existing
attributes to create it manually.

Airflow Assets
^^^^^^^^^^^^^^^

Airflow Assets allow you to schedule DAGs based on when tasks update assets (data dependencies). When a task updates
an asset and another DAG is scheduled based on that asset, OpenLineage tracks the asset relationship.

**OpenLineage Behavior:**

Tasks that produce assets (using ``outlets=[Asset(...)]``) and DAGs scheduled based on assets
(using ``schedule=[Asset(...)]``) are tracked by OpenLineage as consuming those assets.

When a DAG run is triggered by asset consumption, OpenLineage adds a ``JobDependenciesRunFacet`` to the DAG run's events
(START and COMPLETE/FAIL). This facet contains upstream job dependencies showing all consumed asset events and OpenLineage
job/run information of the asset-producing jobs. Each dependency includes:

- Job identifier (OpenLineage namespace and name) of the producing task
- Run identifier (OpenLineage run ID) of the producing task instance, if available
- Dependency type: ``IMPLICIT_ASSET_DEPENDENCY``
- Asset events information: details about all asset events consumed from that job, including asset URI, asset ID,
  source DAG run ID, and other metadata

Note that a ``ParentRunFacet`` is **not** added to consuming DAG run events. Instead, the ``JobDependenciesRunFacet``
provides a more flexible representation that can handle multiple upstream dependencies (when a DAG consumes assets from
multiple producing tasks) and preserves detailed information about each asset event.

The asset relationship creates a data lineage connection in OpenLineage, showing which tasks produce and consume assets.

.. code-block:: json

    "run": {
      "facets": {
        "jobDependencies": {
          "upstream": [
            {
              "job": {
                "name": "dag_asset_1_producer.produce_dataset_1",
                "namespace": "airflow"
              },
              "run": {
                "runId": "019b6ff1-f2f0-79bf-a797-0bbe6983c753"
              },
              "airflow": {
                "asset_events": [
                  {
                    "asset_id": 1,
                    "asset_uri": "s3://first-bucket/ds1.csv",
                    "dag_run_id": "manual__2025-12-30T15:48:06+00:00",
                    "asset_event_id": 1
                  }
                ]
              },
              "dependency_type": "IMPLICIT_ASSET_DEPENDENCY"
            },
            {
              "job": {
                "name": "dag_asset_1_producer.produce_dataset_1",
                "namespace": "airflow"
              },
              "run": {
                "runId": "019b6ff4-4c80-7b5f-9f35-da28a44030df"
              },
              "airflow": {
                "asset_events": [
                  {
                    "asset_id": 1,
                    "asset_uri": "s3://first-bucket/ds1.csv",
                    "dag_run_id": "manual__2025-12-30T15:50:40+00:00",
                    "asset_event_id": 2
                  }
                ]
              },
              "dependency_type": "IMPLICIT_ASSET_DEPENDENCY"
            },
            {
              "job": {
                "name": "dag_asset_2_producer.produce_dataset_2",
                "namespace": "airflow"
              },
              "run": {
                "runId": "019b6ff4-7f48-7ee5-aacb-a88072516b1e"
              },
              "airflow": {
                "asset_events": [
                  {
                    "asset_id": 2,
                    "asset_uri": "gs://second-bucket/ds2.xlsx",
                    "dag_run_id": "manual__2025-12-30T15:50:53+00:00",
                    "asset_event_id": 3
                  }
                ]
              },
              "dependency_type": "IMPLICIT_ASSET_DEPENDENCY"
            }
          ],
          "downstream": []
        }
      }
    }


**User Action Required:** None. Relationships are automatically tracked for data lineage.


Manually Emitting Asset Events via API
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When you manually emit asset events via the Airflow REST API (e.g., when assets are updated outside of Airflow tasks),
you can include OpenLineage job information in the asset event's ``extra`` field. This allows OpenLineage to
track the relationship between the asset producer and consumers, even when the asset event is not directly linked to
an Airflow TaskInstance.

**OpenLineage Behavior:**

When an asset event is manually created via API without a TaskInstance reference,
OpenLineage checks (after TaskInstance and AssetEvent source fields) for parent job information in
``asset_event.extra["openlineage"]``. If present, this information is used to create job dependencies in the
``JobDependenciesRunFacet`` in events of DAG runs that consume the asset

**Required fields in ``asset_event.extra["openlineage"]``:**

- **parentJobName** (required) - The name of the parent job that produced the asset
- **parentJobNamespace** (required) - The namespace of the parent job
- **parentRunId** (optional) - The run ID (UUID) of the parent job execution. If provided, must be a valid UUID format

**Example API call:**

.. code-block:: shell

    curl -X POST "http://<AIRFLOW_HOST>/api/v2/assets/events" \
    -H "Content-Type: application/json" \
    -d '{
      "asset_id": 3,
      "extra": {
        "openlineage": {
          "parentJobName": "external_system.data_processor",
          "parentJobNamespace": "prod_etl",
          "parentRunId": "3bb703d1-09c1-4a42-8da5-35a0b3216072"
        }
      }
    }'

**User Action Required:** Yes - you must manually include the OpenLineage job information in the asset event's
``extra`` field when emitting events via API.

Child Jobs Outside Airflow
^^^^^^^^^^^^^^^^^^^^^^^^^^^

When Airflow tasks trigger external systems (e.g., Spark applications, external APIs, other schedulers), those child
jobs need to be explicitly configured with parent job information to establish the hierarchy in OpenLineage.

**User Action Required:** Yes - you must use macros (see below) or automatic injection mechanisms (e.g., for Spark)
to pass parent job information to the child job.


Preserving Job Hierarchy with Macros
--------------------------------------

To establish a correct job hierarchy in lineage tracking, child jobs (e.g., Spark applications, external systems, or downstream DAGs)
need to know about their parent job (the Airflow task that triggered them). This allows the child job's OpenLineage integration to
automatically add a ``ParentRunFacet`` to its OpenLineage events, linking the child job to its originating Airflow job in the lineage graph.

The macros provided by the OpenLineage provider allow you to pass this parent job information from Airflow tasks to child jobs.
The ``lineage_*`` macros describe the Airflow task itself, which from the child job's perspective is the parent.
The ``lineage_root_*`` macros forward the Airflow task's root information into the child job, allowing the child job to maintain
the complete job hierarchy and information about the root of the job hierarchy.

They can be invoked as a Jinja template, e.g.

Lineage job & run macros
^^^^^^^^^^^^^^^^^^^^^^^^^

These macros:

  * ``lineage_job_namespace()`` - Returns OpenLineage namespace for a given task_instance
  * ``lineage_job_name(task_instance)`` - Returns OpenLineage job name for a given task_instance
  * ``lineage_run_id(task_instance)`` - Returns the generated OpenLineage run id for a given task_instance

describe the Airflow task and should be used as **parent** information when configuring child jobs. From the child job's perspective,
the Airflow task is the parent.

**Example: Using macros with Spark applications**

When triggering Spark jobs from Airflow, you can pass parent job information using these macros:

.. code-block:: python

    SparkSubmitOperator(
        task_id="my_task",
        application="/script.py",
        conf={
            "spark.openlineage.parentJobNamespace": "{{ macros.OpenLineageProviderPlugin.lineage_job_namespace() }}",
            "spark.openlineage.parentJobName": "{{ macros.OpenLineageProviderPlugin.lineage_job_name(task_instance) }}",
            "spark.openlineage.parentRunId": "{{ macros.OpenLineageProviderPlugin.lineage_run_id(task_instance) }}",
        },
    )

**Example: Using macros with other child jobs**

These macros work with any child job that accepts parent job information. For example, you might pass this information
to external systems, downstream DAGs, or other processing frameworks:

.. code-block:: python

    PythonOperator(
        task_id="trigger_external_job",
        python_callable=call_external_api,
        op_kwargs={
            "parent_job_namespace": "{{ macros.OpenLineageProviderPlugin.lineage_job_namespace() }}",
            "parent_job_name": "{{ macros.OpenLineageProviderPlugin.lineage_job_name(task_instance) }}",
            "parent_run_id": "{{ macros.OpenLineageProviderPlugin.lineage_run_id(task_instance) }}",
        },
    )


Lineage root macros
^^^^^^^^^^^^^^^^^^^^

These macros:

  * ``lineage_root_job_namespace(task_instance)`` - Returns OpenLineage namespace of root job of a given task_instance
  * ``lineage_root_job_name(task_instance)`` - Returns OpenLineage job name of root job of a given task_instance
  * ``lineage_root_run_id(task_instance)`` - Returns OpenLineage run ID of root run of a given task_instance

forward the Airflow task's root information into the child job and should be used as **root** information when configuring child jobs.
This allows the child job to maintain the complete job hierarchy, especially in scenarios where tasks are executed as part of a larger workflow.

**Example: Using root macros with Spark applications**

.. code-block:: python

    SparkSubmitOperator(
        task_id="my_task",
        application="/script.py",
        conf={
            "spark.openlineage.rootJobNamespace": "{{ macros.OpenLineageProviderPlugin.lineage_root_job_namespace(task_instance) }}",
            "spark.openlineage.rootJobName": "{{ macros.OpenLineageProviderPlugin.lineage_root_job_name(task_instance) }}",
            "spark.openlineage.rootRunId": "{{ macros.OpenLineageProviderPlugin.lineage_root_run_id(task_instance) }}",
        },
    )

Joined identifiers
^^^^^^^^^^^^^^^^^^^

Instead of passing separate components, you can use combined macros that return all information in a single string.
These macros are useful when you need to pass the complete identifier to a child job in one parameter.

The ``lineage_parent_id(task_instance)`` macro combines the parent information (namespace, job name, and run id)
into one string structured as ``{namespace}/{job_name}/{run_id}``. This represents the Airflow task and should be used
as parent information when configuring child jobs.

Similarly, the ``lineage_root_parent_id(task_instance)`` macro combines the root information (root namespace, root job name,
and root run id) into one string structured as ``{namespace}/{job_name}/{run_id}``. This forwards the Airflow task's root
information and should be used as root information when configuring child jobs.

.. code-block:: python

    def my_task_function(templates_dict, **kwargs):
        parent_job_namespace, parent_job_name, parent_run_id = templates_dict["parentRun"].split("/")
        root_job_namespace, root_job_name, root_run_id = templates_dict["rootRun"].split("/")
        ...


    PythonOperator(
        task_id="render_template",
        python_callable=my_task_function,
        templates_dict={
            # Parent information as one string `<namespace>/<name>/<run_id>`
            "parentRun": "{{ macros.OpenLineageProviderPlugin.lineage_parent_id(task_instance) }}",
            # Root information as one string `<namespace>/<name>/<run_id>`
            "rootRun": "{{ macros.OpenLineageProviderPlugin.lineage_root_parent_id(task_instance) }}",
        },
        provide_context=False,
        dag=dag,
    )


Example
^^^^^^^^

When you need to pass both parent and root lineage information to a child job, you can combine all macros
in a single operator configuration. This example shows how to use both parent and root macros with a Spark application:

.. code-block:: python

    SparkSubmitOperator(
        task_id="process_data",
        application="/path/to/spark/app.py",
        conf={
            # Parent lineage information
            "spark.openlineage.parentJobNamespace": "{{ macros.OpenLineageProviderPlugin.lineage_job_namespace() }}",
            "spark.openlineage.parentJobName": "{{ macros.OpenLineageProviderPlugin.lineage_job_name(task_instance) }}",
            "spark.openlineage.parentRunId": "{{ macros.OpenLineageProviderPlugin.lineage_run_id(task_instance) }}",
            # Root lineage information
            "spark.openlineage.rootJobNamespace": "{{ macros.OpenLineageProviderPlugin.lineage_root_job_namespace(task_instance) }}",
            "spark.openlineage.rootJobName": "{{ macros.OpenLineageProviderPlugin.lineage_root_job_name(task_instance) }}",
            "spark.openlineage.rootRunId": "{{ macros.OpenLineageProviderPlugin.lineage_root_run_id(task_instance) }}",
        },
    )

For more Spark-specific examples and automatic injection options, see :ref:`spark:openlineage`.
