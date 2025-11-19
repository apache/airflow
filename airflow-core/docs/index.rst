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

What is Airflow®?
=========================================

`Apache Airflow® <https://github.com/apache/airflow>`_ is an open-source platform for developing, scheduling,
and monitoring batch-oriented workflows. Airflow's extensible Python framework enables you to build workflows
connecting with virtually any technology. A web-based UI helps you visualize, manage, and debug your workflows.
You can run Airflow in a variety of configurations — from a single process on your laptop to a distributed system
capable of handling massive workloads.

Workflows as code
=========================================
Airflow workflows are defined entirely in Python. This "workflows as code" approach brings several advantages:

- **Dynamic**: Pipelines are defined in code, enabling dynamic Dag generation and parameterization.
- **Extensible**: The Airflow framework includes a wide range of built-in operators and can be extended to fit your needs.
- **Flexible**: Airflow leverages the `Jinja <https://jinja.palletsprojects.com>`_ templating engine, allowing rich customizations.

.. _task-sdk-docs:

Task SDK
========

For Airflow Task SDK, see the standalone reference & tutorial site:

:doc:`task-sdk:index`

Dags
-----------------------------------------

.. include:: /../../devel-common/src/sphinx_exts/includes/dag-definition.rst
    :start-after: .. dag-definition-start
    :end-before: .. dag-definition-end

Let's look at a code snippet that defines a simple Dag:

.. code-block:: python

    from datetime import datetime

    from airflow.sdk import DAG, task
    from airflow.providers.standard.operators.bash import BashOperator

    # A Dag represents a workflow, a collection of tasks
    with DAG(dag_id="demo", start_date=datetime(2022, 1, 1), schedule="0 0 * * *") as dag:
        # Tasks are represented as operators
        hello = BashOperator(task_id="hello", bash_command="echo hello")

        @task()
        def airflow():
            print("airflow")

        # Set dependencies between tasks
        hello >> airflow()


Here you see:

- A Dag named ``"demo"``, scheduled to run daily starting on January 1st, 2022. A Dag is how Airflow represents a workflow.
- Two tasks: One using a ``BashOperator`` to run a shell script, and another using the ``@task`` decorator to define a Python function.
- The ``>>`` operator defines a dependency between the two tasks and controls execution order.

Airflow parses the script, schedules the tasks, and executes them in the defined order. The status of the ``"demo"`` Dag
is displayed in the web interface:

.. image:: /img/ui-light/demo_graph_and_code_view.png
    :alt: Demo Dag in the Graph View, showing the status of one Dag run along with Dag code.

|

This example uses a simple Bash command and Python function, but Airflow tasks can run virtually any code. You might use
tasks to run a Spark job, move files between storage buckets, or send a notification email. Here's what that same Dag looks
like over time, with multiple runs:

.. image:: /img/ui-light/demo_grid_view_with_task_logs.png
    :alt: Demo Dag in the Grid View, showing the status of all Dag runs, as well as logs for a task instance

|

Each column in the grid represents a single Dag run. While the graph and grid views are most commonly used, Airflow provides
several other views to help you monitor and troubleshoot workflows — such as the ``Dag Overview`` view:

.. image:: /img/ui-light/demo_complex_dag_overview_with_failed_tasks.png
    :alt: Overview of a complex Dag in the Grid View, showing the status of all Dag runs, as well as quick links to recently failed task logs

|

.. include:: /../../devel-common/src/sphinx_exts/includes/dag-definition.rst
    :start-after: .. dag-etymology-start
    :end-before: .. dag-etymology-end

Why Airflow®?
=========================================
Airflow is a platform for orchestrating batch workflows. It offers a flexible framework with a wide range of built-in operators
and makes it easy to integrate with new technologies.

If your workflows have a clear start and end and run on a schedule, they're a great fit for Airflow Dags.

If you prefer coding over clicking, Airflow is built for you. Defining workflows as Python code provides several key benefits:

- **Version control**: Track changes, roll back to previous versions, and collaborate with your team.
- **Team collaboration**: Multiple developers can work on the same workflow codebase.
- **Testing**: Validate pipeline logic through unit and integration tests.
- **Extensibility**: Customize workflows using a large ecosystem of existing components — or build your own.

Airflow's rich scheduling and execution semantics make it easy to define complex, recurring pipelines. From the web interface,
you can manually trigger Dags, inspect logs, and monitor task status. You can also backfill Dag runs to process historical
data, or rerun only failed tasks to minimize cost and time.

The Airflow platform is highly customizable. With the :doc:`public-airflow-interface` you can extend and adapt nearly
every part of the system — from operators to UI plugins to execution logic.

Because Airflow is open source, you're building on components developed, tested, and maintained by a global community.
You'll find a wealth of learning resources, including blog posts, books, and conference talks — and you can connect with
others via the `community <https://airflow.apache.org/community>`_, `Slack <https://s.apache.org/airflow-slack>`_, and mailing lists.

Why not Airflow®?
=================

Airflow® is designed for finite, batch-oriented workflows. While you can trigger Dags using the CLI or REST API, Airflow is not
intended for continuously running, event-driven, or streaming workloads. That said, Airflow often complements streaming systems like Apache Kafka.
Kafka handles real-time ingestion, writing data to storage. Airflow can then periodically pick up that data and process it in batch.

If you prefer clicking over coding, Airflow might not be the best fit. The web UI simplifies workflow management, and the developer
experience is continuously improving, but defining workflows as code is central to how Airflow works — so some coding is always required.


.. toctree::
    :hidden:
    :caption: Content

    Overview <self>
    start
    installation/index
    security/index
    tutorial/index
    howto/index
    ui
    core-concepts/index
    authoring-and-scheduling/index
    administration-and-deployment/index
    integration
    public-airflow-interface
    best-practices
    faq
    troubleshooting
    Release Policies <release-process>
    release_notes
    privacy_notice
    project
    license

.. toctree::
    :hidden:
    :caption: References

    Operators and hooks <operators-and-hooks-ref>
    CLI <cli-and-env-variables-ref>
    Templates <templates-ref>
    Airflow public API <stable-rest-api-ref>
    Configurations <configurations-ref>
    Extra packages <extra-packages-ref>

.. toctree::
    :hidden:
    :caption: Internal DB details

    Database Migrations <migrations-ref>
    Database ERD Schema <database-erd-ref>
