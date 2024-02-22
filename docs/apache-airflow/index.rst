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

What is Airflow™?
=========================================

`Apache Airflow™ <https://github.com/apache/airflow>`_ is an open-source platform for developing, scheduling,
and monitoring distributable, batch-oriented workflows. Airflow's extensible Python framework enables you to build workflows
connecting with virtually any technology. A web interface helps manage the state of your workflows. Airflow is
deployable in many ways, varying from a single process on your laptop to a distributed setup across a network of many
machines to support even the largest of workflows.

Workflows as code
=========================================
The main characteristic of Airflow workflows is that all workflows are defined in Python code as a
`Directed Acyclic Graph <https://en.wikipedia.org/wiki/Directed_acyclic_graph>`__.

"Workflows as code" serves several purposes:

- **Dynamic**: Airflow pipelines are configured as Python code, allowing for dynamic pipeline generation.
- **Extensible**: The Airflow™ framework contains operators to connect with numerous technologies. All Airflow components are extensible to easily adjust to your environment.
- **Flexible**: Workflows can be easily parameterized by using the built-in `Jinja <https://jinja.palletsprojects.com>`_ templating engine.

Take a look at the following snippet of code:

.. code-block:: python

  # Create tasks using a decorated DAG and pass values between those tasks.
  from datetime import datetime
  from airflow.decorators import task, dag

  @dag(dag_id="demo", start_date=datetime(2024, 2, 21))                           # Decorate the following function to make a dag.
  def define_dag():

    @task
    def add_one(x):                                                               # Add one
      print(f"AAAAAAAAAAAA A2 add one to: {x}")                                   # Look in the airflow/logs folder to see the results
      return x + 1

    @task
    def sum_it(values):                                                           # Sum the values
      return sum(values)

    @task
    def print_it(ban):                                                            # Print the sum
      print(f"BBBBBBBBBBBBBB2 total is:  {ban}")                                  # Look in the airflow/logs folder to see the results

    @task
    def say_it_twice(ban):                                                        # Print the sum again
      print(f"CCCCCCCCCCCCCC2 total is:  {ban} {ban}")                            # Look in the airflow/logs folder to see the results

    added_values = add_one.expand(x=[1, 2, 3, 4, 5, 6])                           # These get executed in parallel
    summed       = sum_it        (added_values)                                   # The results of each return in add_one are returned as array via xcom inter task communication
    print_it                     (summed)                                         # Print the result in the airflow logs/folder
    say_it_twice                 (summed)                                         # Print the result in the airflow logs/folder

  define_dag()                                                                    # Call the decorated routine to define the dag


Here you see:

- A `DAG <https://en.wikipedia.org/wiki/Directed_acyclic_graph>`__ named "demo" created via the ``@dag`` decorator, starting on Feb 21st 2024 and running once a day.
- Four task descriptions created via ``@task`` decorators that between them take an array of numbers, add them up and print their sum.
- The execution order dependencies between the tasks can be explicitly stated with ``>>`` or inferred by Airflow from their parameters.

Airflow evaluates this script and executes the tasks at the set interval and in the defined order. The status
of the "demo" DAG is visible in the web interface:

.. image:: /img/demo_graph_view.png
  :alt: Demo DAG in the Graph View, showing the status of one DAG run

This example demonstrates a simple Python script, but these tasks can run any arbitrary code. Think
of running a Spark job, moving data between two buckets, or sending an email. The same structure can also be
seen running over time:

.. image:: /img/demo_grid_view.png
  :alt: Demo DAG in the Grid View, showing the status of all DAG runs

Each column represents one DAG run. These are two of the most used views in Airflow, but there are several
other views which allow you to deep dive into the state of your workflows.


Why Airflow™?
=========================================
Airflow™ is a batch workflow orchestration platform. The Airflow framework contains operators to connect with
many technologies and is easily extensible to connect with a new technology. If your workflows have a clear
start and end, and run at regular intervals, they can be programmed as an Airflow DAG.

If you prefer coding over clicking, Airflow is the tool for you. Workflows are defined as Python code which
means:

- Workflows can be stored in version control so that you can roll back to previous versions
- Workflows can be developed by multiple people simultaneously
- Tests can be written to validate functionality
- Components are extensible and you can build on a wide collection of existing components

Rich scheduling and execution semantics enable you to easily define complex pipelines, running at regular
intervals. Backfilling allows you to (re-)run pipelines on historical data after making changes to your logic.
And the ability to rerun partial pipelines after resolving an error helps maximize efficiency.

Airflow's user interface provides:

  1. In-depth views of two things:

    i. Pipelines
    ii. Tasks

  2. Overview of your pipelines over time

From the interface, you can inspect logs and manage tasks, for example retrying a task in
case of failure.

The open-source nature of Airflow ensures you work on components developed, tested, and used by many other
`companies <https://github.com/apache/airflow/blob/main/INTHEWILD.md>`_ around the world. In the active
`community <https://airflow.apache.org/community>`_ you can find plenty of helpful resources in the form of
blog posts, articles, conferences, books, and more. You can connect with other peers via several channels
such as `Slack <https://s.apache.org/airflow-slack>`_ and mailing lists.

Airflow as a Platform is highly customizable. By utilizing :doc:`public-airflow-interface` you can extend
and customize almost every aspect of Airflow.

Why not Airflow™?
=================

Airflow™ was built for finite batch workflows. While the CLI and REST API do allow triggering workflows,
Airflow was not built for infinitely running event-based workflows. Airflow is not a streaming solution.
However, a streaming system such as Apache Kafka is often seen working together with Apache Airflow. Kafka can
be used for ingestion and processing in real-time, event data is written to a storage location, and Airflow
periodically starts a workflow processing a batch of data.

If you prefer clicking over coding, Airflow is probably not the right solution. The web interface aims to make
managing workflows as easy as possible and the Airflow framework is continuously improved to make the
developer experience as smooth as possible. However, the philosophy of Airflow is to define workflows as code
so coding will always be required.


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
    Stable REST API <stable-rest-api-ref>
    deprecated-rest-api-ref
    Configurations <configurations-ref>
    Extra packages <extra-packages-ref>

.. toctree::
    :hidden:
    :caption: Internal DB details

    Database Migrations <migrations-ref>
    Database ERD Schema <database-erd-ref>
