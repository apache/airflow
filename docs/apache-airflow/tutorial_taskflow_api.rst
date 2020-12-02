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




Tutorial on the Taskflow API
============================

This tutorial builds on the regular Airflow Tutorial and focuses specifically
on writing data pipelines using the Taskflow API paradigm which is introduced as
part of Airflow 2.0 and contrasts this with DAGs written using the traditional paradigm.

The data pipeline chosen here is a simple ETL pattern with
three separate tasks for Extract, Transform, and Load.

Example "Taskflow API" ETL Pipeline
-----------------------------------

Here is very simple ETL pipeline using the Taskflow API paradigm. A more detailed
explanation is given below.

.. exampleinclude:: /../../airflow/example_dags/tutorial_taskflow_api_etl.py
    :language: python
    :start-after: [START tutorial]
    :end-before: [END tutorial]

It's a DAG definition file
--------------------------

If this is the first DAG file you are looking at, please note that this Python script
is interpreted by Airflow and is a configuration file for your data pipeline.
For a complete introduction to DAG files, please look at the core :doc:`Airflow tutorial<tutorial>`
which covers DAG structure and definitions extensively.


Instantiate a DAG
-----------------

We are creating a DAG which is the collection of our tasks with dependencies between
the tasks. This is a very simple definition, since we just want the DAG to be run
when we set this up with Airflow, without any retries or complex scheduling.

.. exampleinclude:: /../../airflow/example_dags/tutorial_taskflow_api_etl.py
    :language: python
    :start-after: [START instantiate_dag]
    :end-before: [END instantiate_dag]

Tasks
-----
In this data pipeline, tasks are created based on Python functions using the decorator
as shown below. The function name acts as a unique identifier for the task.

.. exampleinclude:: /../../airflow/example_dags/tutorial_taskflow_api_etl.py
    :language: python
    :start-after: [START extract]
    :end-before: [END extract]

The returned value, which in this case is a dictionary, will be made available for use in later tasks.

The Transform and Load tasks are created in the same manner as the Extract task shown above.

Main flow of the DAG
--------------------
Now that we have the Extract, Transform, and Load tasks defined based on the Python functions,
we can move to the main part of the DAG.

.. exampleinclude:: /../../airflow/example_dags/tutorial_taskflow_api_etl.py
    :language: python
    :start-after: [START main_flow]
    :end-before: [END main_flow]

That's it, we are done!
We have invoked the Extract task, obtained the order data from there and sent it over to
the Transform task for summarization, and then invoked the Load task with the summarized data.
The dependencies between the tasks and the passing of data between these tasks which could be
running on different workers on different nodes on the network is all handled by Airflow.

But how?
--------
For experienced Airflow DAG authors, this is startlingly simple! Let's contrast this with
how this DAG had to be written before Airflow 2.0 below:

.. exampleinclude:: /../../airflow/example_dags/tutorial_etl_dag.py
    :language: python
    :start-after: [START tutorial]
    :end-before: [END tutorial]

All of the processing shown above is being done in the new Airflow 2.0 dag as well, but
it is all abstracted from the DAG developer.

Let's examine this in detail by looking at the Transform task in isolation since it is
in the middle of the data pipeline. In Airflow 1.x, this task is defined as shown below:

.. exampleinclude:: /../../airflow/example_dags/tutorial_etl_dag.py
    :language: python
    :start-after: [START transform_function]
    :end-before: [END transform_function]

As we see here, the data being processed in the Transform function is passed to it using Xcom
variables. In turn, the summarized data from the Transform function is also placed
into another Xcom variable which will then be used by the Load task.

Contrasting that with Taskflow API in Airflow 2.0 as shown below.

.. exampleinclude:: /../../airflow/example_dags/tutorial_taskflow_api_etl.py
    :language: python
    :start-after: [START transform]
    :end-before: [END transform]

All of the Xcom usage for data passing between these tasks is abstracted away from the DAG author
in Airflow 2.0. However, Xcom variables are used behind the scenes and can be viewed using
the Airflow UI as necessary for debugging or DAG monitoring.

Similarly, task dependencies are automatically generated within TaskFlows based on the
functional invocation of tasks. In Airflow 1.x, tasks had to be explicitly created and
dependencies specified as shown below.

.. exampleinclude:: /../../airflow/example_dags/tutorial_etl_dag.py
    :language: python
    :start-after: [START main_flow]
    :end-before: [END main_flow]

In contrast, with the Taskflow API in Airflow 2.0, the invocation itself automatically generates
the dependencies as shown below.

.. exampleinclude:: /../../airflow/example_dags/tutorial_taskflow_api_etl.py
    :language: python
    :start-after: [START main_flow]
    :end-before: [END main_flow]


What's Next?
------------

You have seen how simple it is to write DAGs using the Taskflow API paradigm within Airflow 2.0. Please do
read the :ref:`Concepts page<concepts>` for detailed explanation of Airflow concepts such as DAGs, Tasks,
Operators, etc, and the :ref:`concepts:task_decorator` in particular.

More details about the Taskflow API, can be found as part of the Airflow Improvement Proposal
`AIP-31: "Taskflow API" for clearer/simpler DAG definition <https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=148638736>`__
and specifically within the Concepts guide at :ref:`Concepts - Taskflow API<concepts:task_flow_api>`.
