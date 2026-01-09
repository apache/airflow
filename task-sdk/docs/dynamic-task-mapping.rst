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

.. _sdk-dynamic-task-mapping:

Dynamic Task Mapping with Task SDK
==================================

Dynamic Task Mapping allows tasks defined with the Task SDK to generate
a variable number of task instances at runtime based on upstream data.
This is enabled via the ``expand()`` method on tasks, providing a way
to parallelize execution without knowing the number of tasks ahead of time.

Simple Mapping
--------------

Map over a Python list directly in the DAG:

.. code-block:: python

   from datetime import datetime

   from airflow.sdk import DAG, task


   @task
   def add_one(x: int):
       return x + 1


   @task
   def sum_it(values: list[int]):
       print(f"Total was {sum(values)}")


   with DAG(dag_id="dynamic-map-simple", start_date=datetime(2022, 1, 1)) as dag:
       summed = sum_it(values=add_one.expand(x=[1, 2, 3, 4, 5]))

Task-Generated Mapping
----------------------

Generate the list at runtime from an upstream task:

.. code-block:: python

   @task
   def make_list():
       # This could fetch data from an API, database, etc.
       return ["a", "b", "c"]


   @task
   def consume(item: str):
       print(item)


   with DAG(dag_id="dynamic-map-generated", start_date=datetime(2022, 1, 1)) as dag:
       consume.expand(item=make_list())

Details
-----------

- Only keyword arguments can be passed to ``expand()``.
- Mapped inputs are provided to tasks as lazy proxy objects. To force
  evaluation into a concrete list, wrap the proxy in ``list()``.
- Combine static parameters with mapped ones using ``partial()``:

  .. code-block:: python

     @task
     def add(x: int, y: int):
         return x + y


     with DAG(dag_id="map-with-partial", start_date=datetime(2022, 1, 1)) as dag:
         add.partial(y=10).expand(x=[1, 2, 3])

Advanced Usage
--------------

For advanced patterns—such as repeated mapping, cross-product mapping,
named mappings (via ``map_index_template``), and handling large
datasets—see the Airflow Core documentation:

`Dynamic Task Mapping in the Airflow Core docs <https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html>`_.
