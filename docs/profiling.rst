..  Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

..  Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

.. TODO: This section would be removed after we migrate to www_rbac completely.

Data Profiling
==============

.. note::
   ``Adhoc Queries`` and ``Charts`` are no longer supported in the new FAB-based webserver
   and UI, due to security concerns.

Part of being productive with data is having the right weapons to
profile the data you are working with. Airflow provides a simple query
interface to write SQL and get results quickly, and a charting application
letting you visualize data.

Adhoc Queries
-------------
The adhoc query UI allows for simple SQL interactions with the database
connections registered in Airflow.

.. image:: img/adhoc.png

Charts
------
A simple UI built on top of flask-admin and highcharts allows building
data visualizations and charts easily. Fill in a form with a label, SQL,
chart type, pick a source database from your environment's connections,
select a few other options, and save it for later use.

You can even use the same templating and macros available when writing
airflow pipelines, parameterizing your queries and modifying parameters
directly in the URL.

These charts are basic, but they're easy to create, modify and share.

Chart Screenshot
................

.. image:: img/chart.png

-----

Chart Form Screenshot
.....................

.. image:: img/chart_form.png
