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



.. _howto/operator:TableauOperator:

TableauOperator
=============

Use the :class:`~airflow.providers.tableau.operators.TableauOperator` to execute
Tableau server client python commands in a `Tableau <https://tableau.github.io/server-client-python/docs/api-ref>`.


Using the Operator
^^^^^^^^^^^^^^^^^^

| **resource**: The name of the resource to use. **str**
| **method**: The name of the resource's method to execute. **str**  
| **find**: The reference of resource wich will recive the action. **str**  
| **match_with**: The resource field name to be matched with find parameter. **str** - Default: **id**  
| **site_id**: The id of the site where the workbook belongs to. **str** - Default: **None**  
| **blocking_refresh**: By default the extract refresh will be blocking means it will wait until it has finished. **bool** - Default: **True**  
| **tableau_conn_id**: The credentials to authenticate to the Tableau Server. **str** - Default: **tableau_default**  
|
|



.. list-table:: Available methods by resource
   :widths: 15 15 15 15 15
   :header-rows: 1

   * - Resource
     - delete
     - refresh
     - remove
     - run
   * - datasources
     - X
     - X
     - 
     -
   * - groups
     - X
     - 
     - 
     -
   * - projects
     - X
     - 
     - 
     -
   * - schedule
     - X
     - 
     - 
     -
   * - sites
     - X
     - 
     - 
     -
   * - tasks
     - X
     - 
     - 
     - X
   * - users
     - 
     - 
     - X
     -
   * - workbooks
     - X
     - X
     - 
     -


An example usage of the TableauOperator is as follows:

.. exampleinclude:: /../../airflow/providers/tableau/example_dags/example_tableau.py
    :language: python
    :start-after: [START howto_operator_tableau]
    :end-before: [END howto_operator_tableau]
