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

 ..   http://www.apache.org/licenses/LICENSE-2.0
 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.


.. NOTE TO CONTRIBUTORS:
   Please, only add notes to the Changelog just below the "Changelog" header when there are some breaking changes
   and you want to add an explanation to the users on how they are supposed to deal with them.
   The changelog is updated and maintained semi-automatically by release manager.

``apache-airflow-providers-standard``


Changelog
---------

0.0.3
.....

Breaking changes
~~~~~~~~~~~~~~~~

* ``The deprecated parameter use_dill was removed in PythonOperator and all virtualenv and branching derivates. Please use serializer='dill' instead.``
* ``The deprecated parameter use_dill was removed in all Python task decorators and virtualenv and branching derivates. Please use serializer='dill' instead.``

0.0.2
.....

Bug Fixes
~~~~~~~~~

* ``Fix TriggerDagRunOperator extra_link when trigger_dag_id is templated (#42810)``

Misc
~~~~

* ``Move 'TriggerDagRunOperator' to standard provider (#44053)``
* ``Move filesystem sensor to standard provider (#43890)``
* ``Rename execution_date to logical_date across codebase (#43902)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use Python 3.9 as target version for Ruff & Black rules (#44298)``
   * ``update standard provider CHANGELOG.rst (#44110)``

0.0.1
.....

.. note::
   This provider created by migrating operators/sensors/hooks from Airflow 2 core.

Breaking changes
~~~~~~~~~~~~~~~~

* ``In BranchDayOfWeekOperator, DayOfWeekSensor, BranchDateTimeOperator parameter use_task_execution_date has been removed. Please use use_task_logical_date.``
* ``PythonVirtualenvOperator uses built-in venv instead of virtualenv package.``
* ``is_venv_installed method has been removed from PythonVirtualenvOperator as venv is built-in.``

* ``Initial version of the provider. (#41564)``
