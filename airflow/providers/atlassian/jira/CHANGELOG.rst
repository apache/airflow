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


.. NOTE TO CONTRIBUTORS:
   Please, only add notes to the Changelog just below the "Changelog" header when there are some breaking changes
   and you want to add an explanation to the users on how they are supposed to deal with them.
   The changelog is updated and maintained semi-automatically by release manager.

Changelog
---------

2.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* ``Changing atlassian JIRA SDK to official atlassian-python-api SDK (#27633)``

Migrated ``Jira`` provider from Atlassian ``Jira`` SDK to ``atlassian-python-api`` SDK.
``Jira`` provider doesn't support ``validate`` and ``get_server_info`` in connection extra dict.
Changed the return type of ``JiraHook.get_conn`` to return an ``atlassian.Jira`` object instead of a ``jira.Jira`` object.

.. warning:: Due to the underlying SDK change, the ``JiraOperator`` now requires ``jira_method`` and ``jira_method_args``
             arguments as per ``atlassian-python-api``.

             Please refer `Atlassian Python API Documentation <https://atlassian-python-api.readthedocs.io/jira.html>`__

1.1.0
.....

This release of provider is only available for Airflow 2.3+ as explained in the
`Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/README.md#support-for-providers>`_.

Misc
~~~~

* ``Move min airflow version to 2.3.0 for all providers (#27196)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Enable string normalization in python formatting - providers (#27205)``

1.0.0
.....

Initial version of the provider.
