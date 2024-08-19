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

``apache-airflow-providers-pinecone``

Changelog
---------

2.1.0
.....

.. note::
  This release of provider is only available for Airflow 2.8+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.8.0 (#41396)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

2.0.1
.....

Misc
~~~~

* ``implement per-provider tests with lowest-direct dependency resolution (#39946)``

2.0.0
.....

.. note::
  This release of provider is only available for Airflow 2.7+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
   This release of provider has breaking changes from previous versions. Changes are based on
   the migration guide from pinecone - <https://canyon-quilt-082.notion.site/Pinecone-Python-SDK-v3-0-0-Migration-Guide-056d3897d7634bf7be399676a4757c7b>

* ``log_level`` field is removed from the Connections as it is not used by the provider anymore.
* ``PineconeHook.get_conn`` is removed in favor of ``conn`` property which returns the Connection object. Use ``pinecone_client`` property to access the Pinecone client.
*  Following ``PineconeHook`` methods are converted from static methods to instance methods. Hence, Initialization is required to use these now:

   + ``PineconeHook.list_indexes``
   + ``PineconeHook.upsert``
   + ``PineconeHook.create_index``
   + ``PineconeHook.describe_index``
   + ``PineconeHook.delete_index``
   + ``PineconeHook.configure_index``
   + ``PineconeHook.create_collection``
   + ``PineconeHook.delete_collection``
   + ``PineconeHook.describe_collection``
   + ``PineconeHook.list_collections``
   + ``PineconeHook.query_vector``
   + ``PineconeHook.describe_index_stats``

* ``PineconeHook.create_index`` is updated to accept a ``ServerlessSpec`` or ``PodSpec`` instead of directly accepting index related configurations
* To initialize ``PineconeHook`` object, API key needs to be passed via argument or the connection.

* ``Pinecone provider support for 'pinecone-client'>=3  (#37307)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.7.0 (#39240)``
* ``Faster 'airflow_version' imports (#39552)``
* ``Simplify 'airflow_version' imports (#39497)``
* ``CreatePodIndexOperator fix defaults of pod_type and metric parameters (#39365)``
* ``Reapply templates for all providers (#39554)``
* ``Fix the argument type of input_vectors in pinecone upsert (#39688)``

.. Review and move the new changes to one of the sections above:
   * ``Prepare docs 1st wave (RC1) April 2024 (#38863)``
   * ``Bump ruff to 0.3.3 (#38240)``
   * ``Prepare docs 1st wave (RC1) March 2024 (#37876)``
   * ``Add comment about versions updated by release manager (#37488)``
   * ``D401 fixes in Pinecone provider (#37270)``
   * ``Prepare docs 1st wave May 2024 (#39328)``
   * ``Prepare docs 2nd wave May 2024 (#39565)``

1.1.2
.....

Misc
~~~~

* ``Limit 'pinecone-client' to <3.0 (#36818)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs 1st wave of Providers January 2024 (#36640)``
   * ``Speed up autocompletion of Breeze by simplifying provider state (#36499)``
   * ``Provide the logger_name param in providers hooks in order to override the logger name (#36675)``
   * ``Revert "Provide the logger_name param in providers hooks in order to override the logger name (#36675)" (#37015)``
   * ``Prepare docs 2nd wave of Providers January 2024 (#36945)``

1.1.1
.....

Bug Fixes
~~~~~~~~~

* ``Follow BaseHook connection fields method signature in child classes (#36086)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.1.0
.....

.. note::
  This release of provider is only available for Airflow 2.6+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.6.0 (#36017)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix and reapply templates for provider documentation (#35686)``

   * ``Prepare docs 2nd wave of Providers November 2023 (#35836)``
   * ``Use reproducible builds for provider packages (#35693)``

1.0.0
.....

Initial version of the provider.
