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

``apache-airflow-providers-weaviate``

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

2.0.0
......

Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
   We bumped the minimum version of weaviate-client to 4.4.0. Many of the concepts and methods have been changed.
   We suggest you read `Migrate from v3 to v4 <https://weaviate.io/developers/weaviate/client-libraries/python/v3_v4_migration>`_ before you upgrade to this version

  Summary of the key changes:
    * Add columns ``Port``, ``gRPC host``, ``gRPC port``  and ``Use https``, ``Use a secure channel for the underlying gRPC API`` options  to the Weaviate connection. The default values from Airflow providers may not be suitable for using Weaviate correctly, so we recommend explicitly specifying these values.
    * Update ``WeaviateIngestOperator`` and ``WeaviateDocumentIngestOperator`` to use ``WeaviateHook`` with ``weaviate-client`` v4 API. The major changes are changing argument ``class_name`` to ``collection_name`` and removing ``batch_params``.
    * Update ``WeaviateHook`` to utilize ``weaviate-client`` v4 API. The implementation has been extensively changed. We recommend reading `Migrate from v3 to v4 <https://weaviate.io/developers/weaviate/client-libraries/python/v3_v4_migration>`_ to understand the changes on the Weaviate side before using the updated ``WeaviateHook``.
    * Migrate the following ``WeaviateHook`` public methods to v4 API: ``test_connections``, ``query_with_vector``, ``create_object``, ``get_object``, ``delete_object``, ``update_object``, ``replace_object``, ``object_exists``, ``batch_data``, ``get_or_create_object``, ``create_or_replace_document_objects``
    * Rename ``WeaviateHook`` public methods ``update_schema`` as ``update_collection_configuration``, ``create_class`` as ``create_collection``, ``get_schema`` as ``get_collection_configuration``, ``delete_classes`` as ``delete_collections`` and ``query_without_vector`` as ``query_with_text``.
    * Remove the following ``WeaviateHook`` public methods: ``validate_object``, ``update_schema``, ``create_schema``, ``delete_all_schema``, ``check_subset_of_schema``
    * Remove deprecated method ``WeaviateHook.get_client``
    * Remove unused argument ``retry_status_codes`` in ``WeaviateHook.__init__``

* ``Upgrade to weaviate-client to v4 (#40194)``

Bug Fixes
~~~~~~~~~

* ``Fix mypy problems in new weaviate client (#40330)``

.. Review and move the new changes to one of the sections above:
   * ``fix two typos (#40670)``
   * ``Fix weaviate changelog to bring back 1.4.2 (#40663)``
   * ``Prepare docs 1st wave July 2024 (#40644)``

1.4.2
.....

Misc
~~~~

* ``Update pandas minimum requirement for Python 3.12 (#40272)``
* ``Add dependency to httpx >= 0.25.0 everywhere (#40256)``


.. Review and move the new changes to one of the sections above:
   * ``Enable enforcing pydocstyle rule D213 in ruff. (#40448)``
   * ``Prepare docs 2nd wave June 2024 (#40273)``
   * ``implement per-provider tests with lowest-direct dependency resolution (#39946)``

1.4.1
.....

Misc
~~~~

* ``Faster 'airflow_version' imports (#39552)``
* ``Simplify 'airflow_version' imports (#39497)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Reapply templates for all providers (#39554)``

1.4.0
.....

.. note::
  This release of provider is only available for Airflow 2.7+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.7.0 (#39240)``

1.3.4
.....

Bug Fixes
~~~~~~~~~

* ``Fix 'WeaviateIngestOperator'/'WeaviateDocumentIngestOperator' arguments in 'MappedOperator' (#38402)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove unused loop variable from airflow package (#38308)``

1.3.3
.....

Misc
~~~~

* ``Limit 'pandas' to '<2.2' (#37748)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix remaining D401 checks (#37434)``
   * ``Add comment about versions updated by release manager (#37488)``

1.3.2
.....

Misc
~~~~

* ``feat: Switch all class, functions, methods deprecations to decorators (#36876)``

1.3.1
.....

Bug Fixes
~~~~~~~~~

* ``Fix stacklevel in warnings.warn into the providers (#36831)``
* ``init templated field explicitly in constructor (#36908)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Set min pandas dependency to 1.2.5 for all providers and airflow (#36698)``
   * ``Prepare docs 1st wave of Providers January 2024 (#36640)``
   * ``Add flake8-implicit-str-concat check to Ruff (#36597)``
   * ``Prepare docs 2nd wave of Providers January 2024 (#36945)``

1.3.0
.....

Features
~~~~~~~~

* ``Add WeaviateDocumentIngestOperator (#36402)``
* ``Add 'uuid_column', 'tenant' params to WeaviateIngestOperator (#36387)``
* ``Add create_or_replace_document_objects method to weaviate provider (#36177)``

Bug Fixes
~~~~~~~~~

* ``Remove 'insertion_errors' as required argument (#36435)``
* ``Handle  list like input objects in weavaite's 'create_or_replace_document_objects' hook method (#36475)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

.. Review and move the new changes to one of the sections above:
   * ``Speed up autocompletion of Breeze by simplifying provider state (#36499)``
   * ``Add documentation for 3rd wave of providers in Deember (#36464)``

1.2.0
.....

Features
~~~~~~~~

* ``Add helper function for CRUD operations on weaviate's schema and class objects (#35919)``
* ``Add retry mechanism and dataframe support for WeaviateIngestOperator (#36085)``

Bug Fixes
~~~~~~~~~

* ``Fixing template_fields for WeaviateIngestOperator (#36359)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.1.0
.....

.. note::
  This release of provider is only available for Airflow 2.6+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Features
~~~~~~~~

* ``Add object methods in weaviate hook (#35934)``
* ``Add a cache for weaviate client (#35983)``
* ``Add more ways to connect to weaviate (#35864)``

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
