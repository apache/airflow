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


Changelog
---------

2.0.1
.....

Breaking changes
~~~~~~~~~~~~~~~~

* ``Auto-apply apply_default decorator (#15667)``
* ``Remove support Jinja templated log_id in Elasticsearch (#16465)``

  While undocumented, previously ``[elasticsearch] log_id`` supported a Jinja templated string.
  Support for Jinja templates has now been removed. ``log_id`` should be a template string instead,
  for example: ``{dag_id}-{task_id}-{execution_date}-{try_number}``.

  If you used a Jinja template previously, the ``execution_date`` on your Elasticsearch documents will need
  to be updated to the new format.

.. warning:: Due to apply_default decorator removal, this version of the provider requires Airflow 2.1.0+.
   If your Airflow version is < 2.1.0, and you want to install this provider version, first upgrade
   Airflow to at least version 2.1.0. Otherwise your Airflow package version will be upgraded
   automatically and you will have to manually run ``airflow upgrade db`` to complete the migration.

Features
~~~~~~~~

* ``Support remote logging in elasticsearch with filebeat 7 (#14625)``
* ``Support non-https elasticsearch external links (#16489)``

Bug fixes
~~~~~~~~~

* ``Fix external elasticsearch logs link (#16357)``
* ``Fix Elasticsearch external log link with &#39;&#39;json_format&#39;&#39; (#16467)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Bump pyupgrade v2.13.0 to v2.18.1 (#15991)``
   * ``Updated documentation for June 2021 provider release (#16294)``
   * ``More documentation update for June providers release (#16405)``
   * ``Docs: Fix url for ''Elasticsearch'' (#16275)``
   * ``Add ElasticSearch Connection Doc (#16436)``
   * ``More documentation update for June providers release (#16405)``
   * ``Synchronizes updated changelog after buggfix release (#16464)``

1.0.4
.....

Bug fixes
~~~~~~~~~

* ``Fix 'logging.exception' redundancy (#14823)``
* ``Fix exception caused by missing keys in the ElasticSearch Record (#15163)``

1.0.3
.....

Bug fixes
~~~~~~~~~

* ``Elasticsearch Provider: Fix logs downloading for tasks (#14686)``

1.0.2
.....

Bug fixes
~~~~~~~~~

* ``Corrections in docs and tools after releasing provider RCs (#14082)``

1.0.1
.....

Updated documentation and readme files.

Bug fixes
~~~~~~~~~

* ``Respect LogFormat when using ES logging with Json Format (#13310)``


1.0.0
.....

Initial version of the provider.
