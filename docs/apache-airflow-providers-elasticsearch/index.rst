
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

``apache-airflow-providers-elasticsearch``
==========================================

Content
-------

.. toctree::
    :maxdepth: 1
    :caption: Guides

    Logging for Tasks <logging>

.. toctree::
    :maxdepth: 1
    :caption: References

    Python API <_api/airflow/providers/elasticsearch/index>

.. toctree::
    :maxdepth: 1
    :caption: Resources

    PyPI Repository <https://pypi.org/project/apache-airflow-providers-elasticsearch/>

.. THE REMINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


Package apache-airflow-providers-elasticsearch
------------------------------------------------------

`Elasticsearch <https://https//www.elastic.co/elasticsearch>`__


Release: 1.0.4

Provider package
----------------

This is a provider package for ``elasticsearch`` provider. All classes for this provider package
are in ``airflow.providers.elasticsearch`` python package.

Installation
------------

You can install this package on top of an existing airflow 2.* installation via
``pip install apache-airflow-providers-elasticsearch``

PIP requirements
----------------

=======================  ==================
PIP package              Version required
=======================  ==================
``elasticsearch-dbapi``  ``==0.1.0``
``elasticsearch-dsl``    ``>=5.0.0``
``elasticsearch``        ``>7, <7.6.0``
=======================  ==================

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
