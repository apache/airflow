.. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

Voyage AI Provider
==================

The Voyage AI provider package allows you to connect to the `Voyage AI API <https://docs.voyageai.com/docs/introduction>`__
to generate high-quality text embeddings within your Airflow DAGs.

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Guides

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: References

    _api/airflow/providers/voyageai/operators/embedding/index
    _api/airflow/providers/voyageai/hooks/voyage/index

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: System Tests

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Resources

    Changelog <https://airflow.apache.org/docs/apache-airflow-providers-voyageai/stable/changelog.html>
    PyPI Repository <https://pypi.org/project/apache-airflow-providers-voyageai/>
    Source Code <https://github.com/apache/airflow/tree/main/airflow/providers/voyageai>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Internals

    _api/airflow/providers/voyageai/index

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


apache-airflow-providers-voyageai package
------------------------------------------------------

`Apache Airflow provider to interact with the Voyage AI embeddings API. <https://docs.voyageai.com/docs/introduction>`__


Release: 0.1.0

Provider package
----------------

This package is for the ``voyageai`` provider.
All classes for this package are included in the ``airflow.providers.voyageai`` python package.

Installation
------------

You can install this package on top of an existing Airflow installation via
``pip install apache-airflow-providers-voyageai``.
For the minimum Airflow version supported, see ``Requirements`` below.

Requirements
------------

The minimum Apache Airflow version supported by this provider distribution is ``2.10.0``.

==================  ==================
PIP package         Version required
==================  ==================
``apache-airflow``  ``>=2.10.0``
``voyageai``        ``>=0.2.0``
==================  ==================
