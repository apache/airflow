
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

``apache-airflow-providers-salesforce``
=======================================

Content
-------

.. toctree::
    :maxdepth: 1
    :caption: Guides

    Connection types <connections/salesforce>

.. toctree::
    :maxdepth: 1
    :caption: References

    Python API <_api/airflow/providers/salesforce/index>

.. toctree::
    :maxdepth: 1
    :caption: Resources

    Example DAGs <https://github.com/apache/airflow/tree/master/airflow/providers/salesforce/example_dags>
    PyPI Repository <https://pypi.org/project/apache-airflow-providers-salesforce/>

.. THE REMINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


Package apache-airflow-providers-salesforce
------------------------------------------------------

`Salesforce <https://www.salesforce.com/>`__


Release: 2.0.0

Provider package
----------------

This is a provider package for ``salesforce`` provider. All classes for this provider package
are in ``airflow.providers.salesforce`` python package.

Installation
------------

.. note::

    On November 2020, new version of PIP (20.3) has been released with a new, 2020 resolver. This resolver
    does not yet work with Apache Airflow and might lead to errors in installation - depends on your choice
    of extras. In order to install Airflow you need to either downgrade pip to version 20.2.4
    ``pip install --upgrade pip==20.2.4`` or, in case you use Pip 20.3, you need to add option
    ``--use-deprecated legacy-resolver`` to your pip install command.


You can install this package on top of an existing airflow 2.* installation via
``pip install apache-airflow-providers-salesforce``

PIP requirements
----------------

=======================  ==================
PIP package              Version required
=======================  ==================
``simple-salesforce``    ``>=1.0.0``
``tableauserverclient``
=======================  ==================

Cross provider package dependencies
-----------------------------------

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified backport providers package in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

.. code-block:: bash

    pip install apache-airflow-providers-salesforce[tableau]


======================================================================================================  ===========
Dependent package                                                                                       Extra
======================================================================================================  ===========
`apache-airflow-providers-tableau <https://airflow.apache.org/docs/apache-airflow-providers-tableau>`_  ``tableau``
======================================================================================================  ===========

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

2.0.0
.....

Tableau provider moved to separate 'tableau' provider

Things done:

    - Tableau classes imports classes from 'tableau' provider with deprecation warning

Breaking changes
~~~~~~~~~~~~~~~~

You need to install ``apache-airflow-providers-tableau`` provider additionally to get
Tableau integration working.


1.0.1
.....

Updated documentation and readme files.


1.0.0
.....

Initial version of the provider.
