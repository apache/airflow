
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

``apache-airflow-providers-hashicorp``
======================================

Content
-------

.. toctree::
    :maxdepth: 1
    :caption: Guides

    Secrets backends <secrets-backends/hashicorp-vault>

.. toctree::
    :maxdepth: 1
    :caption: References

    Python API <_api/airflow/providers/hashicorp/index>

.. toctree::
    :maxdepth: 1
    :caption: Resources

    PyPI Repository <https://pypi.org/project/apache-airflow-providers-hashicorp/>

.. THE REMINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


Package apache-airflow-providers-hashicorp
------------------------------------------------------

Hashicorp including `Hashicorp Vault <https://www.vaultproject.io/>`__


Release: 2.0.0

Provider package
----------------

This is a provider package for ``hashicorp`` provider. All classes for this provider package
are in ``airflow.providers.hashicorp`` python package.

Installation
------------

You can install this package on top of an existing airflow 2.* installation via
``pip install apache-airflow-providers-hashicorp``

PIP requirements
----------------

=============  ==================
PIP package    Version required
=============  ==================
``hvac``       ``~=0.10``
=============  ==================

Cross provider package dependencies
-----------------------------------

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified provider packages in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

.. code-block:: bash

    pip install apache-airflow-providers-hashicorp[google]


====================================================================================================  ==========
Dependent package                                                                                     Extra
====================================================================================================  ==========
`apache-airflow-providers-google <https://airflow.apache.org/docs/apache-airflow-providers-google>`_  ``google``
====================================================================================================  ==========

Downloading official packages
-----------------------------

You can download officially released packages and verify their checksums and signatures from the
`Official Apache Download site <https://downloads.apache.org/airflow/providers/>`_

* `The apache-airflow-providers-hashicorp 2.0.0 sdist package <https://downloads.apache.org/airflow/providers/apache-airflow-providers-hashicorp-2.0.0.tar.gz>`_ (`asc <https://downloads.apache.org/airflow/providers/apache-airflow-providers-hashicorp-2.0.0.tar.gz.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache-airflow-providers-hashicorp-2.0.0.tar.gz.sha512>`__)
* `The apache-airflow-providers-hashicorp 2.0.0 wheel package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_hashicorp-2.0.0-py3-none-any.whl>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_hashicorp-2.0.0-py3-none-any.whl.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_hashicorp-2.0.0-py3-none-any.whl.sha512>`__)

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

Breaking changes
~~~~~~~~~~~~~~~~

* ``Auto-apply apply_default decorator (#15667)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.0.2
.....

Bug Fixes
~~~~~~~~~

* ``Fix deprecated warning hvac auth (#15216)``

1.0.1
.....

Updated documentation and readme files.

1.0.0
.....

Initial version of the provider.
