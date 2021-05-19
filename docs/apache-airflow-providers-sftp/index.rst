
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

``apache-airflow-providers-sftp``
=================================

Content
-------

.. toctree::
    :maxdepth: 1
    :caption: References

    Connection types <connections/sftp>
    Python API <_api/airflow/providers/sftp/index>

.. toctree::
    :maxdepth: 1
    :caption: Resources

    PyPI Repository <https://pypi.org/project/apache-airflow-providers-sftp/>

.. THE REMINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


Package apache-airflow-providers-sftp
------------------------------------------------------

`SSH File Transfer Protocol (SFTP) <https://tools.ietf.org/wg/secsh/draft-ietf-secsh-filexfer/>`__


Release: 1.2.0

Provider package
----------------

This is a provider package for ``sftp`` provider. All classes for this provider package
are in ``airflow.providers.sftp`` python package.

Installation
------------

You can install this package on top of an existing airflow 2.* installation via
``pip install apache-airflow-providers-sftp``

PIP requirements
----------------

=============  ==================
PIP package    Version required
=============  ==================
``paramiko``   ``>=2.6.0``
``pysftp``     ``>=0.2.9``
``sshtunnel``  ``>=0.1.4,<0.2``
=============  ==================

Cross provider package dependencies
-----------------------------------

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified provider packages in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

.. code-block:: bash

    pip install apache-airflow-providers-sftp[ssh]


==============================================================================================  =======
Dependent package                                                                               Extra
==============================================================================================  =======
`apache-airflow-providers-ssh <https://airflow.apache.org/docs/apache-airflow-providers-ssh>`_  ``ssh``
==============================================================================================  =======

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

1.2.0
.....

Features
~~~~~~~~

* ``Undeprecate private_key option in SFTPHook (#15348)``
* ``Add logs to show last modified in SFTP, FTP and Filesystem sensor (#15134)``

1.1.1
.....

Bug fixes
~~~~~~~~~

* ``Corrections in docs and tools after releasing provider RCs (#14082)``


1.1.0
.....

Updated documentation and readme files.

Features
~~~~~~~~

* ``Add retryer to SFTP hook connection (#13065)``


1.0.0
.....

Initial version of the provider.
