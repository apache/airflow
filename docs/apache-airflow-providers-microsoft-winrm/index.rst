
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

``apache-airflow-providers-microsoft-winrm``
============================================

Content
-------

.. toctree::
    :maxdepth: 1
    :caption: References

    Python API <_api/airflow/providers/microsoft/winrm/index>

.. toctree::
    :maxdepth: 1
    :caption: Resources

    Example DAGs <https://github.com/apache/airflow/tree/master/airflow/providers/microsoft/winrm/example_dags>
    PyPI Repository <https://pypi.org/project/apache-airflow-providers-microsoft-winrm/>

.. THE REMINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


Package apache-airflow-providers-microsoft-winrm
------------------------------------------------------

`Windows Remote Management (WinRM) <https://docs.microsoft.com/en-us/windows/win32/winrm/portal>`__


Release: 1.2.0

Provider package
----------------

This is a provider package for ``microsoft.winrm`` provider. All classes for this provider package
are in ``airflow.providers.microsoft.winrm`` python package.

Installation
------------

You can install this package on top of an existing airflow 2.* installation via
``pip install apache-airflow-providers-microsoft-winrm``

PIP requirements
----------------

=============  ==================
PIP package    Version required
=============  ==================
``pywinrm``    ``~=0.4``
=============  ==================

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

* ``Display explicit error in case UID has no actual username (#15212)``

1.1.0
.....

Features
~~~~~~~~

* ``WinRM Operator: Fix stout decoding issue (#13153)``

Bug Fixes
~~~~~~~~~

* ``WinRM Operator: Fix stout decoding issue (#13153)``


1.0.1
.....

Updated documentation and readme files.

1.0.0
.....

Initial version of the provider.
