
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

``apache-airflow-providers-microsoft-azure``
============================================

Content
-------

.. toctree::
    :maxdepth: 1
    :caption: Guides

    Connection types <connections/index>
    Operators <operators/index>
    Secrets backends <secrets-backends/azure-key-vault>
    Logging for Tasks <logging>

.. toctree::
    :maxdepth: 1
    :caption: References

    Python API <_api/airflow/providers/microsoft/azure/index>

.. toctree::
    :maxdepth: 1
    :caption: Resources

    Example DAGs <https://github.com/apache/airflow/tree/main/airflow/providers/microsoft/azure/example_dags>
    PyPI Repository <https://pypi.org/project/apache-airflow-providers-microsoft-azure/>

.. THE REMINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


Package apache-airflow-providers-microsoft-azure
------------------------------------------------------

`Microsoft Azure <https://azure.microsoft.com/>`__


Release: 3.0.0

Provider package
----------------

This is a provider package for ``microsoft.azure`` provider. All classes for this provider package
are in ``airflow.providers.microsoft.azure`` python package.

Installation
------------

You can install this package on top of an existing airflow 2.* installation via
``pip install apache-airflow-providers-microsoft-azure``

PIP requirements
----------------

================================  ==================
PIP package                       Version required
================================  ==================
``azure-batch``                   ``>=8.0.0``
``azure-cosmos``                  ``>=3.0.1,<4``
``azure-datalake-store``          ``>=0.0.45``
``azure-identity``                ``>=1.3.1``
``azure-keyvault``                ``>=4.1.0``
``azure-kusto-data``              ``>=0.0.43,<0.1``
``azure-mgmt-containerinstance``  ``>=1.5.0,<2.0``
``azure-mgmt-datafactory``        ``>=1.0.0,<2.0``
``azure-mgmt-datalake-store``     ``>=0.5.0``
``azure-mgmt-resource``           ``>=2.2.0``
``azure-storage-blob``            ``>=12.7.0``
``azure-storage-common``          ``>=2.1.0``
``azure-storage-file``            ``>=2.1.0``
================================  ==================

Cross provider package dependencies
-----------------------------------

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified provider packages in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

.. code-block:: bash

    pip install apache-airflow-providers-microsoft-azure[google]


====================================================================================================  ==========
Dependent package                                                                                     Extra
====================================================================================================  ==========
`apache-airflow-providers-google <https://airflow.apache.org/docs/apache-airflow-providers-google>`_  ``google``
`apache-airflow-providers-oracle <https://airflow.apache.org/docs/apache-airflow-providers-oracle>`_  ``oracle``
====================================================================================================  ==========

Downloading official packages
-----------------------------

You can download officially released packages and verify their checksums and signatures from the
`Official Apache Download site <https://downloads.apache.org/airflow/providers/>`_

* `The apache-airflow-providers-microsoft-azure 3.0.0 sdist package <https://downloads.apache.org/airflow/providers/apache-airflow-providers-microsoft-azure-3.0.0.tar.gz>`_ (`asc <https://downloads.apache.org/airflow/providers/apache-airflow-providers-microsoft-azure-3.0.0.tar.gz.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache-airflow-providers-microsoft-azure-3.0.0.tar.gz.sha512>`__)
* `The apache-airflow-providers-microsoft-azure 3.0.0 wheel package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_microsoft_azure-3.0.0-py3-none-any.whl>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_microsoft_azure-3.0.0-py3-none-any.whl.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_microsoft_azure-3.0.0-py3-none-any.whl.sha512>`__)

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

3.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* ``Auto-apply apply_default decorator (#15667)``

Features
~~~~~~~~

* ``add oracle  connection link (#15632)``
* ``Add delimiter argument to WasbHook delete_file method (#15637)``

Bug Fixes
~~~~~~~~~

* ``Fix colon spacing in ``AzureDataExplorerHook`` docstring (#15841)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Bump pyupgrade v2.13.0 to v2.18.1 (#15991)``
   * ``Rename example bucket names to use INVALID BUCKET NAME by default (#15651)``
   * ``Docs: Replace 'airflow' to 'apache-airflow' to install extra (#15628)``

2.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* ``Removes unnecessary AzureContainerInstance connection type (#15514)``

This change removes ``azure_container_instance_default`` connection type and replaces it with the
``azure_default``. The problem was that AzureContainerInstance was not needed as it was exactly the
same as the plain "azure" connection, however it's presence caused duplication in the field names
used in the UI editor for connections and unnecessary warnings generated. This version uses
plain Azure Hook and connection also for Azure Container Instance. If you already have
``azure_container_instance_default`` connection created in your DB, it will continue to work, but
the first time you edit it with the UI you will have to change it's type to ``azure_default``.

Features
~~~~~~~~

* ``Add dynamic connection fields to Azure Connection (#15159)``

Bug fixes
~~~~~~~~~

* ``Fix 'logging.exception' redundancy (#14823)``


1.3.0
.....

Features
~~~~~~~~

* ``A bunch of template_fields_renderers additions (#15130)``

Bug fixes
~~~~~~~~~

* ``Fix attributes for AzureDataFactory hook (#14704)``

1.2.0
.....

Features
~~~~~~~~

* ``Add Azure Data Factory hook (#11015)``

Bug fixes
~~~~~~~~~

* ``BugFix: Fix remote log in azure storage blob displays in one line (#14313)``
* ``Fix AzureDataFactoryHook failing to instantiate its connection (#14565)``

1.1.0
.....

Updated documentation and readme files.

Features
~~~~~~~~

* ``Upgrade azure blob to v12 (#12188)``
* ``Fix Azure Data Explorer Operator (#13520)``
* ``add AzureDatalakeStorageDeleteOperator (#13206)``

1.0.0
.....

Initial version of the provider.
