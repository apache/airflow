
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

``apache-airflow-providers-apache-ozone``
============================================

.. toctree::
   :hidden:
   :maxdepth: 1
   :caption: Basics

   Home <self>
   Changelog <changelog>
   Security <security>

.. toctree::
   :hidden:
   :maxdepth: 1
   :caption: References

   Python API <_api/airflow/providers/apache/ozone/index>

.. toctree::
   :hidden:
   :maxdepth: 1
   :caption: Guides

   Connections <connections>
   Operators <operators>
   Sensors <sensors>
   Transfers <transfers>
   Hooks <hooks>
   Example DAGs <example-dags>

.. toctree::
   :hidden:
   :maxdepth: 1
   :caption: Resources

   Installing from sources <installing-providers-from-sources>

Package overview
----------------

`Apache Ozone <https://ozone.apache.org/>`__ provider package for Airflow.

Release: ``0.1.0``

Python package path:
``airflow.providers.apache.ozone``

Current provider scope
----------------------

This provider is Native CLI-first and supports Ozone workflows through
``ozone sh`` and ``ozone fs`` commands:

* Ozone administration (volume/bucket lifecycle, quotas, admin extras)
* Ozone filesystem operations (create/list/delete/move/copy/upload/download)
* Ozone path sensors
* HDFS -> Ozone DistCp transfer
* Ozone bucket snapshot backup operator
* Connection-driven SSL and Kerberos runtime wiring

Runtime deployment model
------------------------

This provider does not provision Ozone client runtime by itself.
It is expected to run in one of the following environments:

* on a shared worker host or container image where Ozone client runtime is already
  installed and maintained together with Airflow;
* on a worker host or container image prepared manually with the required
  Ozone runtime files.

For manual preparation, the worker runtime should be provisioned with:

* Java runtime installed on the worker host or available in the container image;
* Ozone CLI runtime copied to a stable location such as ``/opt/ozone``;
* Ozone client configuration copied to a stable directory such as
  ``/etc/ozone/conf``;
* at minimum, ``core-site.xml`` and ``ozone-site.xml`` available in that
  configuration directory;
* environment variables wired for task runtime, typically
  ``JAVA_HOME``, ``JAVA``, ``OZONE_HOME``, ``OZONE_LIBEXEC_DIR``,
  ``OZONE_CONF_DIR``, and ``HADOOP_CONF_DIR``;
* ``ozone`` available in ``PATH`` for interactive shell runs and for Airflow
  service runtime;
* working hostname resolution for Ozone Manager and related cluster endpoints,
  whether via DNS or explicit host mappings.

In practical terms, this means the provider works best when Airflow runs close
to an Ozone-enabled runtime, or when the same runtime is reproduced explicitly
on the worker host or in the container image.

Quick start checklist
---------------------

For a minimal working setup:

1. Prepare Ozone client runtime on the worker host or in the container image.
2. Make ``ozone`` available in ``PATH`` together with Java runtime.
3. Place ``core-site.xml`` and ``ozone-site.xml`` in a directory available to
   the worker, then point ``ozone_conf_dir`` to that directory in the Airflow
   connection.
4. Create an ``ozone`` connection in Airflow and verify it with
   ``test_connection()`` or an example DAG.

Not in scope
------------

* No built-in S3 layer in this provider
* No compatibility aliases for connection keys

Connection contract
-------------------

Runtime connection parsing is centralized in:
``airflow.providers.apache.ozone.utils.connection_schema``
(``OzoneConnSnapshot``).

Built-in provider runtime uses typed snapshot fields.
Raw ``Connection.extra`` is exposed as ``snapshot.raw_extra`` only for
custom DAG-level extensions.

Requirements
------------

* ``apache-airflow`` >= ``2.11.0``

Example DAGs
------------

Example DAGs are located in:
``providers/apache/ozone/src/airflow/providers/apache/ozone/example_dags``.

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


apache-airflow-providers-apache-ozone package
------------------------------------------------------

`Apache Ozone <https://ozone.apache.org/>`__

Provider package for interacting with Apache Ozone via:

- Native CLI (``ozone``) for ``ofs://`` / ``o3fs://`` paths
- Native Ozone administration and filesystem workflows


Release: 0.1.0

Provider package
----------------

This package is for the ``apache.ozone`` provider.
All classes for this package are included in the ``airflow.providers.apache.ozone`` python package.

Installation
------------

You can install this package on top of an existing Airflow installation via
``pip install apache-airflow-providers-apache-ozone``.
For the minimum Airflow version supported, see ``Requirements`` below.

Requirements
------------

The minimum Apache Airflow version supported by this provider distribution is ``2.11.0``.

==================  ==================
PIP package         Version required
==================  ==================
``apache-airflow``  ``>=2.11.0``
==================  ==================

Downloading official packages
-----------------------------

You can download officially released packages and verify their checksums and signatures from the
`Official Apache Download site <https://downloads.apache.org/airflow/providers/>`_

* `The apache-airflow-providers-apache-ozone 0.1.0 sdist package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_apache_ozone-0.1.0.tar.gz>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_apache_ozone-0.1.0.tar.gz.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_apache_ozone-0.1.0.tar.gz.sha512>`__)
* `The apache-airflow-providers-apache-ozone 0.1.0 wheel package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_apache_ozone-0.1.0-py3-none-any.whl>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_apache_ozone-0.1.0-py3-none-any.whl.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_apache_ozone-0.1.0-py3-none-any.whl.sha512>`__)
