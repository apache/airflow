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

``apache-airflow-providers-modal``
==================================

The ``modal`` provider owns the Airflow side of `Modal <https://modal.com/>`__ credentials:
the ``modal`` connection type and :class:`~airflow.providers.modal.hooks.modal.ModalHook`,
which turns that connection into an authenticated ``modal.Client``.

What this provider is for
-------------------------

Modal's SDK reads credentials from the worker's environment (``MODAL_TOKEN_ID`` /
``MODAL_TOKEN_SECRET``) or from ``~/.modal.toml``. That works on a laptop and fails the moment
two deployments need different tokens, a token lives in a secrets backend, or a task should
run under a narrower token than the worker has. This provider gives Modal credentials the same
home every other integration has, an Airflow connection, and one hook that every Modal-facing
piece of Airflow code resolves it through:

* ``ModalHook.client`` is a ``modal.Client`` built from the connection.
* ``ModalHook.client_kwargs`` splats ``client=`` into any Modal SDK call that reaches the API
  (``modal.Sandbox.create``, ``modal.Sandbox.list``, ``modal.Secret.from_name``, ...).
* ``ModalHook.lookup_app`` looks up or creates an app in the connection's environment. Sandboxes
  inherit the environment from their app, so this is where the connection's ``environment``
  takes effect.

The hook keeps working with no connection configured: it then defers to the Modal SDK's own
resolution, so a worker that already ran ``modal token new`` needs no Airflow setup. See
:doc:`connections` for the full precedence rules.

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
    :caption: Guides

    Connection types <connections>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Resources

    Python API <_api/airflow/providers/modal/index>
    PyPI Repository <https://pypi.org/project/apache-airflow-providers-modal/>
    Installing from sources <installing-providers-from-sources>

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


apache-airflow-providers-modal package
------------------------------------------------------

`Modal <https://modal.com/>`__ provider for Apache Airflow.
Owns the ``modal`` connection type and a hook that turns it into an authenticated
Modal client, so operators, toolsets and executors share one credential path.


Release: 0.1.0

Provider package
----------------

This package is for the ``modal`` provider.
All classes for this package are included in the ``airflow.providers.modal`` python package.

Installation
------------

You can install this package on top of an existing Airflow installation via
``pip install apache-airflow-providers-modal``.
For the minimum Airflow version supported, see ``Requirements`` below.

Requirements
------------

The minimum Apache Airflow version supported by this provider distribution is ``3.0.0``.

==========================================  ==================
PIP package                                 Version required
==========================================  ==================
``apache-airflow``                          ``>=3.0.0``
``apache-airflow-providers-common-compat``  ``>=1.12.0``
``modal``                                   ``>=1.5.0``
==========================================  ==================

Downloading official packages
-----------------------------

You can download officially released packages and verify their checksums and signatures from the
`Official Apache Download site <https://downloads.apache.org/airflow/providers/>`_

* `The apache-airflow-providers-modal 0.1.0 sdist package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_modal-0.1.0.tar.gz>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_modal-0.1.0.tar.gz.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_modal-0.1.0.tar.gz.sha512>`__)
* `The apache-airflow-providers-modal 0.1.0 wheel package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_modal-0.1.0-py3-none-any.whl>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_modal-0.1.0-py3-none-any.whl.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_modal-0.1.0-py3-none-any.whl.sha512>`__)
