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

Package ``apache-airflow-providers-akeyless``
=============================================

Release: ``1.0.0``

`Akeyless <https://www.akeyless.io/>`__

Provider package
----------------

This is a provider package for the ``akeyless`` provider. All classes for this
provider package are in the ``airflow.providers.akeyless`` python package.

Installation
------------

You can install this package on top of an existing Airflow installation via
``pip install apache-airflow-providers-akeyless``.

The package supports the following Python versions: 3.10, 3.11, 3.12, 3.13.

Requirements
------------

==================  ================
PIP package         Version required
==================  ================
``apache-airflow``  ``>=2.11.0``
``akeyless``        ``>=5.0.0``
==================  ================

Cross provider package dependencies
------------------------------------

Those are dependencies that might be needed in order to use all the features of
the package.

===================================  ===========
Dependent package                    Extra
===================================  ===========
``akeyless_cloud_id``                ``cloud_id``
===================================  ===========

Features
--------

* **Hook** (``AkeylessHook``): Interact with Akeyless Vault Platform — static,
  dynamic, and rotated secrets; list/describe/create/update/delete items.
* **Connection type** (``akeyless``): Custom Airflow connection with UI fields for
  all Akeyless authentication methods (API Key, AWS IAM, GCP, Azure AD, UID,
  JWT, Kubernetes, Certificate).
* **Secrets Backend** (``AkeylessBackend``): Source Airflow Connections, Variables,
  and Configuration directly from Akeyless.
