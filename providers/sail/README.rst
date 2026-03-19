
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

Package ``apache-airflow-providers-sail``

Release: ``1.0.0``


`Sail <https://lakesail.com/>`__


Provider package
----------------

This is a provider package for ``sail`` provider. All classes for this provider package
are in ``airflow.providers.sail`` python package.

Sail is a Rust-native computation engine compatible with Apache Spark that implements the
Spark Connect protocol. It enables existing PySpark code to run without modification,
without requiring the JVM.

Installation
------------

You can install this package on top of an existing Airflow installation (see ``Requirements`` below
for the minimum Airflow version supported) via
``pip install apache-airflow-providers-sail``

The package supports the following python versions: 3.10,3.11,3.12,3.13

Requirements
------------

==========================================  ==================
PIP package                                 Version required
==========================================  ==================
``apache-airflow``                          ``>=2.11.0``
``apache-airflow-providers-common-compat``  ``>=1.12.0``
``pysail``                                  ``>=0.5.0``
``pyspark``                                 ``>=3.5.2``
``grpcio-status``                           ``>=1.59.0``
==========================================  ==================
