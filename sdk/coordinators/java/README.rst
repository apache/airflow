
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

Package ``apache-airflow-coordinators-java``
===========================================

Java runtime coordinator for the Apache Airflow Task SDK.

This distribution contributes the ``airflow.sdk.coordinators.java.JavaCoordinator``
class, which spawns a JVM subprocess to parse Java DAG bundles (``.jar``)
and execute Java tasks. It is loaded via the ``[sdk] coordinators`` configuration
and is *not* a standard Airflow provider — it does not register hooks, operators,
or any other provider-managed resources.

Configure it in ``airflow.cfg``::

    [sdk]
    coordinators = [
        {
            "name": "jdk-17",
            "classpath": "airflow.sdk.coordinators.java.JavaCoordinator",
            "kwargs": {
                "java_executable": "/usr/lib/jvm/java-17-openjdk/bin/java",
                "jvm_args": ["-Xmx1024m"],
                "bundles_folder": "~/airflow/java-bundles"
            }
        }
    ]
    queue_to_coordinator = {"java-queue": "jdk-17"}

Installation
------------

::

    pip install apache-airflow-coordinators-java
