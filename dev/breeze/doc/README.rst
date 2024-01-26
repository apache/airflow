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

.. raw:: html

    <div align="center">
      <img src="../../../images/AirflowBreeze_logo.png"
           alt="Airflow Breeze - Development and Test Environment for Apache Airflow">
    </div>

Airflow Breeze CI environment
=============================

Airflow Breeze is an easy-to-use development and test environment using
`Docker Compose <https://docs.docker.com/compose/>`_.
The environment is available for local use and is also used in Airflow's CI tests.

We call it *Airflow Breeze* as **It's a Breeze to contribute to Airflow**.

The advantages and disadvantages of using the Breeze environment vs. other ways of testing Airflow
are described in
`Integration test development environments <../../../contributing-docs/06_development_environments.rst>`_.

You can use the Breeze environment to run Airflow's tests locally and reproduce CI failures.

The following documents describe how to use the Breeze environment:

* `Installation <01_installation.rst>`_ - describes how to install the Breeze environment on your machine.
* `Customizing <02_customizing.rst>`_ - describes how to customize the Breeze environment.
* `Developer tasks <03_developer_tasks.rst>`_ - describes how to use Breeze for regular developer tasks.
* `Troubleshooting <04_troubleshooting.rst>`_ - describes how to troubleshoot the Breeze environment.
* `Test commands <05_test_commands.rst>`_ - describes how to run Airflow's tests (and reproduce CI failures) using the Breeze environment.
* `Managing Docker images <06_managing_docker_images.rst>`_ - describes how to manage the Breeze images.
* `Breeze maintenance tasks <07_breeze_maintenance_tasks.rst>`_ - describes how to perform maintenance tasks when you develop Breeze itself.
* `CI tasks <08_ci_tasks.rst>`_ - describes how Breeze commands are used in our CI.
* `Release management tasks <09_release_management_tasks.rst>`_ - describes how to use Breeze for release management tasks.
* `Advanced Breeze topics <10_advanced_breeze_topics.rst>`_ - describes advanced Breeze topics/internals of Breeze.

You can also learn more context and Architecture Decisions taken when developing Breeze in the
`Architecture Decision Records <adr>`_.

Next step: Follow the `Installation <01_installation.rst>`__ instructions to install the Breeze environment.
