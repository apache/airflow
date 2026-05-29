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

FAB ``airflowctl`` Commands
===========================

The FAB provider exposes ``users`` and ``roles`` subcommands through
``airflowctl``. They mirror the core ``airflow users`` / ``airflow roles``
CLI but talk to the FAB auth-manager Public API
(``/auth/fab/v1/``) over HTTP instead of accessing the metadata database
directly.

Installation
------------

These commands light up automatically when both ``apache-airflow-ctl`` and
the FAB provider are installed. The provider exposes the ``[airflowctl]``
extra so you can pull both in one step:

.. code-block:: bash

   pip install "apache-airflow-providers-fab[airflowctl]"

Discovery
---------

``airflowctl`` walks the ``apache_airflow_provider`` entry-point group at
parser-construction time, calls each provider's ``get_provider_info()``,
and reads the ``ctl`` field. The FAB provider declares its ``ctl`` factory
in ``provider.yaml``:

.. code-block:: yaml

   ctl:
     - airflow.providers.fab.auth_manager.ctl_commands.definition.get_fab_airflowctl_commands

Command reference
-----------------

.. argparse::
   :module: airflow.providers.fab.auth_manager.ctl_commands.definition
   :func: get_parser
   :prog: airflowctl
