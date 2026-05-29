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

Edge ``airflowctl`` Commands
============================

The Edge3 provider exposes the ``edge`` subcommand through ``airflowctl``.
Today it is intentionally narrow — only the public worker-api routes under
``airflow.providers.edge3.worker_api.routes`` (excluding ``ui.py``, which is
not part of the public surface) qualify, per the AIP-94 direction described
in :doc:`apache-airflow:contributing-docs/27_cli_implementation_guide`.

The remaining ``airflow edge ...`` operator commands (e.g. ``list-workers``,
``remove-remote-edge-worker``, ``add-worker-queues``) currently rely on
direct DB access from ``airflow.providers.edge3.cli.edge_command`` and are
**not** part of the public API contract. They will move here once
equivalent public routes exist; until then, keep using the core
``airflow edge ...`` CLI for those.

Discovery
---------

``airflowctl`` walks the ``apache_airflow_provider`` entry-point group at
parser-construction time, calls each provider's ``get_provider_info()``,
and reads the ``ctl`` field. The Edge3 provider declares its ``ctl``
factory in ``provider.yaml``:

.. code-block:: yaml

   ctl:
     - airflow.providers.edge3.ctl.definition.get_edge_airflowctl_commands

Command reference
-----------------

.. argparse::
   :module: airflow.providers.edge3.ctl.definition
   :func: get_parser
   :prog: airflowctl
