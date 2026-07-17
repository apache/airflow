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

Celery Executor Commands
------------------------

.. note::
   The CLI commands below are used from provider by Airflow 2.7.0+.
   Previously they were part of the core Airflow, so if you are using Airflow below 2.7.0 look at
   the core Airflow documentation for the list of CLI commands and parameters available.


.. argparse::
   :module: airflow.providers.celery.cli.definition
   :func: get_parser
   :prog: airflow
