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

*********************************
Contributor's Quick Start - Hatch
*********************************

**The outline for this document in GitHub is available at top-right corner button (with 3-dots and 3 lines).**

`Hatch <https://hatch.pypa.io/latest/>`_ is a popular Python build tool and environment manager
Hatch is the recommended way to run Airflow in a virtual environment.

Installation
############

1. Install pipx: ``pip install --user "pipx>=1.4.1"``
   1. MacOS users may want to consider using brew to install pipx: ``brew install pipx``
2. Add pipx to your PATH:
  1. If you installed pipx with pip on MacOS, run ``python -m pipx ensurepath``
  2. Otherwise, run ``pipx ensurepath``
3. Install hatch: ``pipx install hatch``


Learn more about using Hatch to manage environments in the `Hatch documentation <https://hatch.pypa.io/latest/environment/>`_.

Creating a virtual environment and running Airflow
##################################################

1. Create an Airflow virtual environment: ``hatch env create`` (Make sure to run this from the root Airflow directory)
   1. Optionally install provider packages as needed, for example: ``hatch run -- pip install -e [celery, cncf-kubernetes]``
2. Create two terminal windows, and in each, enter the Airflow virtual environment: ``hatch shell``
3. Run Airflow!
   1. In one terminal, run the Airflow scheduler: ``airflow scheduler``
   2. In the other, run the Airflow webserver: ``airflow webserver``
