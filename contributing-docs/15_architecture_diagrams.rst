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

Architecture Diagrams
=====================

We started to use (and gradually convert old diagrams to use it) `Diagrams <https://diagrams.mingrammer.com/>`_
as our tool of choice to generate diagrams. The diagrams are generated from Python code and can be
automatically updated when the code changes. The diagrams are generated using pre-commit hooks (See
static checks below) but they can also be generated manually by running the corresponding Python code.

To run the code you need to install the dependencies in the virtualenv you use to run it:
* ``pip install diagrams rich``. You need to have graphviz installed in your
system (``brew install graphviz`` on macOS for example).

The source code of the diagrams are next to the generated diagram, the difference is that the source
code has ``.py`` extension and the generated diagram has ``.png`` extension. The pre-commit hook ``generate-airflow-diagrams``
will look for ``diagram_*.py`` files in the ``docs`` subdirectories
to find them and runs them when the sources changed and the diagrams are not up to date (the
pre-commit will automatically generate an .md5sum hash of the sources and store it next to the diagram
file).

In order to generate the diagram manually you can run the following command:

.. code-block:: bash

    python <path-to-diagram-file>.py

You can also generate all diagrams by:

.. code-block:: bash

    pre-commit run generate-airflow-diagrams

or with Breeze:

.. code-block:: bash

    breeze static-checks --type generate-airflow-diagrams --all-files

When you iterate over a diagram, you can also setup a "save" action in your IDE to run the python
file automatically when you save the diagram file.

Once you've done iteration and you are happy with the diagram, you can commit the diagram, the source
code and the .md5sum file. The pre-commit hook will then not run the diagram generation until the
source code for it changes.

----

You can  now see an overview of the whole `contribution workflow <16_contribution_workflow.rst>`__
