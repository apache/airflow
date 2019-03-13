..  Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

..  Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

BashOperator
============

Use the :class:`~airflow.operators.bash_operator.BashOperator` to execute
commands in a `Bash <https://www.gnu.org/software/bash/>`__ shell.

.. literalinclude:: ../../../airflow/example_dags/example_bash_operator.py
    :language: python
    :start-after: [START howto_operator_bash]
    :end-before: [END howto_operator_bash]

Templating
----------

You can use :ref:`Jinja templates <jinja-templating>` to parameterize the
``bash_command`` argument.

.. literalinclude:: ../../../airflow/example_dags/example_bash_operator.py
    :language: python
    :start-after: [START howto_operator_bash_template]
    :end-before: [END howto_operator_bash_template]

Troubleshooting
---------------

Jinja template not found
""""""""""""""""""""""""

Add a space after the script name when directly calling a Bash script with
the ``bash_command`` argument. This is because Airflow tries to apply a Jinja
template to it, which will fail.

.. code-block:: python

    t2 = BashOperator(
        task_id='bash_example',

        # This fails with `Jinja template not found` error
        # bash_command="/home/batcher/test.sh",

        # This works (has a space after)
        bash_command="/home/batcher/test.sh ",
        dag=dag)
