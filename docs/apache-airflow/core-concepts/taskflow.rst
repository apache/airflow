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

TaskFlow
========

.. versionadded:: 2.0

If you write most of your DAGs using plain Python code rather than Operators, then the TaskFlow API will make it much easier to author clean DAGs without extra boilerplate, all using the ``@task`` decorator.

TaskFlow takes care of moving inputs and outputs between your Tasks using XComs for you, as well as automatically calculating dependencies - when you call a TaskFlow function in your DAG file, rather than executing it, you will get an object representing the XCom for the result (an ``XComArg``), that you can then use as inputs to downstream tasks or operators. For example::

    from airflow.decorators import task
    from airflow.operators.email import EmailOperator

    @task
    def get_ip():
        return my_ip_service.get_main_ip()

    @task
    def compose_email(external_ip):
        return {
            'subject':f'Server connected from {external_ip}',
            'body': f'Your server executing Airflow is connected from the external IP {external_ip}<br>'
        }

    email_info = compose_email(get_ip())

    EmailOperator(
        task_id='send_email',
        to='example@example.com',
        subject=email_info['subject'],
        html_content=email_info['body']
    )

Here, there are three tasks - ``get_ip``, ``compose_email``, and ``send_email``.

The first two are declared using TaskFlow, and automatically pass the return value of ``get_ip`` into ``compose_email``, not only linking the XCom across, but automatically declaring that ``compose_email`` is *downstream* of ``get_ip``.

``send_email`` is a more traditional Operator, but even it can use the return value of ``compose_email`` to set its parameters, and again, automatically work out that it must be *downstream* of ``compose_email``.

You can also use a plain value or variable to call a TaskFlow function - for example, this will work as you expect (but, of course, won't run the code inside the task until the DAG is executed - the ``name`` value is persisted as a task parameter until that time)::

    @task
    def hello_name(name: str):
        print(f'Hello {name}!')

    hello_name('Airflow users')

If you want to learn more about using TaskFlow, you should consult :doc:`the TaskFlow tutorial </tutorial/taskflow>`.

Context
-------

When running your callable, Airflow will pass a set of keyword arguments that can be used in your function. This set of kwargs correspond exactly to the :ref:`context variables<templates:variables>` you can use in your Jinja templates.

For this to work, you need to define ``**kwargs`` in your function header, or you can add directly the keyword arguments you would like to get such as ``ti=None`` to have the task instance passed.

Logging
-------

To use logging from your task functions, simply import and use Python's logging system:

.. code-block:: python

   logger = logging.getlogger("airflow.task")

Every logging line created this way will be recorded in the task log.

History
-------

The TaskFlow API is new as of Airflow 2.0, and you are likely to encounter DAGs written for previous versions of Airflow that instead use ``PythonOperator`` to achieve similar goals, albeit with a lot more code.

More context around the addition and design of the TaskFlow API can be found as part of its Airflow Improvement Proposal
`AIP-31: "TaskFlow API" for clearer/simpler DAG definition <https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=148638736>`_
