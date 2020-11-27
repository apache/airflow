
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

HTTP Operators
==============

The following code examples use the ``http_default`` connection which means the requests are sent against
`httpbin <https://www.httpbin.org/>`__ site to perform basic HTTP operations.

.. _howto/operator:HttpSensor:

HttpSensor
----------

Use the :class:`~airflow.providers.http.sensors.http.HttpSensor` to poke until the ``response_check`` callable evaluates
to ``true``.

Here we are poking until httpbin gives us a response text containing ``httpbin``.

.. exampleinclude:: /../../airflow/providers/http/example_dags/example_http.py
    :language: python
    :start-after: [START howto_operator_http_http_sensor_check]
    :end-before: [END howto_operator_http_http_sensor_check]

.. _howto/operator:SimpleHttpOperator:

SimpleHttpOperator
------------------

Use the :class:`~airflow.providers.http.operators.http.SimpleHttpOperator` to call HTTP requests and get
the response text back.

In the first example we are calling a ``POST`` with json data and succeed when we get the same json data back
otherwise the task will fail.

.. exampleinclude:: /../../airflow/providers/http/example_dags/example_http.py
    :language: python
    :start-after: [START howto_operator_http_task_post_op]
    :end-before: [END howto_operator_http_task_post_op]

Here we are calling a ``GET`` request and pass params to it. The task will succeed regardless of the response text.

.. exampleinclude:: /../../airflow/providers/http/example_dags/example_http.py
    :language: python
    :start-after: [START howto_operator_http_task_get_op]
    :end-before: [END howto_operator_http_task_get_op]

SimpleHttpOperator returns the response body as text by default. If you want to modify the response before passing
it on the next task downstream use ``response_filter``. This is useful if:

- the API you are consuming returns a large JSON payload and you're interested in a subset of the data
- the API returns data in xml or csv and you want to convert it to JSON
- you're interested in the headers of the response instead of the body

Below is an example of retrieving data from a REST API and only returning a nested property instead of the full
response body.

.. exampleinclude:: /../../airflow/providers/http/example_dags/example_http.py
    :language: python
    :start-after: [START howto_operator_http_task_get_op_response_filter]
    :end-before: [END howto_operator_http_task_get_op_response_filter]

In the third example we are performing a ``PUT`` operation to put / set data according to the data that is being
provided to the request.

.. exampleinclude:: /../../airflow/providers/http/example_dags/example_http.py
    :language: python
    :start-after: [START howto_operator_http_task_put_op]
    :end-before: [END howto_operator_http_task_put_op]

In this example we call a ``DELETE`` operation to the ``delete`` endpoint. This time we are passing form data to the
request.

.. exampleinclude:: /../../airflow/providers/http/example_dags/example_http.py
    :language: python
    :start-after: [START howto_operator_http_task_del_op]
    :end-before: [END howto_operator_http_task_del_op]

Here we pass form data to a ``POST`` operation which is equal to a usual form submit.

.. exampleinclude:: /../../airflow/providers/http/example_dags/example_http.py
    :language: python
    :start-after: [START howto_operator_http_task_post_op_formenc]
    :end-before: [END howto_operator_http_task_post_op_formenc]
