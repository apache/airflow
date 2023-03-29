
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

.. exampleinclude:: /../../tests/system/providers/http/example_http.py
    :language: python
    :start-after: [START howto_operator_http_http_sensor_check]
    :end-before: [END howto_operator_http_http_sensor_check]

.. _howto/operator:SimpleHttpOperator:

SimpleHttpOperator
------------------

Use the :class:`~airflow.providers.http.operators.http.SimpleHttpOperator` to call HTTP requests and get
the response text back.

.. warning:: Configuring ``https`` via SimpleHttpOperator is counter-intuitive

   For historical reasons, configuring ``HTTPS`` connectivity via HTTP operator is, well, difficult and
   counter-intuitive. The Operator defaults to ``http`` protocol and you can change the schema used by the
   operator via ``scheme`` connection attribute. However, this field was originally added to connection for
   database type of URIs, where database schemes are set traditionally as first component of URI ``path``.
   Therefore if you want to configure as ``https`` connection via URI, you need to pass ``https`` scheme
   to the SimpleHttpOperator. AS stupid as it looks, your connection URI will look like this:
   ``http://your_host:443/https``. Then if you want to use different URL paths in SimpleHttpOperator
   you should pass your path as ``endpoint`` parameter when running the task. For example to run a query to
   ``https://your_host:443/my_endpoint`` you need to set the endpoint parameter to ``my_endpoint``.
   Alternatively, if you want, you could also percent-encode the host including the ``https://`` prefix,
   and as long it contains ``://`` (percent-encoded ``%3a%2f%2f``), the first component of the path will
   not be used as scheme. Your URI definition might then look like ``http://https%3a%2f%2fyour_host:443/``.
   In this case, however, the ``path`` will not be used at all - you still need to use ``endpoint``
   parameter in the task if wish to make a request with specific path. As counter-intuitive as it is, this
   is historically the way how the operator/hook works and it's not easy to change without breaking
   backwards compatibility because there are other operators build on top of the ``SimpleHttpOperator`` that
   rely on that functionality and there are many users using it already.


In the first example we are calling a ``POST`` with json data and succeed when we get the same json data back
otherwise the task will fail.

.. exampleinclude:: /../../tests/system/providers/http/example_http.py
    :language: python
    :start-after: [START howto_operator_http_task_post_op]
    :end-before: [END howto_operator_http_task_post_op]

Here we are calling a ``GET`` request and pass params to it. The task will succeed regardless of the response text.

.. exampleinclude:: /../../tests/system/providers/http/example_http.py
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

.. exampleinclude:: /../../tests/system/providers/http/example_http.py
    :language: python
    :start-after: [START howto_operator_http_task_get_op_response_filter]
    :end-before: [END howto_operator_http_task_get_op_response_filter]

In the third example we are performing a ``PUT`` operation to put / set data according to the data that is being
provided to the request.

.. exampleinclude:: /../../tests/system/providers/http/example_http.py
    :language: python
    :start-after: [START howto_operator_http_task_put_op]
    :end-before: [END howto_operator_http_task_put_op]

In this example we call a ``DELETE`` operation to the ``delete`` endpoint. This time we are passing form data to the
request.

.. exampleinclude:: /../../tests/system/providers/http/example_http.py
    :language: python
    :start-after: [START howto_operator_http_task_del_op]
    :end-before: [END howto_operator_http_task_del_op]

Here we pass form data to a ``POST`` operation which is equal to a usual form submit.

.. exampleinclude:: /../../tests/system/providers/http/example_http.py
    :language: python
    :start-after: [START howto_operator_http_task_post_op_formenc]
    :end-before: [END howto_operator_http_task_post_op_formenc]
