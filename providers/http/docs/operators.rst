
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

.. exampleinclude:: /../../http/tests/system/http/example_http.py
    :language: python
    :start-after: [START howto_operator_http_http_sensor_check]
    :end-before: [END howto_operator_http_http_sensor_check]

This sensor can also be used in deferrable mode

.. exampleinclude:: /../../http/tests/system/http/example_http.py
    :language: python
    :start-after: [START howto_operator_http_http_sensor_check_deferrable]
    :end-before: [END howto_operator_http_http_sensor_check_deferrable]

.. _howto/operator:HttpOperator:

HttpOperator
------------

Use the :class:`~airflow.providers.http.operators.http.HttpOperator` to call HTTP requests and get
the response text back.

Deferrable mode runs the request in the Triggerer. A Triggerer restart can replay
it, so non-idempotent methods (including the default ``POST``) log a warning.
See :ref:`howto/deferrable:HttpOperator` for the idempotent method set and how
to silence the warning with ``warn_on_non_idempotent=False``.

.. warning:: Configuring ``https`` via HttpOperator is counter-intuitive

   For historical reasons, configuring ``HTTPS`` connectivity via HTTP operator is, well, difficult and
   counter-intuitive. The Operator defaults to ``http`` protocol and you can change the schema used by the
   operator via ``scheme`` connection attribute. However, this field was originally added to connection for
   database type of URIs, where database schemes are set traditionally as first component of URI ``path``.
   Therefore if you want to configure as ``https`` connection via URI, you need to pass ``https`` scheme
   to the HttpOperator. AS stupid as it looks, your connection URI will look like this:
   ``http://your_host:443/https``. Then if you want to use different URL paths in HttpOperator
   you should pass your path as ``endpoint`` parameter when running the task. For example to run a query to
   ``https://your_host:443/my_endpoint`` you need to set the endpoint parameter to ``my_endpoint``.
   Alternatively, if you want, you could also percent-encode the host including the ``https://`` prefix,
   and as long it contains ``://`` (percent-encoded ``%3a%2f%2f``), the first component of the path will
   not be used as scheme. Your URI definition might then look like ``http://https%3a%2f%2fyour_host:443/``.
   In this case, however, the ``path`` will not be used at all - you still need to use ``endpoint``
   parameter in the task if wish to make a request with specific path. As counter-intuitive as it is, this
   is historically the way how the operator/hook works and it's not easy to change without breaking
   backwards compatibility because there are other operators build on top of the ``HttpOperator`` that
   rely on that functionality and there are many users using it already.


In the first example we are calling a ``POST`` with json data and succeed when we get the same json data back
otherwise the task will fail.

.. exampleinclude:: /../../http/tests/system/http/example_http.py
    :language: python
    :start-after: [START howto_operator_http_task_post_op]
    :end-before: [END howto_operator_http_task_post_op]

Here we are calling a ``GET`` request and pass params to it. The task will succeed regardless of the response text.

.. exampleinclude:: /../../http/tests/system/http/example_http.py
    :language: python
    :start-after: [START howto_operator_http_task_get_op]
    :end-before: [END howto_operator_http_task_get_op]

HttpOperator returns the response body as text by default. If you want to modify the response before passing
it on to the next downstream task, use ``response_filter``. The value returned by ``response_filter`` becomes
the task result, which can be pulled by downstream tasks with XCom. This is useful if:

- the API you are consuming returns a large JSON payload and you're interested in a subset of the data
- the API returns data in xml or csv and you want to convert it to JSON
- you're interested in the headers of the response instead of the body

Below is an example of retrieving data from a REST API and only returning the ``status`` query parameter from
the response body.

.. exampleinclude:: /../../http/tests/system/http/example_http.py
    :language: python
    :start-after: [START howto_operator_http_task_get_op_response_filter]
    :end-before: [END howto_operator_http_task_get_op_response_filter]

In the third example we are performing a ``PUT`` operation to put / set data according to the data that is being
provided to the request.

.. exampleinclude:: /../../http/tests/system/http/example_http.py
    :language: python
    :start-after: [START howto_operator_http_task_put_op]
    :end-before: [END howto_operator_http_task_put_op]

In this example we call a ``DELETE`` operation to the ``delete`` endpoint. This time we are passing form data to the
request.

.. exampleinclude:: /../../http/tests/system/http/example_http.py
    :language: python
    :start-after: [START howto_operator_http_task_del_op]
    :end-before: [END howto_operator_http_task_del_op]

Here we pass form data to a ``POST`` operation which is equal to a usual form submit.

.. exampleinclude:: /../../http/tests/system/http/example_http.py
    :language: python
    :start-after: [START howto_operator_http_task_post_op_formenc]
    :end-before: [END howto_operator_http_task_post_op_formenc]

:class:`~airflow.providers.http.operators.http.HttpOperator` can call an endpoint repeatedly when
you pass ``pagination_function``. Every response is kept in memory and returned together, so a long
listing uses more memory than a single call. With a pagination function, the result is a list of
response texts, and ``response_check`` / ``response_filter`` receive that list.

``max_pages`` stops the walk after that many accumulated responses. If the callback still asks for
another page, the operator logs a warning and returns the pages it already has. It does not fail the
task, and it does not keep requesting. The same cap applies when ``deferrable=True``. Leave
``max_pages`` unset to follow the callback until it returns a falsy value, which is the existing
contract.

Example - an API returns a cursor in the JSON body. The callback receives the raw ``requests.Response``
and returns the next request parameters, or nothing when the listing is finished:

.. exampleinclude:: /../../http/tests/system/http/example_http.py
    :language: python
    :start-after: [START howto_operator_http_pagination_function]
    :end-before: [END howto_operator_http_pagination_function]

Built-in pagination
~~~~~~~~~~~~~~~~~~~

Four helpers in ``airflow.providers.http.operators.pagination`` build that callback for the usual
listing shapes. Each one is stateless: the next page comes only from the last response body and the
query string of ``response.url``. A deferred task is rebuilt between pages, so a counter stored on
the callback would reset and repeat a page. The helpers themselves have no page cap; set ``max_pages``
on the operator, including when you write the callback yourself.

* ``offset_pagination`` advances an ``offset`` query parameter by ``page_size``.
* ``page_number_pagination`` advances a page number (page 1 when the URL has none).
* ``cursor_pagination`` sends the maximum value of a record field. Set ``numeric=True`` when the
  values are numeric strings. Text ordering ranks ``"999"`` above ``"1000"``, so the cursor stops
  moving and later ids are never requested.
* ``next_link_pagination`` follows a next URL in the body (``path="page.next"``). A link on another
  host is ignored unless ``allow_other_hosts=True``. The operator still calls the connection host:
  only the link's path and query are kept, so connection credentials are not sent elsewhere. Pass
  ``base_path`` when the connection host already contains a path (``https://api.example.com/v2``)
  and the next URL repeats it, otherwise the hook requests ``/v2/v2/...``.

Offset, page, and cursor are read from the URL query. ``HttpHook`` puts ``data`` in the query string
for ``GET`` and in the body for other methods, so the helpers target GET listings. A cursor that
lives only in a POST body still needs a custom callback.

.. code-block:: python

    from airflow.providers.http.operators.http import HttpOperator
    from airflow.providers.http.operators.pagination import offset_pagination

    get_orders = HttpOperator(
        task_id="get_orders",
        http_conn_id="orders_api",
        method="GET",
        endpoint="/orders",
        data={"offset": 0, "limit": 100, "status": "open"},
        pagination_function=offset_pagination(
            page_size=100,
            records_key="orders",
            has_more_key="has_more",
        ),
        max_pages=50,
        response_filter=lambda responses: [
            order for response in responses for order in response.json()["orders"]
        ],
    )
