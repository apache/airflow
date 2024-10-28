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

.. _howto/operator:GithubOperator:

Operators
=========

Use the :class:`~airflow.providers.github.operators.github.GithubOperator` to execute
Operations in a `GitHub <https://www.github.com/>`__.

You can build your own operator using :class:`~airflow.providers.github.operators.github.GithubOperator`
and passing :class:`github_method <airflow.providers.github.operators.github.GithubOperator>`
and :class:`github_method_args <airflow.providers.github.operators.github.GithubOperator>`
from top level `PyGithub <https://pygithub.readthedocs.io/>`__ methods.

You can further process the result using
:class:`result_processor <airflow.providers.github.operators.github.GithubOperator>` Callable as you like.

An example of Listing all Repositories owned by a user, **client.get_user().get_repos()** can be implemented as following:

.. exampleinclude:: /../../providers/tests/system/github/example_github.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_list_repos_github]
    :end-before: [END howto_operator_list_repos_github]



An example of Listing Tags in a Repository, **client.get_repo(full_name_or_id='apache/airflow').get_tags()** can be implemented as following:

.. exampleinclude:: /../../providers/tests/system/github/example_github.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_list_tags_github]
    :end-before: [END howto_operator_list_tags_github]


Sensors
========

You can build your own sensor  using :class:`~airflow.providers.github.sensors.GithubSensor`,

You can also implement your own sensor on Repository using :class:`~airflow.providers.github.sensors.BaseGithubRepositorySensor`,
an example of this is :class:`~airflow.providers.github.sensors.GithubTagSensor`


Use the :class:`~airflow.providers.github.sensors.github.GithubTagSensor` to wait for creation of
a Tag in `GitHub <https://www.github.com/>`__.

An example for tag **v1.0**:

.. exampleinclude:: /../../providers/tests/system/github/example_github.py
    :language: python
    :dedent: 4
    :start-after: [START howto_tag_sensor_github]
    :end-before: [END howto_tag_sensor_github]

Similar Functionality can be achieved by directly using
:class:`~from airflow.providers.github.sensors.github.GithubSensor`.

.. exampleinclude:: /../../providers/tests/system/github/example_github.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_github]
    :end-before: [END howto_sensor_github]
