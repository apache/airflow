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

How to create your own provider
-------------------------------

Custom provider packages
''''''''''''''''''''''''

You can develop and release your own providers. Your custom operators, hooks, sensors, transfer operators
can be packaged together in a standard airflow package and installed using the same mechanisms.
Moreover they can also use the same mechanisms to extend the Airflow Core with auth backends,
custom connections, logging, secret backends and extra operator links as described in the previous chapter.

As mentioned in the `Providers <http://airflow.apache.org/docs/apache-airflow-providers/index.html>`_
documentation, custom providers can extend Airflow core - they can add extra links to operators as well
as custom connections. You can use build your own providers and install them as packages if you would like
to use the mechanism for your own, custom providers.

How to create a provider
''''''''''''''''''''''''

Adding a provider to Airflow is just a matter of building a Python package and adding the right meta-data to
the package. We are using standard mechanism of python to define
`entry points <https://docs.python.org/3/library/importlib.metadata.html#entry-points>`_ . Your package
needs to define appropriate entry-point ``apache_airflow_provider`` which has to point to a callable
implemented by your package and return a dictionary containing the list of discoverable capabilities
of your package. The dictionary has to follow the
`json-schema specification <https://github.com/apache/airflow/blob/main/airflow/provider_info.schema.json>`_.

Most of the schema provides extension point for the documentation (which you might want to also use for
your own purpose) but the important fields from the extensibility point of view are those:

Displaying package information in CLI/API:

* ``package-name`` - Name of the package for the provider.

* ``name`` - Human-friendly name of the provider.

* ``description`` - Additional description of the provider.

* ``version`` - List of versions of the package (in reverse-chronological order). The first version in the
  list is the current package version. It is taken from the version of package installed, not from the
  provider_info information.

Exposing customized functionality to the Airflow's core:

* ``extra-links`` - this field should contain the list of all the operator class names that are adding extra links
  capability. See :doc:`apache-airflow:howto/define-extra-link` for description of how to add extra link
  capability to the operators of yours.

* ``connection-types`` - this field should contain the list of all the connection types together with hook
  class names implementing those custom connection types (providing custom extra fields and
  custom field behaviour). This field is available as of Airflow 2.2.0 and it replaces deprecated
  ``hook-class-names``. See :doc:`apache-airflow:howto/connection` for more details.

* ``secret-backends`` - this field should contain the list of all the secret backends class names that the
  provider provides. See :doc:`apache-airflow:security/secrets/secrets-backend/index` for description of how
  to add.

* ``task-decorators`` - this field should contain the list of dictionaries of name/path where the decorators
  are available. See :doc:`apache-airflow:howto/create-custom-decorator` for description of how to add
  custom decorators.

* ``logging`` - this field should contain the list of all the logging handler class names that the
  provider provides. See :doc:`apache-airflow:administration-and-deployment/logging-monitoring/logging-tasks`
  for description of the logging handlers.

* ``auth-backends`` - this field should contain the authentication backend module names for API/UI.
  See :doc:`apache-airflow:security/api` for description of the auth backends.

* ``notifications`` - this field should contain the notification classes.
  See :doc:`apache-airflow:howto/notifications` for description of the notifications.

* ``executors`` - this field should contain the executor class names.
  See :doc:`apache-airflow:core-concepts/executor/index` for description of the executors.

* ``config`` - this field should contain dictionary that should conform to the
  ``airflow/config_templates/config.yml.schema.json`` with configuration contributed by the providers
  See :doc:`apache-airflow:howto/set-config` for details about setting configuration.

.. note:: Deprecated values

  * ``hook-class-names`` (deprecated) - this field should contain the list of all hook class names that provide
    custom connection types with custom extra fields and field behaviour. The ``hook-class-names`` array
    is deprecated as of Airflow 2.2.0 (for optimization reasons) and will be removed in Airflow 3. If your
    providers are targeting Airflow 2.2.0+ you do not have to include the ``hook-class-names`` array, if
    you want to also target earlier versions of Airflow 2, you should include both ``hook-class-names`` and
    ``connection-types`` arrays. See :doc:`apache-airflow:howto/connection` for more details.


When your providers are installed you can query the installed providers and their capabilities with the
``airflow providers`` command. This way you can verify if your providers are properly recognized and whether
they define the extensions properly. See :doc:`apache-airflow:cli-and-env-variables-ref` for details of available CLI
sub-commands.

When you write your own provider, consider following the
`Naming conventions for provider packages <https://github.com/apache/airflow/blob/main/contributing-docs/11_provider_packages.rst#naming-conventions-for-provider-packages>`_

Special considerations
''''''''''''''''''''''

Optional provider features
--------------------------

  .. versionadded:: 2.3.0

    This feature is available in Airflow 2.3+.

Some providers might provide optional features, which are only available when some packages or libraries
are installed. Such features will typically result in ``ImportErrors``; however, those import errors
should be silently ignored rather than pollute the logs of Airflow with false warnings. False warnings
are a very bad pattern, as they tend to turn into blind spots, so avoiding false warnings is encouraged.
However, until Airflow 2.3, Airflow had no mechanism to selectively ignore "known" ImportErrors. So
Airflow 2.1 and 2.2 silently ignored all ImportErrors coming from providers with actually lead to
ignoring even important import errors - without giving the clue to Airflow users that there is something
missing in provider dependencies.

Using Providers with dynamic task mapping
-----------------------------------------

Airflow 2.3 added `Dynamic Task Mapping <https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-42+Dynamic+Task+Mapping>`_
and it added the possibility of assigning a unique key to each task. Which means that when such dynamically
mapped task wants to retrieve a value from XCom (for example in case an extra link should calculated)
it should always check if the ti_key value passed is not None an only then retrieve the XCom value using
XCom.get_value. This allows to keep backwards compatibility with earlier versions of Airflow.

Typical code to access XCom Value in providers that want to keep backwards compatibility should look similar to
this (note the ``if ti_key is not None:`` condition).

  .. code-block:: python

    def get_link(
        self,
        operator: BaseOperator,
        dttm: datetime | None = None,
        ti_key: "TaskInstanceKey" | None = None,
    ):
        if ti_key is not None:
            job_ids = XCom.get_value(key="job_id", ti_key=ti_key)
        else:
            assert dttm is not None
            job_ids = XCom.get_one(
                key="job_id",
                dag_id=operator.dag.dag_id,
                task_id=operator.task_id,
                execution_date=dttm,
            )
        if not job_ids:
            return None
        if len(job_ids) < self.index:
            return None
        job_id = job_ids[self.index]
        return BIGQUERY_JOB_DETAILS_LINK_FMT.format(job_id=job_id)




FAQ for custom providers
''''''''''''''''''''''''

**When I write my own provider, do I need to do anything special to make it available to others?**

You do not need to do anything special besides creating the ``apache_airflow_provider`` entry point
returning properly formatted meta-data  - dictionary with ``extra-links`` and ``connection-types`` fields
(and deprecated ``hook-class-names`` field if you are also targeting versions of Airflow before 2.2.0).

Anyone who runs airflow in an environment that has your Python package installed will be able to use the
package as a provider package.


**Should I name my provider specifically or should it be created in ``airflow.providers`` package?**

We have quite a number (>80) of providers managed by the community and we are going to maintain them
together with Apache Airflow. All those providers have well-defined structured and follow the
naming conventions we defined and they are all in ``airflow.providers`` package. If your intention is
to contribute your provider, then you should follow those conventions and make a PR to Apache Airflow
to contribute to it. But you are free to use any package name as long as there are no conflicts with other
names, so preferably choose package that is in your "domain".

**What do I need to do to turn a package into a provider?**

You need to do the following to turn an existing Python package into a provider (see below for examples):

* Add the ``apache_airflow_provider`` entry point in the ``pyproject.toml`` file - this tells airflow
  where to get the required provider metadata
* Create the function that you refer to in the first step as part of your package: this functions returns a
  dictionary that contains all meta-data about your provider package
* If you want Airflow to link to documentation of your Provider in the providers page, make sure
  to add "project-url/documentation" `metadata <https://peps.python.org/pep-0621/#example>`_ to your package.
  This will also add link to your documentation in PyPI.
* note that the dictionary should be compliant with ``airflow/provider_info.schema.json`` JSON-schema
  specification. The community-managed providers have more fields there that are used to build
  documentation, but the requirement for runtime information only contains several fields which are defined
  in the schema:

.. exampleinclude:: /../../airflow/provider_info.schema.json
    :language: json

Example ``pyproject.toml``:

.. code-block:: toml

   [project.entry-points."apache_airflow_provider"]
   provider_info = "airflow.providers.myproviderpackage.get_provider_info:get_provider_info"


Example ``myproviderpackage/get_provider_info.py``:

.. code-block:: Python

   def get_provider_info():
       return {
           "package-name": "my-package-name",
           "name": "name",
           "description": "a description",
           "hook-class-names": [
               "myproviderpackage.hooks.source.SourceHook",
           ],
       }


**Is there a convention for a connection id and type?**

Very good question. Glad that you asked. We usually follow the convention ``<NAME>_default`` for connection
id and just ``<NAME>`` for connection type. Few examples:

* ``google_cloud_default`` id and ``google_cloud_platform`` type
* ``aws_default`` id and ``aws`` type

You should follow this convention. It is important, to use unique names for connection type,
so it should be unique for your provider. If two providers try to add connection with the same type
only one of them will succeed.

**Can I contribute my own provider to Apache Airflow?**

The answer depends on the provider. We have a policy for that in the
`PROVIDERS.rst <https://github.com/apache/airflow/blob/main/PROVIDERS.rst>`_ developer documentation.

**Can I advertise my own provider to Apache Airflow users and share it with others as package in PyPI?**

Absolutely! We have an `Ecosystem <https://airflow.apache.org/ecosystem/>`_ area on our website where
we share non-community managed extensions and work for Airflow. Feel free to make a PR to the page and
add we will evaluate and merge it when we see that such provider can be useful for the community of
Airflow users.

**Can I charge for the use of my provider?**

This is something that is outside of our control and domain. As an Apache project, we are
commercial-friendly and there are many businesses built around Apache Airflow and many other
Apache projects. As a community, we provide all the software for free and this will never
change. What 3rd-party developers are doing is not under control of Apache Airflow community.
