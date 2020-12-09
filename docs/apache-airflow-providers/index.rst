
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

Provider packages
-----------------

.. contents:: :local:

Provider packages context
'''''''''''''''''''''''''

Unlike Apache Airflow 1.10, the Airflow 2.0 is delivered in multiple, separate, but connected packages.
The core of Airflow scheduling system is delivered as ``apache-airflow`` package and there are around
60 providers packages which can be installed separately as so called "Airflow Provider packages".
Those provider packages are separated per-provider (for example ``amazon``, ``google``, ``salesforce``
etc.)  Those packages are available as ``apache-airflow-providers`` packages - separately per each provider
(for example there is an ``apache-airflow-providers-amazon`` or ``apache-airflow-providers-google`` package.

You can install those provider packages separately in order to interface with a given provider. For those
providers that have corresponding extras, the provider packages (latest version from PyPI) are installed
automatically when Airflow is installed with the extra.

Providers are released and versioned separately from the Airflow releases. We are following the
`Semver <https://semver.org/>`_ versioning scheme for the packages. Some versions of the provider
packages might depend on particular versions of Airflow, but the general approach we have is that unless
there is a good reason, new version of providers should work with recent versions of Airflow 2.x. Details
will vary per-provider and if there is a limitation for particular version of particular provider,
constraining the Airflow version used, it will be included as limitation of dependencies in the provider
package.

Some of the providers have cross-provider dependencies as well. Those are not required dependencies, they
might simply enable certain features (for example transfer operators often create dependency between
different providers. Again, the general approach here is that the providers are backwards compatible,
including cross-dependencies. Any kind of breaking changes and requirements on particular versions of other
provider packages are automatically documented in the release notes of every provider.

.. note::
    We also provide ``apache-airflow-backport-providers`` packages that can be installed for Airflow 1.10.
    Those are the same providers as for 2.0 but automatically back-ported to work for Airflow 1.10. Those
    backport providers are going to be updated and released for 3 months after Apache Airflow 2.0 release.

Provider packages functionality
'''''''''''''''''''''''''''''''

Separate provider packages provide the possibilities that were not available in 1.10:

1. You can upgrade to latest version of particular providers without the need of Apache Airflow core upgrade.

2. You can downgrade to previous version of particular provider in case the new version introduces
   some problems, without impacting the main Apache Airflow core package.

3. You can release and upgrade/downgrade provider packages incrementally, independent from each other. This
   means that you can incrementally validate each of the provider package update in your environment,
   following the usual tests you have in your environment.


Extending Airflow Connections and Extra links via Providers
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

Providers, can not only deliver operators, hooks, sensor, transfer operators to communicate with
multitude of external systems, but they can also extend Airflow. Airflow has several extension capabilities
that can be used by providers. Airflow automatically discovers, which providers add those additional
capabilities and once you install provider package and re-start Airflow, those are becoming automatically
available to Airflow Users.

The capabilities are:

* Adding Extra Links to operators delivered by the provider.
  See :doc:`apache-airflow:howto/define_extra_link`
  for description of what extra links are and examples of provider registering an operator with extra links

* Adding custom connection types, extending connection form and handling custom form field behaviour for the
  connections defined by the provider. See :doc:`apache-airflow:howto/connection` for description of
  connection and what capabilities of custom connection you can define.

How to create your own provider
"""""""""""""""""""""""""""""""

Adding provider to Airflow is just a matter of building a Python package and adding the right meta-data to
the package. We are using standard mechanism of python to define
`entry points <https://docs.python.org/3/library/importlib.metadata.html#entry-points>`_ . Your package
needs to define appropriate entry-point ``apache_airflow_provider`` which has to point to a callable
implemented by your package and return a dictionary containing the list of discoverable capabilities
of your package. The dictionary has to follow the
`json-schema specification <https://github.com/apache/airflow/blob/master/airflow/provider.yaml.schema.json>`_.

Most of the schema provides extension point for the documentation (which you might want to also use for
your own purpose) but the two important fields from the extensibility point of view are:

* ``extra-links`` - this field should contain the list of all operator class names that are adding extra links
  capability. See :doc:`apache-airflow:howto/define_extra_link` for description of how to add extra link
  capability to the operators of yours.

* ``hook-class-names`` - this field should contain the list of all hook class names that provide
  custom connection types with custom extra fields and field behaviour. See
  :doc:`apache-airflow:howto/connection` for more details.


When your providers are installed you can query the installed providers and their capabilities with
``airflow providers`` command. This way you can verify if your providers are properly recognized and whether
they define the extensions properly. See :doc:`cli-and-env-variables-ref` for details of available CLI
sub-commands.

When you write your own provider, consider following the
`Naming conventions for provider packages <https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst#naming-conventions-for-provider-packages>`_


Q&A for Airflow and Providers
'''''''''''''''''''''''''''''

Upgrading Airflow 2.0 and Providers
"""""""""""""""""""""""""""""""""""

**When upgrading to a new Airflow version such as 2.0, but possibly 2.0.1 and beyond, is the best practice
to also upgrade provider packages at the same time?**

It depends on your use case. If you have automated or semi-automated verification of your installation,
that you can run a new version of Airflow including all provider packages, then definitely go for it.
If you rely more on manual testing, it is advised that you upgrade in stages. Depending on your choice
you can either upgrade all used provider packages first, and then upgrade Airflow Core or the other way
round. The first approach - when you first upgrade all providers is probably safer, as you can do it
incrementally, step-by-step replacing provider by provider in your environment.

Using Backport Providers in Airflow 1.10
""""""""""""""""""""""""""""""""""""""""

**I have an Airflow version (1.10.12) running and it is stable. However, because of a Cloud provider change,
I would like to upgrade the provider package. If I don't need to upgrade the Airflow version anymore,
how do I know that this provider version is compatible with my Airflow version?**


Backport Provider Packages (those are needed in 1.10.* Airflow series) are going to be released for
3 months after the release. We will stop releasing new updates to the backport providers afterwards.
You will be able to continue using the provider packages that you already use and unless you need to
get some new release of the provider that is only released for 2.0, there is no need to upgrade
Airflow. This might happen if for example the provider is migrated to use newer version of client
libraries or when new features/operators/hooks are added to it. Those changes will only be
backported to 1.10.* compatible backport providers up to 3 months after releasing Airflow 2.0.
Also we expect more providers, changes and fixes added to the existing providers to come after the
3 months pass. Eventually you will have to upgrade to Airflow 2.0 if you would like to make use of those.
When it comes to compatibility of providers with different Airflow 2 versions, each
provider package will keep its own dependencies, and while we expect those providers to be generally
backwards-compatible, particular versions of particular providers might introduce dependencies on
specific Airflow versions.

Customizing Provider Packages
"""""""""""""""""""""""""""""

**I have an older version of my provider package which we have lightly customized and is working
fine with my MSSQL installation. I am upgrading my Airflow version. Do I need to upgrade my provider,
or can I keep it as it is?**

It depends on the scope of customization. There is no need to upgrade the provider packages to later
versions unless you want to upgrade to Airflow version that introduces backwards-incompatible changes.
Generally speaking, with Airflow 2 we are following the `Semver <https://semver.org/>`_  approach where
we will introduce backwards-incompatible changes in Major releases, so all your modifications (as long
as you have not used internal Airflow classes) should work for All Airflow 2.* versions.


Creating your own providers
"""""""""""""""""""""""""""

**When I write my own provider, do I need to do anything special to make it available to others?**

You do not need to do anything special besides creating the ``apache_airflow_provider`` entry point
returning properly formatted meta-data (dictionary with ``extra-links`` and ``hook-class-names`` fields.


**Should I named my provider specifically or should it be created in ``airflow.providers`` package?**

We have quite a number (>70) of providers managed by the community and we are going to maintain them
together with Apache Airflow. All those providers have well-defined structured and follow the
naming conventions we defined and they are all in ``airflow.providers`` package. If your intention is
to contribute your provider, then you should follow those conventions and make a PR to Apache Airflow
to contribute to it. But you are free to use any package name as long as there are no conflicts with other
names,so preferably choose package that is in your "domain".

**Is there a convention for a connection id and type?**

Very good question. Glad that you asked. We usually follow the convention ``<NAME>_default`` for connection
id and just ``<NAME>`` for connection type. Few examples:

* ``google_cloud_default`` id and ``google_cloud_platform`` type
* ``aws_default`` id and ``aws`` type

You should follow this convention. It is important, to use unique names for connection type,
so it should be unique for your provider. If two providers try to add connection with the same type
only one of them will succeed.

**Can I contribute my own provider to Apache Airflow?**

However, community only accepts providers that are generic enough and can be managed
by the community, so we might not always be in the position to accept such contributions. In case you
have your own, specific provider, you are free to use your own structure and package names and to
publish the providers in whatever form you find appropriate.

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


Content
-------

.. toctree::
    :maxdepth: 1

    Packages <packages-ref>
    Operators and hooks <operators-and-hooks-ref/index>
