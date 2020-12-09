
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


Q&A for Airflow and Providers
'''''''''''''''''''''''''''''

Upgrading Airflow 2.0 and Providers
"""""""""""""""""""""""""""""""""""

Q. **When upgrading to a new Airflow version such as 2.0, but possibly 2.0.1 and beyond, is the best practice
   to also upgrade provider packages at the same time?**

A. It depends on your use case. If you have automated or semi-automated verification of your installation,
   that you can run a new version of Airflow including all provider packages, then definitely go for it.
   If you rely more on manual testing, it is advised that you upgrade in stages. Depending on your choice
   you can either upgrade all used provider packages first, and then upgrade Airflow Core or the other way
   round. The first approach - when you first upgrade all providers is probably safer, as you can do it
   incrementally, step-by-step replacing provider by provider in your environment.

Using Backport Providers in Airflow 1.10
""""""""""""""""""""""""""""""""""""""""

Q. **I have an Airflow version (1.10.12) running and it is stable. However, because of a Cloud provider change,
   I would like to upgrade the provider package. If I don't need to upgrade the Airflow version anymore,
   how do I know that this provider version is compatible with my Airflow version?**


A. Backport Provider Packages (those are needed in 1.10.* Airflow series) are going to be released for
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

Q. **I have an older version of my provider package which we have lightly customized and is working
   fine with my MSSQL installation. I am upgrading my Airflow version. Do I need to upgrade my provider,
   or can I keep it as it is.**

A. It depends on the scope of customization. There is no need to upgrade the provider packages to later
   versions unless you want to upgrade to Airflow version that introduces backwards-incompatible changes.
   Generally speaking, with Airflow 2 we are following the `Semver <https://semver.org/>`_  approach where
   we will introduce backwards-incompatible changes in Major releases, so all your modifications (as long
   as you have not used internal Airflow classes) should work for All Airflow 2.* versions.


Content
-------

.. toctree::
    :maxdepth: 1

    Packages <packages-ref>
    Operators and hooks <operators-and-hooks-ref/index>
