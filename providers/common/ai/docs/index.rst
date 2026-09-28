
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

``apache-airflow-providers-common-ai``
##################################################

Run model calls and tool-using agents as Airflow tasks. A task can classify, extract,
summarize or route with any model vendor, or hand a model a set of tools built from your
Airflow connections and let it work. Airflow supplies what a script does not: the API key
comes from a connection, a failed call retries, a run can pause for a person to approve the
output, the result lands in XCom for the next task, and the whole thing runs on a schedule.

Start here
----------

- :doc:`quickstart`: install, connect a vendor, run a two-task Dag and check its output.
- :doc:`use_cases/index`: ten jobs a data team already has, each with the Dag that does it.
- :doc:`model_providers`: which vendors work, and the extra, connection and prefix for each.

This is the Dag the quick start runs. The ``summarize`` task sends the release notes to the
model on the ``pydanticai_default`` connection; ``publish`` receives the answer like any
other upstream result:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_quickstart.py
    :language: python
    :start-after: [START howto_quickstart_llm]
    :end-before: [END howto_quickstart_llm]

Point ``pydanticai_default`` at OpenAI, Anthropic, Google, Bedrock or a self-hosted server
and the Dag does not change. :doc:`concepts` explains the ideas behind the provider in one
page.

When to use this provider
-------------------------

.. list-table::
   :header-rows: 1
   :widths: 40 30 30

   * - Use case
     - Use
     - Package
   * - Portable generation, classification, extraction, branching, or a
       worker-run agent with toolsets
     - ``common.ai``
     - ``apache-airflow-providers-common-ai``
   * - Many prompts through a batch API at half the price, with retry-safe re-attachment
       and results landed on object storage
     - ``common.ai``
     - ``apache-airflow-providers-common-ai`` (:doc:`operators/llm_batch`)
   * - A vendor's native Embeddings or Responses API, or a batch of raw provider request
       bodies (multi-turn, images, non-chat endpoints)
     - The vendor's own provider
     - e.g. :doc:`apache-airflow-providers-openai:index`,
       :doc:`apache-airflow-providers-anthropic:index`,
       :doc:`apache-airflow-providers-cohere:index`
   * - A vendor-managed, server-side agent session (e.g. Anthropic Managed Agents)
     - The vendor's own provider
     - e.g. :doc:`apache-airflow-providers-anthropic:index`

As a rule of thumb: if Airflow should *run* the AI step (and the model should stay
swappable), use ``common.ai``; if the Dag *submits work to* a vendor-managed service and
waits for the result, use that vendor's provider.

.. toctree::
    :titlesonly:
    :hidden:
    :caption: Basics

    Home <self>
    Changelog <changelog>
    Security <security>

.. toctree::
    :titlesonly:
    :hidden:
    :caption: Getting started

    Installation <installation>
    Quick start <quickstart>
    Core concepts <concepts>
    Develop and test locally <local_development>

.. toctree::
    :titlesonly:
    :hidden:
    :caption: Guides

    What you can build <use_cases/index>
    Models and providers <model_providers>
    Operators <operators/index>
    Toolsets <toolsets/index>
    LLM and agent features <features>
    Document and RAG pipelines <rag_pipelines>
    Reliability and operations <operations>

.. toctree::
    :titlesonly:
    :hidden:
    :caption: References

    Example Dags <examples>
    Configuration <configurations-ref>
    Python API <_api/airflow/providers/common/ai/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: System tests

    System Tests <_api/tests/system/common/ai/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Resources

    PyPI Repository <https://pypi.org/project/apache-airflow-providers-common-ai/>
    Installing from sources <installing-providers-from-sources>

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


apache-airflow-providers-common-ai package
------------------------------------------------------

AI/LLM hooks and operators for Airflow pipelines using `pydantic-ai <https://ai.pydantic.dev/>`__.


Release: 0.10.0

Provider package
----------------

This package is for the ``common.ai`` provider.
All classes for this package are included in the ``airflow.providers.common.ai`` python package.

Installation
------------

You can install this package on top of an existing Airflow installation via
``pip install apache-airflow-providers-common-ai``.
For the minimum Airflow version supported, see ``Requirements`` below.

Requirements
------------

The minimum Apache Airflow version supported by this provider distribution is ``3.0.0``.

==========================================  ==================
PIP package                                 Version required
==========================================  ==================
``apache-airflow``                          ``>=3.0.0``
``apache-airflow-providers-common-compat``  ``>=1.15.0``
``apache-airflow-providers-standard``       ``>=1.20.0``
``pydantic-ai-slim``                        ``>=2.33.0``
==========================================  ==================

Optional cross provider package dependencies
--------------------------------------------

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified provider distributions in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

.. code-block:: bash

    pip install apache-airflow-providers-common-ai[common.sql]


============================================================================================================  ==============
Dependent package                                                                                             Extra
============================================================================================================  ==============
`apache-airflow-providers-common-sql <https://airflow.apache.org/docs/apache-airflow-providers-common-sql>`_  ``common.sql``
`apache-airflow-providers-git <https://airflow.apache.org/docs/apache-airflow-providers-git>`_                ``git``
============================================================================================================  ==============

Optional dependencies
---------------------

These extras install optional third-party libraries that enable additional features of the provider.
Install them when installing from PyPI. For example:

.. code-block:: bash

    pip install apache-airflow-providers-common-ai[anthropic]


==============  =======================================================================================================================================
Extra           Dependencies
==============  =======================================================================================================================================
``anthropic``   ``pydantic-ai-slim[anthropic]>=2.33.0``, ``anthropic>=1.0.0``
``bedrock``     ``pydantic-ai-slim[bedrock]>=2.33.0``
``google``      ``pydantic-ai-slim[google]>=2.33.0``
``openai``      ``pydantic-ai-slim[openai]>=2.33.0``, ``openai>=2.47.0``
``typesafe``    ``typesafe-sdk>=0.6.0``
``mcp``         ``pydantic-ai-slim[mcp]>=2.33.0``
``modal``       ``modal>=1.5.0``
``code-mode``   ``pydantic-ai-harness[codemode]>=0.3.0``
``shields``     ``pydantic-ai-shields>=0.3.4``
``skills``      ``apache-airflow-providers-git>=0.4.0``, ``pydantic-ai-skills>=1.2.0``
``avro``        ``fastavro>=1.10.0; python_version < "3.14"``, ``fastavro>=1.12.1; python_version >= "3.14"``
``parquet``     ``pyarrow>=18.0.0; python_version < '3.14'``, ``pyarrow>=22.0.0; python_version >= '3.14'``
``sql``         ``apache-airflow-providers-common-sql>=1.33.0``, ``sqlglot>=30.0.0``
``common.sql``  ``apache-airflow-providers-common-sql>=1.33.0``
``langchain``   ``langchain>=1.0.0``
``llamaindex``  ``dataclasses-json>=0.6.7``, ``llama-index-core>=0.14.5``, ``llama-index-embeddings-openai>=0.6.0``, ``llama-index-llms-openai>=0.6.8``
``pdf``         ``pypdf>=4.0.0``
``docx``        ``python-docx>=1.0.0``
``git``         ``apache-airflow-providers-git``
==============  =======================================================================================================================================

Downloading official packages
-----------------------------

You can download officially released packages and verify their checksums and signatures from the
`Official Apache Download site <https://downloads.apache.org/airflow/providers/>`_

* `The apache-airflow-providers-common-ai 0.10.0 sdist package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_common_ai-0.10.0.tar.gz>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_common_ai-0.10.0.tar.gz.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_common_ai-0.10.0.tar.gz.sha512>`__)
* `The apache-airflow-providers-common-ai 0.10.0 wheel package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_common_ai-0.10.0-py3-none-any.whl>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_common_ai-0.10.0-py3-none-any.whl.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_common_ai-0.10.0-py3-none-any.whl.sha512>`__)
