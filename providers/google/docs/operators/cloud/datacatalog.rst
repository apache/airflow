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



Google Cloud Data Catalog Operators
=======================================
.. _datacatalog-deprecation-warning:
.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog.
    The Data Catalog operators replacement can be found at `airflow.providers.google.cloud.operators.dataplex`
    For further understanding please refer to the `official guide <https://cloud.google.com/dataplex/docs/transition-to-dataplex-catalog>`__.
    Mapping between entities from Data Catalog and Dataplex Universal Catalog presented in table
    `Mapping between Data Catalog and Dataplex Universal Catalog <https://cloud.google.com/dataplex/docs/transition-to-dataplex-catalog#preparatory-phase>`__
    under `Learn more about simultaneous availability of Data Catalog metadata in Dataplex Universal Catalog` block.

The `Data Catalog <https://cloud.google.com/data-catalog>`__ is a fully managed and scalable metadata
management service that allows organizations to quickly discover, manage and understand all their data in
Google Cloud. It offers:

* A simple and easy to use search interface for data discovery, powered by the same Google search technology that
  supports Gmail and Drive
* A flexible and powerful cataloging system for capturing technical and business metadata
* An auto-tagging mechanism for sensitive data with DLP API integration

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst


.. _howto/operator:CloudDataCatalogEntryOperators:

Managing an entries
^^^^^^^^^^^^^^^^^^^

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogGetEntryOperator` or
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogLookupEntryOperator`.
    For more information please check this :ref:`section <datacatalog-deprecation-warning>`.

Operators uses a :class:`~google.cloud.datacatalog_v1beta1.types.Entry` for representing entry

.. contents::
  :depth: 1
  :local:

.. _howto/operator:CloudDataCatalogLookupEntryOperator:
.. _howto/operator:CloudDataCatalogGetEntryOperator:

Getting an entry
""""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogGetEntryOperator` or
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogLookupEntryOperator`.
    For more information please check this :ref:`section <datacatalog-deprecation-warning>`.

Getting an entry is performed with the
:class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogGetEntryOperator` and
:class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogLookupEntryOperator`
operators.

The ``CloudDataCatalogGetEntryOperator`` use Project ID, Entry Group ID, Entry ID to get the entry.

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogGetEntryOperator`
parameters which allows you to dynamically determine values.

The result is saved to :ref:`XCom <concepts:xcom>`, which allows it to be used by other operators.

The ``CloudDataCatalogLookupEntryOperator`` use the resource name to get the entry.

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogLookupEntryOperator`
parameters which allows you to dynamically determine values.

The result is saved to :ref:`XCom <concepts:xcom>`, which allows it to be used by other operators.

.. _howto/operator:CloudDataCatalogCreateEntryOperator:

Creating an entry
"""""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogCreateEntryOperator`.
    For more information please check this :ref:`section <datacatalog-deprecation-warning>`.

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateEntryOperator`
operator create the entry.

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateEntryOperator`
parameters which allows you to dynamically determine values.

The result is saved to :ref:`XCom <concepts:xcom>`, which allows it to be used by other operators.

The newly created entry ID can be read with the ``entry_id`` key.

.. _howto/operator:CloudDataCatalogUpdateEntryOperator:

Updating an entry
"""""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogUpdateEntryOperator`.
    For more information please check this :ref:`section <datacatalog-deprecation-warning>`.

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogUpdateEntryOperator`
operator update the entry.

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogUpdateEntryOperator`
parameters which allows you to dynamically determine values.

.. _howto/operator:CloudDataCatalogDeleteEntryOperator:

Deleting a entry
""""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogDeleteEntryOperator`.
    For more information please check this :ref:`section <datacatalog-deprecation-warning>`.

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteEntryOperator`
operator delete the entry.

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteEntryOperator`
parameters which allows you to dynamically determine values.

.. _howto/operator:CloudDataCatalogEntryGroupOperators:

Managing a entry groups
^^^^^^^^^^^^^^^^^^^^^^^

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogCreateEntryGroupOperator`.
    For more information please check this :ref:`section <datacatalog-deprecation-warning>`.

Operators uses a :class:`~google.cloud.datacatalog_v1beta1.types.Entry` for representing a entry groups.

.. contents::
  :depth: 1
  :local:

.. _howto/operator:CloudDataCatalogCreateEntryGroupOperator:

Creating an entry group
"""""""""""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogCreateEntryGroupOperator`.
    For more information please check this :ref:`section <datacatalog-deprecation-warning>`.

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateEntryGroupOperator`
operator create the entry group.

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateEntryGroupOperator`
parameters which allows you to dynamically determine values.

The result is saved to :ref:`XCom <concepts:xcom>`, which allows it to be used by other operators.

The newly created entry group ID can be read with the ``entry_group_id`` key.

.. _howto/operator:CloudDataCatalogGetEntryGroupOperator:

Getting an entry group
""""""""""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogGetEntryGroupOperator`.
    For more information please check this :ref:`section <datacatalog-deprecation-warning>`.

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogGetEntryGroupOperator`
operator get the entry group.

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogGetEntryGroupOperator`
parameters which allows you to dynamically determine values.

The result is saved to :ref:`XCom <concepts:xcom>`, which allows it to be used by other operators.

.. _howto/operator:CloudDataCatalogDeleteEntryGroupOperator:

Deleting an entry group
"""""""""""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogDeleteEntryGroupOperator`.
    For more information please check this :ref:`section <datacatalog-deprecation-warning>`.

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteEntryGroupOperator`
operator delete the entry group.

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteEntryGroupOperator`
parameters which allows you to dynamically determine values.

.. _howto/operator:CloudDataCatalogTagTemplateOperators:

Managing tag templates
^^^^^^^^^^^^^^^^^^^^^^

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogCreateAspectTypeOperator`.
    For more information please check this :ref:`section <datacatalog-deprecation-warning>`.

Operators uses a :class:`~google.cloud.datacatalog_v1beta1.types.TagTemplate` for representing a tag templates.

.. contents::
  :depth: 1
  :local:

.. _howto/operator:CloudDataCatalogCreateTagTemplateOperator:

Creating a tag template
"""""""""""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogCreateAspectTypeOperator`.
    For more information please check this :ref:`section <datacatalog-deprecation-warning>`.

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateTagTemplateOperator`
operator get the tag template.

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateTagTemplateOperator`
parameters which allows you to dynamically determine values.

The result is saved to :ref:`XCom <concepts:xcom>`, which allows it to be used by other operators.

The newly created tag template ID can be read with the ``tag_template_id`` key.

.. _howto/operator:CloudDataCatalogDeleteTagTemplateOperator:

Deleting a tag template
"""""""""""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogDeleteAspectTypeOperator`.
    For more information please check this :ref:`section <datacatalog-deprecation-warning>`.

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteTagTemplateOperator`
operator delete the tag template.

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteTagTemplateOperator`
parameters which allows you to dynamically determine values.


.. _howto/operator:CloudDataCatalogGetTagTemplateOperator:

Getting a tag template
""""""""""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogGetAspectTypeOperator`.
    For more information please check this :ref:`section <datacatalog-deprecation-warning>`.

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogGetTagTemplateOperator`
operator get the tag template.

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogGetTagTemplateOperator`
parameters which allows you to dynamically determine values.

The result is saved to :ref:`XCom <concepts:xcom>`, which allows it to be used by other operators.

.. _howto/operator:CloudDataCatalogUpdateTagTemplateOperator:

Updating a tag template
"""""""""""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogUpdateAspectTypeOperator`.
    For more information please check this :ref:`section <datacatalog-deprecation-warning>`.

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogUpdateTagTemplateOperator`
operator update the tag template.

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogUpdateTagTemplateOperator`
parameters which allows you to dynamically determine values.

.. _howto/operator:CloudDataCatalogTagOperators:

Managing tags
^^^^^^^^^^^^^

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogCreateEntryOperator` or
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogUpdateEntryOperator`.
    For more information please check this :ref:`section <datacatalog-deprecation-warning>`.

Operators uses a :class:`~google.cloud.datacatalog_v1beta1.types.Tag` for representing a tag.

.. contents::
  :depth: 1
  :local:

.. _howto/operator:CloudDataCatalogCreateTagOperator:

Creating a tag on an entry
""""""""""""""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogCreateEntryOperator` or
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogUpdateEntryOperator`.
    For more information please check this :ref:`section <datacatalog-deprecation-warning>`.

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateTagOperator`
operator get the tag template.

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateTagOperator`
parameters which allows you to dynamically determine values.

The result is saved to :ref:`XCom <concepts:xcom>`, which allows it to be used by other operators.

The newly created tag ID can be read with the ``tag_id`` key.

.. _howto/operator:CloudDataCatalogUpdateTagOperator:

Updating a tag
""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogUpdateEntryOperator`.
    For more information please check this :ref:`section <datacatalog-deprecation-warning>`.

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogUpdateTagOperator`
operator update the tag template.

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogUpdateTagOperator`
parameters which allows you to dynamically determine values.

.. _howto/operator:CloudDataCatalogDeleteTagOperator:

Deleting a tag
""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogUpdateEntryOperator`.
    For more information please check this :ref:`section <datacatalog-deprecation-warning>`.

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteTagOperator`
operator delete the tag template.

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteTagOperator`
parameters which allows you to dynamically determine values.

.. _howto/operator:CloudDataCatalogListTagsOperator:

Listing tags on an entry
""""""""""""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogGetEntryOperator`.
    For more information please check this :ref:`section <datacatalog-deprecation-warning>`.

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogListTagsOperator`
operator get list of the tags on the entry.

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogListTagsOperator`
parameters which allows you to dynamically determine values.

The result is saved to :ref:`XCom <concepts:xcom>`, which allows it to be used by other operators.

.. _howto/operator:CloudDataCatalogTagTemplateFieldssOperators:

Managing a tag template fields
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogUpdateAspectTypeOperator` or
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogCreateAspectTypeOperator`.
    For more information please check this :ref:`section <datacatalog-deprecation-warning>`.

Operators uses a :class:`~google.cloud.datacatalog_v1beta1.types.TagTemplateField` for representing a tag template fields.

.. contents::
  :depth: 1
  :local:

.. _howto/operator:CloudDataCatalogCreateTagTemplateFieldOperator:

Creating a field
""""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogUpdateAspectTypeOperator` or
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogCreateAspectTypeOperator`.
    For more information please check this :ref:`section <datacatalog-deprecation-warning>`.

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateTagTemplateFieldOperator`
operator get the tag template field.

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateTagTemplateFieldOperator`
parameters which allows you to dynamically determine values.

The result is saved to :ref:`XCom <concepts:xcom>`, which allows it to be used by other operators.

The newly created field ID can be read with the ``tag_template_field_id`` key.

.. _howto/operator:CloudDataCatalogRenameTagTemplateFieldOperator:

Renaming a field
""""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogUpdateAspectTypeOperator`.
    For more information please check this :ref:`section <datacatalog-deprecation-warning>`.

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogRenameTagTemplateFieldOperator`
operator rename the tag template field.

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogRenameTagTemplateFieldOperator`
parameters which allows you to dynamically determine values.

.. _howto/operator:CloudDataCatalogUpdateTagTemplateFieldOperator:

Updating a field
""""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogUpdateAspectTypeOperator`.
    For more information please check this :ref:`section <datacatalog-deprecation-warning>`.

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogUpdateTagTemplateFieldOperator`
operator get the tag template field.

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogUpdateTagTemplateFieldOperator`
parameters which allows you to dynamically determine values.


.. _howto/operator:CloudDataCatalogDeleteTagTemplateFieldOperator:

Deleting a field
""""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogUpdateAspectTypeOperator`.
    For more information please check this :ref:`section <datacatalog-deprecation-warning>`.

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteTagTemplateFieldOperator`
operator delete the tag template field.

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteTagTemplateFieldOperator`
parameters which allows you to dynamically determine values.


.. _howto/operator:CloudDataCatalogSearchCatalogOperator:

Search resources
""""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogSearchEntriesOperator`.
    For more information please check this :ref:`section <datacatalog-deprecation-warning>`.

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogSearchCatalogOperator`
operator searches Data Catalog for multiple resources like entries, tags that match a query.

The ``query`` parameters should defined using `search syntax <https://cloud.google.com/data-catalog/docs/how-to/search-reference>`__.

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogSearchCatalogOperator`
parameters which allows you to dynamically determine values.

The result is saved to :ref:`XCom <concepts:xcom>`, which allows it to be used by other operators.

Reference
^^^^^^^^^

For further information, look at:

* `Client Library Documentation <https://googleapis.dev/python/datacatalog/latest/index.html>`__
* `Product Documentation <https://cloud.google.com/data-catalog/docs/>`__
