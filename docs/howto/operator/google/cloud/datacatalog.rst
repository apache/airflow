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

The `Data Catalog <https://cloud.google.com/data-catalog>`__ is a fully managed and scalable metadata
management service that allows organizations to quickly discover, manage and understand all their data in
Google Cloud. It offers:

* A simple and easy to use search interface for data discovery, powered by the same Google search technology that
  supports Gmail and Drive
* A flexible and powerful cataloging system for capturing technical and business metadata
* An auto-tagging mechanism for sensitive data with DLP API integration

.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /howto/operator/google/_partials/prerequisite_tasks.rst


.. _howto/operator:CloudDataCatalogEntryOperators:

Managing an entries
^^^^^^^^^^^^^^^^^^^

Operators uses a :class:`~google.cloud.datacatalog_v1beta1.types.Entry` for representing entry

.. contents::
  :depth: 1
  :local:

.. _howto/operator:CloudDataCatalogLookupEntryOperator:
.. _howto/operator:CloudDataCatalogGetEntryOperator:

Getting an entry
""""""""""""""""

Getting an entry is performed with the
:class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogGetEntryOperator` and
:class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogLookupEntryOperator`
operators.

The ``CloudDataCatalogGetEntryOperator`` use Project ID, Entry Group ID, Entry ID to get the entry.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_datacatalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_datacatalog_get_entry]
    :end-before: [END howto_operator_gcp_datacatalog_get_entry]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogGetEntryOperator`
parameters which allows you to dynamically determine values.

The result is saved to :ref:`XCom <concepts:xcom>`, which allows it to be used by other operators.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_datacatalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_datacatalog_get_entry_result]
    :end-before: [END howto_operator_gcp_datacatalog_get_entry_result]

The ``CloudDataCatalogLookupEntryOperator`` use the resource name to get the entry.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_datacatalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_datacatalog_lookup_entry_linked_resource]
    :end-before: [END howto_operator_gcp_datacatalog_lookup_entry_linked_resource]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogLookupEntryOperator`
parameters which allows you to dynamically determine values.

The result is saved to :ref:`XCom <concepts:xcom>`, which allows it to be used by other operators.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_datacatalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_datacatalog_lookup_entry_result]
    :end-before: [END howto_operator_gcp_datacatalog_lookup_entry_result]

.. _howto/operator:CloudDataCatalogCreateEntryOperator:

Creating an entry
"""""""""""""""""

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateEntryOperator`
operator create the entry.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_datacatalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_datacatalog_create_entry_gcs]
    :end-before: [END howto_operator_gcp_datacatalog_create_entry_gcs]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateEntryOperator`
parameters which allows you to dynamically determine values.

The result is saved to :ref:`XCom <concepts:xcom>`, which allows it to be used by other operators.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_datacatalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_datacatalog_create_entry_gcs_result2]
    :end-before: [END howto_operator_gcp_datacatalog_create_entry_gcs_result2]

The newly created entry ID can be read with the ``entry_id`` key.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_datacatalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_datacatalog_create_entry_gcs_result]
    :end-before: [END howto_operator_gcp_datacatalog_create_entry_gcs_result]

.. _howto/operator:CloudDataCatalogUpdateEntryOperator:

Updating an entry
"""""""""""""""""

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogUpdateEntryOperator`
operator update the entry.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_datacatalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_datacatalog_update_entry]
    :end-before: [END howto_operator_gcp_datacatalog_update_entry]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogUpdateEntryOperator`
parameters which allows you to dynamically determine values.

.. _howto/operator:CloudDataCatalogDeleteEntryOperator:

Deleting a entry
""""""""""""""""

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteEntryOperator`
operator delete the entry.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_datacatalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_datacatalog_delete_entry]
    :end-before: [END howto_operator_gcp_datacatalog_delete_entry]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteEntryOperator`
parameters which allows you to dynamically determine values.

.. _howto/operator:CloudDataCatalogEntryGroupOperators:

Managing a entry groups
^^^^^^^^^^^^^^^^^^^^^^^

Operators uses a :class:`~google.cloud.datacatalog_v1beta1.types.Entry` for representing a entry groups.

.. contents::
  :depth: 1
  :local:

.. _howto/operator:CloudDataCatalogCreateEntryGroupOperator:

Creating an entry group
"""""""""""""""""""""""

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateEntryGroupOperator`
operator create the entry group.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_datacatalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_datacatalog_create_entry_group]
    :end-before: [END howto_operator_gcp_datacatalog_create_entry_group]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateEntryGroupOperator`
parameters which allows you to dynamically determine values.

The result is saved to :ref:`XCom <concepts:xcom>`, which allows it to be used by other operators.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_datacatalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_datacatalog_create_entry_group_result2]
    :end-before: [END howto_operator_gcp_datacatalog_create_entry_group_result2]

The newly created entry group ID can be read with the ``entry_group_id`` key.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_datacatalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_datacatalog_create_entry_group_result2]
    :end-before: [END howto_operator_gcp_datacatalog_create_entry_group_result2]

.. _howto/operator:CloudDataCatalogGetEntryGroupOperator:

Getting an entry group
""""""""""""""""""""""

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogGetEntryGroupOperator`
operator get the entry group.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_datacatalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_datacatalog_get_entry_group]
    :end-before: [END howto_operator_gcp_datacatalog_get_entry_group]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogGetEntryGroupOperator`
parameters which allows you to dynamically determine values.

The result is saved to :ref:`XCom <concepts:xcom>`, which allows it to be used by other operators.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_datacatalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_datacatalog_get_entry_group_result]
    :end-before: [END howto_operator_gcp_datacatalog_get_entry_group_result]

.. _howto/operator:CloudDataCatalogDeleteEntryGroupOperator:

Deleting an entry group
"""""""""""""""""""""""

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteEntryGroupOperator`
operator delete the entry group.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_datacatalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_datacatalog_delete_entry_group]
    :end-before: [END howto_operator_gcp_datacatalog_delete_entry_group]

vYou can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteEntryGroupOperator`
parameters which allows you to dynamically determine values.

.. _howto/operator:CloudDataCatalogTagTemplateOperators:

Managing a tag templates
^^^^^^^^^^^^^^^^^^^^^^^^

Operators uses a :class:`~google.cloud.datacatalog_v1beta1.types.TagTemplate` for representing a tag templates.

.. contents::
  :depth: 1
  :local:

.. _howto/operator:CloudDataCatalogCreateTagTemplateOperator:

Creating a tag templates
""""""""""""""""""""""""

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateTagTemplateOperator`
operator get the tag template.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_datacatalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_datacatalog_create_tag_template]
    :end-before: [END howto_operator_gcp_datacatalog_create_tag_template]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateTagTemplateOperator`
parameters which allows you to dynamically determine values.

The result is saved to :ref:`XCom <concepts:xcom>`, which allows it to be used by other operators.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_datacatalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_datacatalog_create_tag_template_result2]
    :end-before: [END howto_operator_gcp_datacatalog_create_tag_template_result2]

The newly created tag template ID can be read with the ``tag_template_id`` key.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_datacatalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_datacatalog_create_tag_template_result]
    :end-before: [END howto_operator_gcp_datacatalog_create_tag_template_result]

.. _howto/operator:CloudDataCatalogDeleteTagTemplateOperator:

Deleting a tag template
"""""""""""""""""""""""

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteTagTemplateOperator`
operator delete the tag template.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_datacatalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_datacatalog_delete_tag_template]
    :end-before: [END howto_operator_gcp_datacatalog_delete_tag_template]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteTagTemplateOperator`
parameters which allows you to dynamically determine values.


.. _howto/operator:CloudDataCatalogGetTagTemplateOperator:

Getting a tag template
""""""""""""""""""""""

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogGetTagTemplateOperator`
operator get the tag template.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_datacatalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_datacatalog_get_tag_template]
    :end-before: [END howto_operator_gcp_datacatalog_get_tag_template]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogGetTagTemplateOperator`
parameters which allows you to dynamically determine values.

The result is saved to :ref:`XCom <concepts:xcom>`, which allows it to be used by other operators.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_datacatalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_datacatalog_get_tag_template_result]
    :end-before: [END howto_operator_gcp_datacatalog_get_tag_template_result]

.. _howto/operator:CloudDataCatalogUpdateTagTemplateOperator:

Updating a tag template
"""""""""""""""""""""""

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogUpdateTagTemplateOperator`
operator update the tag template.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_datacatalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_datacatalog_update_tag_template]
    :end-before: [END howto_operator_gcp_datacatalog_update_tag_template]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogUpdateTagTemplateOperator`
parameters which allows you to dynamically determine values.

.. _howto/operator:CloudDataCatalogTagOperators:

Managing a tags
^^^^^^^^^^^^^^^

Operators uses a :class:`~google.cloud.datacatalog_v1beta1.types.Tag` for representing a tag.

.. contents::
  :depth: 1
  :local:

.. _howto/operator:CloudDataCatalogCreateTagOperator:

Creating a tag on an entry
""""""""""""""""""""""""""

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateTagOperator`
operator get the tag template.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_datacatalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_datacatalog_create_tag]
    :end-before: [END howto_operator_gcp_datacatalog_create_tag]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateTagOperator`
parameters which allows you to dynamically determine values.

The result is saved to :ref:`XCom <concepts:xcom>`, which allows it to be used by other operators.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_datacatalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_datacatalog_create_tag_result2]
    :end-before: [END howto_operator_gcp_datacatalog_create_tag_result2]

The newly created tag ID can be read with the ``tag_id`` key.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_datacatalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_datacatalog_create_entry_group_result2]
    :end-before: [END howto_operator_gcp_datacatalog_create_entry_group_result2]

.. _howto/operator:CloudDataCatalogUpdateTagOperator:

Updating an tag
"""""""""""""""

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogUpdateTagOperator`
operator update the tag template.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_datacatalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_datacatalog_update_tag_template]
    :end-before: [END howto_operator_gcp_datacatalog_update_tag_template]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogUpdateTagOperator`
parameters which allows you to dynamically determine values.

.. _howto/operator:CloudDataCatalogDeleteTagOperator:

Deleting an tag
"""""""""""""""

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteTagOperator`
operator delete the tag template.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_datacatalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_datacatalog_delete_tag_template]
    :end-before: [END howto_operator_gcp_datacatalog_delete_tag_template]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteTagOperator`
parameters which allows you to dynamically determine values.

.. _howto/operator:CloudDataCatalogListTagsOperator:

Listing an tags on an entry
"""""""""""""""""""""""""""

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogListTagsOperator`
operator get list of the tags on the entry.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_datacatalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_datacatalog_list_tags]
    :end-before: [END howto_operator_gcp_datacatalog_list_tags]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogListTagsOperator`
parameters which allows you to dynamically determine values.

The result is saved to :ref:`XCom <concepts:xcom>`, which allows it to be used by other operators.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_datacatalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_datacatalog_list_tags_result]
    :end-before: [END howto_operator_gcp_datacatalog_list_tags_result]


.. _howto/operator:CloudDataCatalogTagTemplateFieldssOperators:

Managing a tag template fields
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Operators uses a :class:`~google.cloud.datacatalog_v1beta1.types.TagTemplateField` for representing a tag template fields.

.. contents::
  :depth: 1
  :local:

.. _howto/operator:CloudDataCatalogCreateTagTemplateFieldOperator:

Creating a field
""""""""""""""""

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateTagTemplateFieldOperator`
operator get the tag template field.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_datacatalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_datacatalog_create_tag_template_field]
    :end-before: [END howto_operator_gcp_datacatalog_create_tag_template_field]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateTagTemplateFieldOperator`
parameters which allows you to dynamically determine values.

The result is saved to :ref:`XCom <concepts:xcom>`, which allows it to be used by other operators.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_datacatalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_datacatalog_create_tag_template_field_result2]
    :end-before: [END howto_operator_gcp_datacatalog_create_tag_template_field_result2]

The newly created field ID can be read with the ``tag_template_field_id`` key.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_datacatalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_datacatalog_create_entry_group_result2]
    :end-before: [END howto_operator_gcp_datacatalog_create_entry_group_result2]

.. _howto/operator:CloudDataCatalogRenameTagTemplateFieldOperator:

Renaming a field
""""""""""""""""

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogRenameTagTemplateFieldOperator`
operator rename the tag template field.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_datacatalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_datacatalog_rename_tag_template_field]
    :end-before: [END howto_operator_gcp_datacatalog_rename_tag_template_field]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogRenameTagTemplateFieldOperator`
parameters which allows you to dynamically determine values.

.. _howto/operator:CloudDataCatalogUpdateTagTemplateFieldOperator:

Updating a field
""""""""""""""""

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogUpdateTagTemplateFieldOperator`
operator get the tag template field.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_datacatalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_datacatalog_update_tag_template_field]
    :end-before: [END howto_operator_gcp_datacatalog_update_tag_template_field]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogUpdateTagTemplateFieldOperator`
parameters which allows you to dynamically determine values.


.. _howto/operator:CloudDataCatalogDeleteTagTemplateFieldOperator:

Deleting a field
""""""""""""""""

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteTagTemplateFieldOperator`
operator delete the tag template field.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_datacatalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_datacatalog_delete_tag_template_field]
    :end-before: [END howto_operator_gcp_datacatalog_delete_tag_template_field]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteTagTemplateFieldOperator`
parameters which allows you to dynamically determine values.


.. _howto/operator:CloudDataCatalogSearchCatalogOperator:

Search resources
^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogSearchCatalogOperator`
operator searches Data Catalog for multiple resources like entries, tags that match a query.

The ``query`` parameters should defined using `search syntax <https://cloud.google.com/data-catalog/docs/how-to/search-reference>`__.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_datacatalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_datacatalog_search_catalog]
    :end-before: [END howto_operator_gcp_datacatalog_search_catalog]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogSearchCatalogOperator`
parameters which allows you to dynamically determine values.

The result is saved to :ref:`XCom <concepts:xcom>`, which allows it to be used by other operators.

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_datacatalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_datacatalog_search_catalog_result]
    :end-before: [END howto_operator_gcp_datacatalog_search_catalog_result]

Reference
^^^^^^^^^

For further information, look at:

* `Client Library Documentation <https://googleapis.dev/python/datacatalog/latest/index.html>`__
* `Product Documentation <https://cloud.google.com/data-catalog/docs/>`__
