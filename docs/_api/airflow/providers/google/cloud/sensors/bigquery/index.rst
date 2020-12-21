:mod:`airflow.providers.google.cloud.sensors.bigquery`
======================================================

.. py:module:: airflow.providers.google.cloud.sensors.bigquery

.. autoapi-nested-parse::

   This module contains a Google Bigquery sensor.



Module Contents
---------------

.. py:class:: BigQueryTableExistenceSensor(*, project_id: str, dataset_id: str, table_id: str, bigquery_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Checks for the existence of a table in Google Bigquery.

   :param project_id: The Google cloud project in which to look for the table.
       The connection supplied to the hook must provide
       access to the specified project.
   :type project_id: str
   :param dataset_id: The name of the dataset in which to look for the table.
       storage bucket.
   :type dataset_id: str
   :param table_id: The name of the table to check the existence of.
   :type table_id: str
   :param bigquery_conn_id: The connection ID to use when connecting to
       Google BigQuery.
   :type bigquery_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['project_id', 'dataset_id', 'table_id', 'impersonation_chain']

      

   .. attribute:: ui_color
      :annotation: = #f0eee4

      

   
   .. method:: poke(self, context: dict)




.. py:class:: BigQueryTablePartitionExistenceSensor(*, project_id: str, dataset_id: str, table_id: str, partition_id: str, bigquery_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Checks for the existence of a partition within a table in Google Bigquery.

   :param project_id: The Google cloud project in which to look for the table.
       The connection supplied to the hook must provide
       access to the specified project.
   :type project_id: str
   :param dataset_id: The name of the dataset in which to look for the table.
       storage bucket.
   :type dataset_id: str
   :param table_id: The name of the table to check the existence of.
   :type table_id: str
   :param partition_id: The name of the partition to check the existence of.
   :type partition_id: str
   :param bigquery_conn_id: The connection ID to use when connecting to
       Google BigQuery.
   :type bigquery_conn_id: str
   :param delegate_to: The account to impersonate, if any.
       For this to work, the service account making the request must
       have domain-wide delegation enabled.
   :type delegate_to: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['project_id', 'dataset_id', 'table_id', 'partition_id', 'impersonation_chain']

      

   .. attribute:: ui_color
      :annotation: = #f0eee4

      

   
   .. method:: poke(self, context: dict)




