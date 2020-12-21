:mod:`airflow.providers.google.cloud.sensors.bigtable`
======================================================

.. py:module:: airflow.providers.google.cloud.sensors.bigtable

.. autoapi-nested-parse::

   This module contains Google Cloud Bigtable sensor.



Module Contents
---------------

.. py:class:: BigtableTableReplicationCompletedSensor(*, instance_id: str, table_id: str, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`, :class:`airflow.providers.google.cloud.operators.bigtable.BigtableValidationMixin`

   Sensor that waits for Cloud Bigtable table to be fully replicated to its clusters.
   No exception will be raised if the instance or the table does not exist.

   For more details about cluster states for a table, have a look at the reference:
   https://googleapis.github.io/google-cloud-python/latest/bigtable/table.html#google.cloud.bigtable.table.Table.get_cluster_states

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:BigtableTableReplicationCompletedSensor`

   :type instance_id: str
   :param instance_id: The ID of the Cloud Bigtable instance.
   :type table_id: str
   :param table_id: The ID of the table to check replication status.
   :type project_id: str
   :param project_id: Optional, the ID of the Google Cloud project.
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: REQUIRED_ATTRIBUTES
      :annotation: = ['instance_id', 'table_id']

      

   .. attribute:: template_fields
      :annotation: = ['project_id', 'instance_id', 'table_id', 'impersonation_chain']

      

   
   .. method:: poke(self, context: dict)




