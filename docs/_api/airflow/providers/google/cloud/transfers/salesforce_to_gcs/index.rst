:mod:`airflow.providers.google.cloud.transfers.salesforce_to_gcs`
=================================================================

.. py:module:: airflow.providers.google.cloud.transfers.salesforce_to_gcs


Module Contents
---------------

.. py:class:: SalesforceToGcsOperator(*, query: str, bucket_name: str, object_name: str, salesforce_conn_id: str, include_deleted: bool = False, query_params: Optional[dict] = None, export_format: str = 'csv', coerce_to_timestamp: bool = False, record_time_added: bool = False, gzip: bool = False, gcp_conn_id: str = 'google_cloud_default', **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Submits Salesforce query and uploads results to Google Cloud Storage

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:SalesforceToGcsOperator`

   :param query: The query to make to Salesforce.
   :type query: str
   :param bucket_name: The bucket to upload to.
   :type bucket_name: str
   :param object_name: The object name to set when uploading the file.
   :type object_name: str
   :param salesforce_conn_id: the name of the connection that has the parameters
       we need to connect to Salesforce.
   :type conn_id: str
   :param include_deleted: True if the query should include deleted records.
   :type include_deleted: bool
   :param query_params: Additional optional arguments
   :type query_params: dict
   :param export_format: Desired format of files to be exported.
   :type export_format: str
   :param coerce_to_timestamp: True if you want all datetime fields to be converted into Unix timestamps.
       False if you want them to be left in the same format as they were in Salesforce.
       Leaving the value as False will result in datetimes being strings. Default: False
   :type coerce_to_timestamp: bool
   :param record_time_added: True if you want to add a Unix timestamp field
       to the resulting data that marks when the data was fetched from Salesforce. Default: False
   :type record_time_added: bool
   :param gzip: Option to compress local file or file data for upload
   :type gzip: bool
   :param gcp_conn_id: the name of the connection that has the parameters we need to connect to GCS.
   :type conn_id: str

   .. attribute:: template_fields
      :annotation: = ['query', 'bucket_name', 'object_name']

      

   .. attribute:: template_ext
      :annotation: = ['.sql']

      

   
   .. method:: execute(self, context: Dict)




