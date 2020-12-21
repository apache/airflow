:mod:`airflow.providers.google.cloud.sensors.dataflow`
======================================================

.. py:module:: airflow.providers.google.cloud.sensors.dataflow

.. autoapi-nested-parse::

   This module contains a Google Cloud Dataflow sensor.



Module Contents
---------------

.. py:class:: DataflowJobStatusSensor(*, job_id: str, expected_statuses: Union[Set[str], str], project_id: Optional[str] = None, location: str = DEFAULT_DATAFLOW_LOCATION, gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Checks for the status of a job in Google Cloud Dataflow.

   :param job_id: ID of the job to be checked.
   :type job_id: str
   :param expected_statuses: The expected state of the operation.
       See:
       https://cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.jobs#Job.JobState
   :type expected_statuses: Union[Set[str], str]
   :param project_id: Optional, the Google Cloud project ID in which to start a job.
       If set to None or missing, the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param location: The location of the Dataflow job (for example europe-west1). See:
       https://cloud.google.com/dataflow/docs/concepts/regional-endpoints
   :type location: str
   :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
   :type gcp_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled. See:
       https://developers.google.com/identity/protocols/oauth2/service-account#delegatingauthority
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
      :annotation: = ['job_id']

      

   
   .. method:: poke(self, context: dict)




.. py:class:: DataflowJobMetricsSensor(*, job_id: str, callback: Callable[[dict], bool], project_id: Optional[str] = None, location: str = DEFAULT_DATAFLOW_LOCATION, gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Checks the metrics of a job in Google Cloud Dataflow.

   :param job_id: ID of the job to be checked.
   :type job_id: str
   :param callback: callback which is called with list of read job metrics
       See:
       https://cloud.google.com/dataflow/docs/reference/rest/v1b3/MetricUpdate
   :type callback: callable
   :param project_id: Optional, the Google Cloud project ID in which to start a job.
       If set to None or missing, the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param location: The location of the Dataflow job (for example europe-west1). See:
       https://cloud.google.com/dataflow/docs/concepts/regional-endpoints
   :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
   :type gcp_conn_id: str
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
      :annotation: = ['job_id']

      

   
   .. method:: poke(self, context: dict)




