:mod:`airflow.providers.google.common.hooks.base_google`
========================================================

.. py:module:: airflow.providers.google.common.hooks.base_google

.. autoapi-nested-parse::

   This module contains a Google Cloud API base hook.



Module Contents
---------------

.. data:: log
   

   

.. data:: INVALID_KEYS
   :annotation: = ['DefaultRequestsPerMinutePerProject', 'DefaultRequestsPerMinutePerUser', 'RequestsPerMinutePerProject', 'Resource has been exhausted (e.g. check quota).']

   

.. data:: INVALID_REASONS
   :annotation: = ['userRateLimitExceeded']

   

.. function:: is_soft_quota_exception(exception: Exception)
   API for Google services does not have a standardized way to report quota violation errors.
   The function has been adapted by trial and error to the following services:

   * Google Translate
   * Google Vision
   * Google Text-to-Speech
   * Google Speech-to-Text
   * Google Natural Language
   * Google Video Intelligence


.. function:: is_operation_in_progress_exception(exception: Exception) -> bool
   Some of the calls return 429 (too many requests!) or 409 errors (Conflict)
   in case of operation in progress.

   * Google Cloud SQL


.. py:class:: retry_if_temporary_quota

   Bases: :class:`tenacity.retry_if_exception`

   Retries if there was an exception for exceeding the temporary quote limit.


.. py:class:: retry_if_operation_in_progress

   Bases: :class:`tenacity.retry_if_exception`

   Retries if there was an exception for exceeding the temporary quote limit.


.. data:: T
   

   

.. data:: RT
   

   

.. py:class:: GoogleBaseHook(gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None)

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   A base hook for Google cloud-related hooks. Google cloud has a shared REST
   API client that is built in the same way no matter which service you use.
   This class helps construct and authorize the credentials needed to then
   call googleapiclient.discovery.build() to actually discover and build a client
   for a Google cloud service.

   The class also contains some miscellaneous helper functions.

   All hook derived from this base hook use the 'Google Cloud' connection
   type. Three ways of authentication are supported:

   Default credentials: Only the 'Project Id' is required. You'll need to
   have set up default credentials, such as by the
   ``GOOGLE_APPLICATION_DEFAULT`` environment variable or from the metadata
   server on Google Compute Engine.

   JSON key file: Specify 'Project Id', 'Keyfile Path' and 'Scope'.

   Legacy P12 key files are not supported.

   JSON data provided in the UI: Specify 'Keyfile JSON'.

   :param gcp_conn_id: The connection ID to use when fetching connection info.
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
       account from the list granting this role to the originating account.
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: project_id
      

      Returns project id.

      :return: id of the project
      :rtype: str


   .. attribute:: num_retries
      

      Returns num_retries from Connection.

      :return: the number of times each API request should be retried
      :rtype: int


   .. attribute:: client_info
      

      Return client information used to generate a user-agent for API calls.

      It allows for better errors tracking.

      This object is only used by the google-cloud-* libraries that are built specifically for
      the Google Cloud. It is not supported by The Google APIs Python Client that use Discovery
      based APIs.


   .. attribute:: scopes
      

      Return OAuth 2.0 scopes.

      :return: Returns the scope defined in the connection configuration, or the default scope
      :rtype: Sequence[str]


   
   .. method:: _get_credentials_and_project_id(self)

      Returns the Credentials object for Google API and the associated project_id



   
   .. method:: _get_credentials(self)

      Returns the Credentials object for Google API



   
   .. method:: _get_access_token(self)

      Returns a valid access token from Google API Credentials



   
   .. method:: _get_credentials_email(self)

      Returns the email address associated with the currently logged in account

      If a service account is used, it returns the service account.
      If user authentication (e.g. gcloud auth) is used, it returns the e-mail account of that user.



   
   .. method:: _authorize(self)

      Returns an authorized HTTP object to be used to build a Google cloud
      service hook connection.



   
   .. method:: _get_field(self, f: str, default: Any = None)

      Fetches a field from extras, and returns it. This is some Airflow
      magic. The google_cloud_platform hook type adds custom UI elements
      to the hook page, which allow admins to specify service_account,
      key_path, etc. They get formatted as shown below.



   
   .. staticmethod:: quota_retry(*args, **kwargs)

      A decorator that provides a mechanism to repeat requests in response to exceeding a temporary quote
      limit.



   
   .. staticmethod:: operation_in_progress_retry(*args, **kwargs)

      A decorator that provides a mechanism to repeat requests in response to
      operation in progress (HTTP 409)
      limit.



   
   .. staticmethod:: fallback_to_default_project_id(func: Callable[..., RT])

      Decorator that provides fallback for Google Cloud project id. If
      the project is None it will be replaced with the project_id from the
      service account the Hook is authenticated with. Project id can be specified
      either via project_id kwarg or via first parameter in positional args.

      :param func: function to wrap
      :return: result of the function call



   
   .. staticmethod:: provide_gcp_credential_file(func: T)

      Function decorator that provides a Google Cloud credentials for application supporting Application
      Default Credentials (ADC) strategy.

      It is recommended to use ``provide_gcp_credential_file_as_context`` context manager to limit the
      scope when authorization data is available. Using context manager also
      makes it easier to use multiple connection in one function.



   
   .. method:: provide_gcp_credential_file_as_context(self)

      Context manager that provides a Google Cloud credentials for application supporting `Application
      Default Credentials (ADC) strategy <https://cloud.google.com/docs/authentication/production>`__.

      It can be used to provide credentials for external programs (e.g. gcloud) that expect authorization
      file in ``GOOGLE_APPLICATION_CREDENTIALS`` environment variable.



   
   .. method:: provide_authorized_gcloud(self)

      Provides a separate gcloud configuration with current credentials.

      The gcloud tool allows you to login to Google Cloud only - ``gcloud auth login`` and
      for the needs of Application Default Credentials ``gcloud auth application-default login``.
      In our case, we want all commands to use only the credentials from ADCm so
      we need to configure the credentials in gcloud manually.



   
   .. staticmethod:: download_content_from_request(file_handle, request: dict, chunk_size: int)

      Download media resources.
      Note that  the Python file object is compatible with io.Base and can be used with this class also.

      :param file_handle: io.Base or file object. The stream in which to write the downloaded
          bytes.
      :type file_handle: io.Base or file object
      :param request: googleapiclient.http.HttpRequest, the media request to perform in chunks.
      :type request: Dict
      :param chunk_size: int, File will be downloaded in chunks of this many bytes.
      :type chunk_size: int




