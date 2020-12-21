:mod:`airflow.providers.google.cloud.operators.mlengine`
========================================================

.. py:module:: airflow.providers.google.cloud.operators.mlengine

.. autoapi-nested-parse::

   This module contains Google Cloud MLEngine operators.



Module Contents
---------------

.. data:: log
   

   

.. function:: _normalize_mlengine_job_id(job_id: str) -> str
   Replaces invalid MLEngine job_id characters with '_'.

   This also adds a leading 'z' in case job_id starts with an invalid
   character.

   :param job_id: A job_id str that may have invalid characters.
   :type job_id: str:
   :return: A valid job_id representation.
   :rtype: str


.. py:class:: MLEngineStartBatchPredictionJobOperator(*, job_id: str, region: str, data_format: str, input_paths: List[str], output_path: str, model_name: Optional[str] = None, version_name: Optional[str] = None, uri: Optional[str] = None, max_worker_count: Optional[int] = None, runtime_version: Optional[str] = None, signature_name: Optional[str] = None, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, labels: Optional[Dict[str, str]] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Start a Google Cloud ML Engine prediction job.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:MLEngineStartBatchPredictionJobOperator`

   NOTE: For model origin, users should consider exactly one from the
   three options below:

   1. Populate ``uri`` field only, which should be a GCS location that
      points to a tensorflow savedModel directory.
   2. Populate ``model_name`` field only, which refers to an existing
      model, and the default version of the model will be used.
   3. Populate both ``model_name`` and ``version_name`` fields, which
      refers to a specific version of a specific model.

   In options 2 and 3, both model and version name should contain the
   minimal identifier. For instance, call::

       MLEngineBatchPredictionOperator(
           ...,
           model_name='my_model',
           version_name='my_version',
           ...)

   if the desired model version is
   ``projects/my_project/models/my_model/versions/my_version``.

   See https://cloud.google.com/ml-engine/reference/rest/v1/projects.jobs
   for further documentation on the parameters.

   :param job_id: A unique id for the prediction job on Google Cloud
       ML Engine. (templated)
   :type job_id: str
   :param data_format: The format of the input data.
       It will default to 'DATA_FORMAT_UNSPECIFIED' if is not provided
       or is not one of ["TEXT", "TF_RECORD", "TF_RECORD_GZIP"].
   :type data_format: str
   :param input_paths: A list of GCS paths of input data for batch
       prediction. Accepting wildcard operator ``*``, but only at the end. (templated)
   :type input_paths: list[str]
   :param output_path: The GCS path where the prediction results are
       written to. (templated)
   :type output_path: str
   :param region: The Google Compute Engine region to run the
       prediction job in. (templated)
   :type region: str
   :param model_name: The Google Cloud ML Engine model to use for prediction.
       If version_name is not provided, the default version of this
       model will be used.
       Should not be None if version_name is provided.
       Should be None if uri is provided. (templated)
   :type model_name: str
   :param version_name: The Google Cloud ML Engine model version to use for
       prediction.
       Should be None if uri is provided. (templated)
   :type version_name: str
   :param uri: The GCS path of the saved model to use for prediction.
       Should be None if model_name is provided.
       It should be a GCS path pointing to a tensorflow SavedModel. (templated)
   :type uri: str
   :param max_worker_count: The maximum number of workers to be used
       for parallel processing. Defaults to 10 if not specified. Should be a
       string representing the worker count ("10" instead of 10, "50" instead
       of 50, etc.)
   :type max_worker_count: str
   :param runtime_version: The Google Cloud ML Engine runtime version to use
       for batch prediction.
   :type runtime_version: str
   :param signature_name: The name of the signature defined in the SavedModel
       to use for this job.
   :type signature_name: str
   :param project_id: The Google Cloud project name where the prediction job is submitted.
       If set to None or missing, the default project_id from the Google Cloud connection is used.
       (templated)
   :type project_id: str
   :param gcp_conn_id: The connection ID used for connection to Google
       Cloud Platform.
   :type gcp_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param labels: a dictionary containing labels for the job; passed to BigQuery
   :type labels: Dict[str, str]
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   :raises: ``ValueError``: if a unique model/version origin cannot be
       determined.

   .. attribute:: template_fields
      :annotation: = ['_project_id', '_job_id', '_region', '_input_paths', '_output_path', '_model_name', '_version_name', '_uri', '_impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: MLEngineManageModelOperator(*, model: dict, operation: str = 'create', project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Operator for managing a Google Cloud ML Engine model.

   .. warning::
      This operator is deprecated. Consider using operators for specific operations:
      MLEngineCreateModelOperator, MLEngineGetModelOperator.

   :param model: A dictionary containing the information about the model.
       If the `operation` is `create`, then the `model` parameter should
       contain all the information about this model such as `name`.

       If the `operation` is `get`, the `model` parameter
       should contain the `name` of the model.
   :type model: dict
   :param operation: The operation to perform. Available operations are:

       * ``create``: Creates a new model as provided by the `model` parameter.
       * ``get``: Gets a particular model where the name is specified in `model`.
   :type operation: str
   :param project_id: The Google Cloud project name to which MLEngine model belongs.
       If set to None or missing, the default project_id from the Google Cloud connection is used.
       (templated)
   :type project_id: str
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
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['_project_id', '_model', '_impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: MLEngineCreateModelOperator(*, model: dict, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Creates a new model.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:MLEngineCreateModelOperator`

   The model should be provided by the `model` parameter.

   :param model: A dictionary containing the information about the model.
   :type model: dict
   :param project_id: The Google Cloud project name to which MLEngine model belongs.
       If set to None or missing, the default project_id from the Google Cloud connection is used.
       (templated)
   :type project_id: str
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
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['_project_id', '_model', '_impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: MLEngineGetModelOperator(*, model_name: str, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Gets a particular model

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:MLEngineGetModelOperator`

   The name of model should be specified in `model_name`.

   :param model_name: The name of the model.
   :type model_name: str
   :param project_id: The Google Cloud project name to which MLEngine model belongs.
       If set to None or missing, the default project_id from the Google Cloud connection is used.
       (templated)
   :type project_id: str
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
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['_project_id', '_model_name', '_impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: MLEngineDeleteModelOperator(*, model_name: str, delete_contents: bool = False, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Deletes a model.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:MLEngineDeleteModelOperator`

   The model should be provided by the `model_name` parameter.

   :param model_name: The name of the model.
   :type model_name: str
   :param delete_contents: (Optional) Whether to force the deletion even if the models is not empty.
       Will delete all version (if any) in the dataset if set to True.
       The default value is False.
   :type delete_contents: bool
   :param project_id: The Google Cloud project name to which MLEngine model belongs.
       If set to None or missing, the default project_id from the Google Cloud connection is used.
       (templated)
   :type project_id: str
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
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['_project_id', '_model_name', '_impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: MLEngineManageVersionOperator(*, model_name: str, version_name: Optional[str] = None, version: Optional[dict] = None, operation: str = 'create', project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Operator for managing a Google Cloud ML Engine version.

   .. warning::
      This operator is deprecated. Consider using operators for specific operations:
      MLEngineCreateVersionOperator, MLEngineSetDefaultVersionOperator,
      MLEngineListVersionsOperator, MLEngineDeleteVersionOperator.

   :param model_name: The name of the Google Cloud ML Engine model that the version
       belongs to. (templated)
   :type model_name: str
   :param version_name: A name to use for the version being operated upon.
       If not None and the `version` argument is None or does not have a value for
       the `name` key, then this will be populated in the payload for the
       `name` key. (templated)
   :type version_name: str
   :param version: A dictionary containing the information about the version.
       If the `operation` is `create`, `version` should contain all the
       information about this version such as name, and deploymentUrl.
       If the `operation` is `get` or `delete`, the `version` parameter
       should contain the `name` of the version.
       If it is None, the only `operation` possible would be `list`. (templated)
   :type version: dict
   :param operation: The operation to perform. Available operations are:

       *   ``create``: Creates a new version in the model specified by `model_name`,
           in which case the `version` parameter should contain all the
           information to create that version
           (e.g. `name`, `deploymentUrl`).

       *   ``set_defaults``: Sets a version in the model specified by `model_name` to be the default.
           The name of the version should be specified in the `version`
           parameter.

       *   ``list``: Lists all available versions of the model specified
           by `model_name`.

       *   ``delete``: Deletes the version specified in `version` parameter from the
           model specified by `model_name`).
           The name of the version should be specified in the `version`
           parameter.
   :type operation: str
   :param project_id: The Google Cloud project name to which MLEngine model belongs.
       If set to None or missing, the default project_id from the Google Cloud connection is used.
       (templated)
   :type project_id: str
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
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['_project_id', '_model_name', '_version_name', '_version', '_impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: MLEngineCreateVersionOperator(*, model_name: str, version: dict, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Creates a new version in the model

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:MLEngineCreateVersionOperator`

   Model should be specified by `model_name`, in which case the `version` parameter should contain all the
   information to create that version

   :param model_name: The name of the Google Cloud ML Engine model that the version belongs to. (templated)
   :type model_name: str
   :param version: A dictionary containing the information about the version. (templated)
   :type version: dict
   :param project_id: The Google Cloud project name to which MLEngine model belongs.
       If set to None or missing, the default project_id from the Google Cloud connection is used.
       (templated)
   :type project_id: str
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
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['_project_id', '_model_name', '_version', '_impersonation_chain']

      

   
   .. method:: _validate_inputs(self)



   
   .. method:: execute(self, context)




.. py:class:: MLEngineSetDefaultVersionOperator(*, model_name: str, version_name: str, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Sets a version in the model.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:MLEngineSetDefaultVersionOperator`

   The model should be specified by `model_name` to be the default. The name of the version should be
   specified in the `version_name` parameter.

   :param model_name: The name of the Google Cloud ML Engine model that the version belongs to. (templated)
   :type model_name: str
   :param version_name: A name to use for the version being operated upon. (templated)
   :type version_name: str
   :param project_id: The Google Cloud project name to which MLEngine model belongs.
       If set to None or missing, the default project_id from the Google Cloud connection is used.
       (templated)
   :type project_id: str
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
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['_project_id', '_model_name', '_version_name', '_impersonation_chain']

      

   
   .. method:: _validate_inputs(self)



   
   .. method:: execute(self, context)




.. py:class:: MLEngineListVersionsOperator(*, model_name: str, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Lists all available versions of the model

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:MLEngineListVersionsOperator`

   The model should be specified by `model_name`.

   :param model_name: The name of the Google Cloud ML Engine model that the version
       belongs to. (templated)
   :type model_name: str
   :param gcp_conn_id: The connection ID to use when fetching connection info.
   :type gcp_conn_id: str
   :param project_id: The Google Cloud project name to which MLEngine model belongs.
       If set to None or missing, the default project_id from the Google Cloud connection is used.
       (templated)
   :type project_id: str
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
      :annotation: = ['_project_id', '_model_name', '_impersonation_chain']

      

   
   .. method:: _validate_inputs(self)



   
   .. method:: execute(self, context)




.. py:class:: MLEngineDeleteVersionOperator(*, model_name: str, version_name: str, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Deletes the version from the model.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:MLEngineDeleteVersionOperator`

   The name of the version should be specified in `version_name` parameter from the model specified
   by `model_name`.

   :param model_name: The name of the Google Cloud ML Engine model that the version
       belongs to. (templated)
   :type model_name: str
   :param version_name: A name to use for the version being operated upon. (templated)
   :type version_name: str
   :param project_id: The Google Cloud project name to which MLEngine
       model belongs.
   :type project_id: str
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
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['_project_id', '_model_name', '_version_name', '_impersonation_chain']

      

   
   .. method:: _validate_inputs(self)



   
   .. method:: execute(self, context)




.. py:class:: AIPlatformConsoleLink

   Bases: :class:`airflow.models.BaseOperatorLink`

   Helper class for constructing AI Platform Console link.

   .. attribute:: name
      :annotation: = AI Platform Console

      

   
   .. method:: get_link(self, operator, dttm)




.. py:class:: MLEngineStartTrainingJobOperator(*, job_id: str, package_uris: List[str], training_python_module: str, training_args: List[str], region: str, scale_tier: Optional[str] = None, master_type: Optional[str] = None, master_config: Optional[Dict] = None, runtime_version: Optional[str] = None, python_version: Optional[str] = None, job_dir: Optional[str] = None, service_account: Optional[str] = None, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, mode: str = 'PRODUCTION', labels: Optional[Dict[str, str]] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Operator for launching a MLEngine training job.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:MLEngineStartTrainingJobOperator`

   :param job_id: A unique templated id for the submitted Google MLEngine
       training job. (templated)
   :type job_id: str
   :param package_uris: A list of package locations for MLEngine training job,
       which should include the main training program + any additional
       dependencies. (templated)
   :type package_uris: List[str]
   :param training_python_module: The Python module name to run within MLEngine
       training job after installing 'package_uris' packages. (templated)
   :type training_python_module: str
   :param training_args: A list of templated command line arguments to pass to
       the MLEngine training program. (templated)
   :type training_args: List[str]
   :param region: The Google Compute Engine region to run the MLEngine training
       job in (templated).
   :type region: str
   :param scale_tier: Resource tier for MLEngine training job. (templated)
   :type scale_tier: str
   :param master_type: Cloud ML Engine machine name.
       Must be set when scale_tier is CUSTOM. (templated)
   :type master_type: str
   :param master_config: Cloud ML Engine master config.
       master_type must be set if master_config is provided. (templated)
   :type master_type: dict
   :param runtime_version: The Google Cloud ML runtime version to use for
       training. (templated)
   :type runtime_version: str
   :param python_version: The version of Python used in training. (templated)
   :type python_version: str
   :param job_dir: A Google Cloud Storage path in which to store training
       outputs and other data needed for training. (templated)
   :type job_dir: str
   :param service_account: Optional service account to use when running the training application.
       (templated)
       The specified service account must have the `iam.serviceAccounts.actAs` role. The
       Google-managed Cloud ML Engine service account must have the `iam.serviceAccountAdmin` role
       for the specified service account.
       If set to None or missing, the Google-managed Cloud ML Engine service account will be used.
   :type service_account: str
   :param project_id: The Google Cloud project name within which MLEngine training job should run.
       If set to None or missing, the default project_id from the Google Cloud connection is used.
       (templated)
   :type project_id: str
   :param gcp_conn_id: The connection ID to use when fetching connection info.
   :type gcp_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param mode: Can be one of 'DRY_RUN'/'CLOUD'. In 'DRY_RUN' mode, no real
       training job will be launched, but the MLEngine training job request
       will be printed out. In 'CLOUD' mode, a real MLEngine training job
       creation request will be issued.
   :type mode: str
   :param labels: a dictionary containing labels for the job; passed to BigQuery
   :type labels: Dict[str, str]
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
      :annotation: = ['_project_id', '_job_id', '_package_uris', '_training_python_module', '_training_args', '_region', '_scale_tier', '_master_type', '_master_config', '_runtime_version', '_python_version', '_job_dir', '_service_account', '_impersonation_chain']

      

   .. attribute:: operator_extra_links
      

      

   
   .. method:: execute(self, context)




.. py:class:: MLEngineTrainingCancelJobOperator(*, job_id: str, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Operator for cleaning up failed MLEngine training job.

   :param job_id: A unique templated id for the submitted Google MLEngine
       training job. (templated)
   :type job_id: str
   :param project_id: The Google Cloud project name within which MLEngine training job should run.
       If set to None or missing, the default project_id from the Google Cloud connection is used.
       (templated)
   :type project_id: str
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
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['_project_id', '_job_id', '_impersonation_chain']

      

   
   .. method:: execute(self, context)




