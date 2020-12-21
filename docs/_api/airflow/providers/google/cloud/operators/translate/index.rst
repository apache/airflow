:mod:`airflow.providers.google.cloud.operators.translate`
=========================================================

.. py:module:: airflow.providers.google.cloud.operators.translate

.. autoapi-nested-parse::

   This module contains Google Translate operators.



Module Contents
---------------

.. py:class:: CloudTranslateTextOperator(*, values: Union[List[str], str], target_language: str, format_: str, source_language: Optional[str], model: str, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Translate a string or list of strings.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudTranslateTextOperator`

   See https://cloud.google.com/translate/docs/translating-text

   Execute method returns str or list.

   This is a list of dictionaries for each queried value. Each
   dictionary typically contains three keys (though not
   all will be present in all cases).

   * ``detectedSourceLanguage``: The detected language (as an
     ISO 639-1 language code) of the text.
   * ``translatedText``: The translation of the text into the
     target language.
   * ``input``: The corresponding input value.
   * ``model``: The model used to translate the text.

   If only a single value is passed, then only a single
   dictionary is set as XCom return value.

   :type values: str or list
   :param values: String or list of strings to translate.

   :type target_language: str
   :param target_language: The language to translate results into. This
     is required by the API and defaults to
     the target language of the current instance.

   :type format_: str or None
   :param format_: (Optional) One of ``text`` or ``html``, to specify
     if the input text is plain text or HTML.

   :type source_language: str or None
   :param source_language: (Optional) The language of the text to
     be translated.

   :type model: str or None
   :param model: (Optional) The model used to translate the text, such
     as ``'base'`` or ``'nmt'``.

   :type impersonation_chain: Union[str, Sequence[str]]
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).

   .. attribute:: template_fields
      :annotation: = ['values', 'target_language', 'format_', 'source_language', 'model', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




