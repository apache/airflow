:mod:`airflow.providers.google.cloud.hooks.translate`
=====================================================

.. py:module:: airflow.providers.google.cloud.hooks.translate

.. autoapi-nested-parse::

   This module contains a Google Cloud Translate Hook.



Module Contents
---------------

.. py:class:: CloudTranslateHook(gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None)

   Bases: :class:`airflow.providers.google.common.hooks.base_google.GoogleBaseHook`

   Hook for Google Cloud translate APIs.

   All the methods in the hook where project_id is used must be called with
   keyword arguments rather than positional.

   
   .. method:: get_conn(self)

      Retrieves connection to Cloud Translate

      :return: Google Cloud Translate client object.
      :rtype: google.cloud.translate_v2.Client



   
   .. method:: translate(self, values: Union[str, List[str]], target_language: str, format_: Optional[str] = None, source_language: Optional[str] = None, model: Optional[Union[str, List[str]]] = None)

      Translate a string or list of strings.

      See https://cloud.google.com/translate/docs/translating-text

      :type values: str or list
      :param values: String or list of strings to translate.
      :type target_language: str
      :param target_language: The language to translate results into. This
                              is required by the API and defaults to
                              the target language of the current instance.
      :type format_: str
      :param format_: (Optional) One of ``text`` or ``html``, to specify
                      if the input text is plain text or HTML.
      :type source_language: str or None
      :param source_language: (Optional) The language of the text to
                              be translated.
      :type model: str or None
      :param model: (Optional) The model used to translate the text, such
                    as ``'base'`` or ``'nmt'``.
      :rtype: str or list
      :returns: A list of dictionaries for each queried value. Each
                dictionary typically contains three keys (though not
                all will be present in all cases)

                * ``detectedSourceLanguage``: The detected language (as an
                  ISO 639-1 language code) of the text.

                * ``translatedText``: The translation of the text into the
                  target language.

                * ``input``: The corresponding input value.

                * ``model``: The model used to translate the text.

                If only a single value is passed, then only a single
                dictionary will be returned.
      :raises: :class:`~exceptions.ValueError` if the number of
               values and translations differ.




