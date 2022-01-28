#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""This module contains a Google Cloud Translate Hook."""
from typing import List, Optional, Sequence, Union

from google.cloud.translate_v2 import Client

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class CloudTranslateHook(GoogleBaseHook):
    """
    Hook for Google Cloud translate APIs.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )
        self._client = None  # type: Optional[Client]

    def get_conn(self) -> Client:
        """
        Retrieves connection to Cloud Translate

        :return: Google Cloud Translate client object.
        :rtype: google.cloud.translate_v2.Client
        """
        if not self._client:
            self._client = Client(credentials=self._get_credentials(), client_info=self.client_info)
        return self._client

    @GoogleBaseHook.quota_retry()
    def translate(
        self,
        values: Union[str, List[str]],
        target_language: str,
        format_: Optional[str] = None,
        source_language: Optional[str] = None,
        model: Optional[Union[str, List[str]]] = None,
    ) -> dict:
        """Translate a string or list of strings.

        See https://cloud.google.com/translate/docs/translating-text

        :param values: String or list of strings to translate.
        :param target_language: The language to translate results into. This
                                is required by the API and defaults to
                                the target language of the current instance.
        :param format_: (Optional) One of ``text`` or ``html``, to specify
                        if the input text is plain text or HTML.
        :param source_language: (Optional) The language of the text to
                                be translated.
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
        """
        client = self.get_conn()

        return client.translate(
            values=values,
            target_language=target_language,
            format_=format_,
            source_language=source_language,
            model=model,
        )
