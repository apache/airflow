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
"""This module contains Google Translate operators."""
from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.translate import CloudTranslateHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class CloudTranslateTextOperator(BaseOperator):
    """
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

    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    """

    # [START translate_template_fields]
    template_fields: Sequence[str] = (
        'values',
        'target_language',
        'format_',
        'source_language',
        'model',
        'gcp_conn_id',
        'impersonation_chain',
    )
    # [END translate_template_fields]

    def __init__(
        self,
        *,
        values: list[str] | str,
        target_language: str,
        format_: str,
        source_language: str | None,
        model: str,
        gcp_conn_id: str = 'google_cloud_default',
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.values = values
        self.target_language = target_language
        self.format_ = format_
        self.source_language = source_language
        self.model = model
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> dict:
        hook = CloudTranslateHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        try:
            translation = hook.translate(
                values=self.values,
                target_language=self.target_language,
                format_=self.format_,
                source_language=self.source_language,
                model=self.model,
            )
            self.log.debug("Translation %s", translation)
            return translation
        except ValueError as e:
            self.log.error('An error has been thrown from translate method:')
            self.log.error(e)
            raise AirflowException(e)
