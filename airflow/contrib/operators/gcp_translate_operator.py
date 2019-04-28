# -*- coding: utf-8 -*-
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

from airflow import AirflowException
from airflow.contrib.hooks.gcp_translate_hook import CloudTranslateHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


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

    """

    # [START translate_template_fields]
    template_fields = ('values', 'target_language', 'format_', 'source_language', 'model', 'gcp_conn_id')
    # [END translate_template_fields]

    @apply_defaults
    def __init__(
        self,
        values,
        target_language,
        format_,
        source_language,
        model,
        gcp_conn_id='google_cloud_default',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.values = values
        self.target_language = target_language
        self.format_ = format_
        self.source_language = source_language
        self.model = model
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        _hook = CloudTranslateHook(gcp_conn_id=self.gcp_conn_id)
        try:
            translation = _hook.translate(
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
