 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

:py:mod:`airflow.providers.amazon.aws.utils.emailer`
====================================================

.. py:module:: airflow.providers.amazon.aws.utils.emailer

.. autoapi-nested-parse::

   Airflow module for email backend using AWS SES.



Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.utils.emailer.send_email



.. py:function:: send_email(to, subject, html_content, files = None, cc = None, bcc = None, mime_subtype = 'mixed', mime_charset = 'utf-8', conn_id = 'aws_default', from_email = None, custom_headers = None, **kwargs)

   Email backend for SES.
