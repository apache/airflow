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

====================================
Amazon Simple Email Service (SES)
====================================

`Amazon Simple Email Service (Amazon SES) <https://aws.amazon.com/ses/>`__ is a cloud email
service provider that can integrate into any application for bulk email sending. Whether you
send transactional or marketing emails, you pay only for what you use. Amazon SES also supports
a variety of deployments including dedicated, shared, or owned IP addresses. Reports on sender
statistics and a deliverability dashboard help businesses make every email count.

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

Generic Parameters
------------------

.. include:: ../_partials/generic_parameters.rst

Operators
---------

.. _howto/operator:SesEmailOperator:

Send an email using Amazon SES
===============================

To send an email using Amazon Simple Email Service you can use
:class:`~airflow.providers.amazon.aws.operators.ses.SesEmailOperator`.

The following example shows how to send a basic email:

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_ses.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_ses_email_basic]
    :end-before: [END howto_operator_ses_email_basic]

You can also send emails with CC and BCC recipients:

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_ses.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_ses_email_cc_bcc]
    :end-before: [END howto_operator_ses_email_cc_bcc]

For more advanced use cases, you can add custom headers and set reply-to addresses:

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_ses.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_ses_email_headers]
    :end-before: [END howto_operator_ses_email_headers]

The operator also supports Jinja templating for dynamic content:

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_ses.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_ses_email_templated]
    :end-before: [END howto_operator_ses_email_templated]

Reference
---------

* `AWS boto3 library documentation for SES <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ses.html>`__
