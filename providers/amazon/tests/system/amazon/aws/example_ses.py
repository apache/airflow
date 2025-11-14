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
from __future__ import annotations

import os
from datetime import datetime

from airflow.providers.amazon.aws.operators.ses import SesEmailOperator

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import DAG, chain, task
else:
    # Airflow 2 path
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
    from airflow.models.baseoperator import chain  # type: ignore[attr-defined,no-redef]
    from airflow.models.dag import DAG  # type: ignore[attr-defined,no-redef,assignment]


@task
def get_verified_email() -> str:
    email = os.getenv("SES_VERIFIED_EMAIL")
    if not email:
        raise ValueError(
            "Please set SES_VERIFIED_EMAIL environment variable to a verified email address in your SES account"
        )
    return email


with DAG(
    dag_id="example_ses",
    start_date=datetime(2021, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["example"],
) as dag:
    verified_email = get_verified_email()

    # [START howto_operator_ses_email_basic]
    # Basic email sending
    # Note: In SES sandbox mode, both sender and recipient must be verified.
    send_basic_email = SesEmailOperator(
        task_id="send_basic_email",
        mail_from=verified_email,
        to=[verified_email],
        subject="Test Email from Airflow",
        html_content="<h1>Hello</h1><p>This is a test email sent via Amazon SES.</p>",
        aws_conn_id="aws_default",
    )
    # [END howto_operator_ses_email_basic]

    # [START howto_operator_ses_email_cc_bcc]
    # Email with CC and BCC
    send_email_with_cc_bcc = SesEmailOperator(
        task_id="send_email_with_cc_bcc",
        mail_from=verified_email,
        to=[verified_email],
        cc=[verified_email],
        bcc=[verified_email],
        subject="Test Email with CC and BCC",
        html_content="<h1>Hello</h1><p>This email has CC and BCC recipients.</p>",
        aws_conn_id="aws_default",
    )
    # [END howto_operator_ses_email_cc_bcc]

    # [START howto_operator_ses_email_headers]
    # Email with custom headers and reply-to
    send_email_with_headers = SesEmailOperator(
        task_id="send_email_with_headers",
        mail_from=verified_email,
        to=[verified_email],
        subject="Test Email with Custom Headers",
        html_content="<h1>Hello</h1><p>This email has custom headers.</p>",
        reply_to=verified_email,
        return_path=verified_email,
        custom_headers={"X-Custom-Header": "CustomValue"},
        aws_conn_id="aws_default",
    )
    # [END howto_operator_ses_email_headers]

    # [START howto_operator_ses_email_templated]
    # Email with template variables
    send_templated_email = SesEmailOperator(
        task_id="send_templated_email",
        mail_from=verified_email,
        to=[verified_email],
        subject="DAG Run: {{ dag.dag_id }} - {{ ds }}",
        html_content="""
        <h1>DAG Run Report</h1>
        <p>DAG ID: {{ dag.dag_id }}</p>
        <p>Execution Date: {{ ds }}</p>
        <p>Run ID: {{ run_id }}</p>
        """,
        aws_conn_id="aws_default",
    )
    # [END howto_operator_ses_email_templated]

    chain(
        verified_email,
        send_basic_email,
        send_email_with_cc_bcc,
        send_email_with_headers,
        send_templated_email,
    )


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
