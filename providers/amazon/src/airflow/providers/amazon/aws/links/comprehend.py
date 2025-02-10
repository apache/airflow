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

from airflow.providers.amazon.aws.links.base_aws import BASE_AWS_CONSOLE_LINK, BaseAwsLink


class ComprehendPiiEntitiesDetectionLink(BaseAwsLink):
    """Helper class for constructing Amazon Comprehend PII Detection console link."""

    name = "PII Detection Job"
    key = "comprehend_pii_detection"
    format_str = (
        BASE_AWS_CONSOLE_LINK
        + "/comprehend/home?region={region_name}#"
        + "/analysis-job-details/pii/{job_id}"
    )


class ComprehendDocumentClassifierLink(BaseAwsLink):
    """Helper class for constructing Amazon Comprehend Document Classifier console link."""

    name = "Document Classifier"
    key = "comprehend_document_classifier"
    format_str = (
        BASE_AWS_CONSOLE_LINK + "/comprehend/home?region={region_name}#" + "classifier-version-details/{arn}"
    )
