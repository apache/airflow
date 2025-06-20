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
"""This module contains Google Dataflow links."""

from __future__ import annotations

from airflow.providers.google.cloud.links.base import BaseGoogleLink

DATAFLOW_BASE_LINK = "/dataflow/jobs"
DATAFLOW_JOB_LINK = DATAFLOW_BASE_LINK + "/{region}/{job_id}?project={project_id}"

DATAFLOW_PIPELINE_BASE_LINK = "/dataflow/pipelines"
DATAFLOW_PIPELINE_LINK = DATAFLOW_PIPELINE_BASE_LINK + "/{location}/{pipeline_name}?project={project_id}"


class DataflowJobLink(BaseGoogleLink):
    """Helper class for constructing Dataflow Job Link."""

    name = "Dataflow Job"
    key = "dataflow_job_config"
    format_str = DATAFLOW_JOB_LINK


class DataflowPipelineLink(BaseGoogleLink):
    """Helper class for constructing Dataflow Pipeline Link."""

    name = "Dataflow Pipeline"
    key = "dataflow_pipeline_config"
    format_str = DATAFLOW_PIPELINE_LINK
