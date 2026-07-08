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

from __future__ import annotations

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class NeptuneAnalyticsHook(AwsBaseHook):
    """
    Interact with Amazon Neptune Analytics.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, *args, **kwargs):
        kwargs["client_type"] = "neptune-graph"
        super().__init__(*args, **kwargs)

    def _get_graph_endpoint_id(self, graph_id: str, vpc_id: str):
        """Return the vpc endpoint id for this graph."""
        result = self.conn.get_private_graph_endpoint(graphIdentifier=graph_id, vpcId=vpc_id)
        return result.get("vpcEndpointId")
