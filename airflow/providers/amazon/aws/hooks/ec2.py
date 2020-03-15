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
#

from typing import Optional

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class EC2Hook(AwsBaseHook):
    """
    Interact with AWS EC2 Service.
    """

    def __init__(self,
                 region_name: Optional[str] = None,
                 *args,
                 **kwargs):
        self.region_name = region_name
        self.conn = None
        super().__init__(*args, **kwargs)

    def get_conn(self):
        """
        Return self.conn, if it is None initialize ec2 resource object.

        :return: ec2 resource
        :rtype: boto3.resource
        """
        # set self.conn in first call
        if not self.conn:
            self.conn = self.get_resource_type("ec2", self.region_name)
        return self.conn
