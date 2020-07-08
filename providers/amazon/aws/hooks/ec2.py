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

import time

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class EC2Hook(AwsBaseHook):
    """
    Interact with AWS EC2 Service.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self,
                 *args,
                 **kwargs):
        super().__init__(resource_type="ec2", *args, **kwargs)

    def get_instance(self, instance_id: str):
        """
        Get EC2 instance by id and return it.

        :param instance_id: id of the AWS EC2 instance
        :type instance_id: str
        :return: Instance object
        :rtype: ec2.Instance
        """
        return self.conn.Instance(id=instance_id)

    def get_instance_state(self, instance_id: str) -> str:
        """
        Get EC2 instance state by id and return it.

        :param instance_id: id of the AWS EC2 instance
        :type instance_id: str
        :return: current state of the instance
        :rtype: str
        """
        return self.get_instance(instance_id=instance_id).state["Name"]

    def wait_for_state(self,
                       instance_id: str,
                       target_state: str,
                       check_interval: float) -> None:
        """
        Wait EC2 instance until its state is equal to the target_state.

        :param instance_id: id of the AWS EC2 instance
        :type instance_id: str
        :param target_state: target state of instance
        :type target_state: str
        :param check_interval: time in seconds that the job should wait in
            between each instance state checks until operation is completed
        :type check_interval: float
        :return: None
        :rtype: None
        """
        instance_state = self.get_instance_state(
            instance_id=instance_id
        )
        while instance_state != target_state:
            self.log.info("instance state: %s", instance_state)
            time.sleep(check_interval)
            instance_state = self.get_instance_state(
                instance_id=instance_id
            )
