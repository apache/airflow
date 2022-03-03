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

# pylint: disable=invalid-name


import json

from aliyunsdkcore.auth.credentials import AccessKeyCredential
from aliyunsdkcore.client import AcsClient
from aliyunsdkcore.request import CommonRequest

from airflow.hooks.base_hook import BaseHook


class AliyunEmrHook(BaseHook):
    def __init__(self, conn_id='oss_default', region=None):
        self.conn_id = conn_id
        self.region = region
        self.conn = self.get_connection(conn_id)
        self.client = self.get_client()

    def create_cluster_from_template(self, template_id, cluster_name):
        request = CommonRequest()
        request.set_accept_format('json')
        request.set_domain("emr-vpc." + self.region + ".aliyuncs.com")
        request.set_method('POST')
        request.set_protocol_type('https')
        request.set_version('2016-04-08')
        request.set_action_name('CreateClusterWithTemplate')
        request.add_query_param('TemplateBizId', template_id)
        request.add_query_param('ClusterName', cluster_name)
        request.set_accept_format('json')
        response = json.loads(str(self.client.do_action_with_exception(request), encoding='utf-8'))
        self.log.info(response)
        return response['ClusterId']

    def tear_down_cluster(self, cluster_id):
        request = CommonRequest()
        request.set_accept_format('json')
        request.set_domain("emr-vpc." + self.region + ".aliyuncs.com")
        request.set_method('POST')
        request.set_protocol_type('https')  # https | http
        request.set_version('2016-04-08')
        request.set_action_name('ReleaseCluster')
        request.add_query_param('RegionId', self.region)
        request.add_query_param('Id', cluster_id)
        response = json.loads(str(self.client.do_action(request), encoding='utf-8'))
        self.log.info(response)

    def get_cluster_info(self, cluster_id):
        request = CommonRequest()
        request.set_accept_format('json')
        request.set_domain("emr-vpc." + self.region + ".aliyuncs.com")
        request.set_method('POST')
        request.set_protocol_type('https')  # https | http
        request.set_version('2016-04-08')
        request.set_action_name('DescribeClusterV2')
        request.add_query_param('RegionId', self.region)
        request.add_query_param('Id', cluster_id)
        response = json.loads(str(self.client.do_action(request), encoding='utf-8'))
        return response

    def get_client(self):
        # self.log.info('trying to establish connection...')

        extra_config = self.conn.extra_dejson
        auth_type = extra_config.get('auth_type', None)
        if not auth_type:
            raise Exception("No auth_type specified in extra_config. ")

        if auth_type == 'AK':
            access_key_id = extra_config.get('access_key_id', None)
            access_key_secret = extra_config.get('access_key_secret', None)
            region = extra_config.get('region', None)
            if not access_key_id:
                raise Exception("No access_key_id is specified for connection: " + self.conn_id)
            if not access_key_secret:
                raise Exception("No access_key_secret is specified for connection: " + self.conn_id)
            if not self.region:
                if not region:
                    raise Exception("No region is specified for connection: " + self.conn_id)
                self.region = region

        else:
            raise Exception("Unsupported auth_type: " + auth_type)

        credentials = AccessKeyCredential(access_key_id, access_key_secret)
        self.client = AcsClient(region_id=self.region, credential=credentials)
        return self.client
