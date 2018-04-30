# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
import requests
import logging
import json


class DingDingAPIOperator(BaseOperator):
    """
    Base DingDingAPI Operator
    The DingDingAPIOperator is derived from this operator.
    In the future additional DingDing API Operators will be 
    derived from this class as well
    :param token: DingDing API token (https://open-doc.dingtalk.com)
    :type token: string
    :param api_params: API Method call parameters 
    (https://open-doc.dingtalk.com/docs/doc.htm?
    spm=0.0.0.0.Aed6Oe&treeId=385&articleId=104981&docType=1)
    :type api_params: dict
    """

    @apply_defaults
    def __init__(self,
                 token='unset',
                 api_params=None,
                 *args, **kwargs):
        super(DingDingAPIOperator, self).__init__(*args, **kwargs)
        self.token = token
        self.api_params = api_params

    def construct_api_call_params(self):
        """
        Used by the execute function. Allows templating on the source fields of 
        the api_call_params dict before construction Override in child classes.
        Each DingDingAPIOperator child class is responsible for having a 
        construct_api_call_params function
        which sets self.api_call_params with a dict of API call parameters
        (https://open-doc.dingtalk.com/docs/doc.htm?
         spm=a219a.7629140.0.0.03YixU&treeId=257&articleId=105735&docType=1)
        """
        pass

    def execute(self, **kwargs):
        """
        DingDingAPIOperator calls will not fail even if the call is not unsuccessful.
        It should not prevent a DAG from completing in success
        """
        if not self.api_params:
            self.construct_api_call_params()
        r = self.do(self.token, self.api_params)
        logging.info("DingDingRobot API call return ({})".format(r.text))
        if json.loads(r.text)['errcode'] != 0:
            logging.error("DingDingRobot API call failed ({})".format(r.text))
            raise AirflowException(
                "DingDing API call failed: ({})".format(
                    r.text))

    def do(
            self,
            token=None,
            post_data=None,
            domain="oapi.dingtalk.com",
            timeout=None):
        """
        Perform a POST request to the DingDing Robot  API
        Args:
            token (str): your authentication token
            timeout (float): stop waiting for a response after a given number of seconds
            post_data (dict): key/value arguments to pass for the request.
            domain (str): if for some reason you want to send your request to 
            something other than oapi.dingtalk.com
        """
        post_data = post_data or {}
        url = 'https://{0}/robot/send?access_token={1}'.format(domain, token)
        headers = {"Content-Type": "application/json; charset=utf-8"}
        return requests.post(url,
                             headers=headers,
                             data=json.dumps(post_data),
                             timeout=timeout
                             )


class DingDingAPITextOperator(DingDingAPIOperator):
    """
    Posts messages to a DingDing Robot

    :param content: message to send to dingding
    :type content: string
    :param atMobiles: mobiles list to @
    :type atMobiles: array of mobile number
    :param isAtAll: @ ALL or Not
    :type isAtAll: bool
    """
    template_fields = ('content', 'atMobiles', 'isAtAll')

    @apply_defaults
    def __init__(self,
                 content='No message has been set.',
                 atMobiles=[],
                 isAtAll=True,
                 msgtype="text",
                 *args, **kwargs):
        self.content = content,
        self.atMobiles = atMobiles,
        self.isAtAll = isAtAll
        self.msgtype = msgtype
        super(DingDingAPITextOperator, self).__init__(*args, **kwargs)

    def construct_api_call_params(self):
        self.api_params = {
            'msgtype': self.msgtype,
            'text': {'content': self.content},
            "at": {
                'atMobiles': self.atMobiles,
                'isAtAll': self.isAtAll
            }
        }


class DingDingAPILinkOperator(DingDingAPIOperator):
    """
    Posts messages to a DingDing Robot

    :param text: text message to send to dingding
    :type text: string
    :param title: message title
    :type title: string
    :param picUrl: url link to show a pic in dingding ,default airflow icon
    :type picUrl: string
    :param messageUrl: url link to show message ,default airflow doc link
    :tpye messageUrl: string
    """

    template_fields = ('text', 'title', 'picUrl', 'messageUrl')

    @apply_defaults
    def __init__(self,
                 text='No message has been set.',
                 title='No title has been set .',
                 picUrl='http://airbnb.io/img/projects/airflow3.png',
                 messageUrl='https://airflow.incubator.apache.org/',
                 msgtype="link",
                 *args, **kwargs):
        self.text = text,
        self.title = title,
        self.picUrl = picUrl
        self.messageUrl = messageUrl
        self.msgtype = msgtype
        super(DingDingAPILinkOperator, self).__init__(*args, **kwargs)

    def construct_api_call_params(self):
        self.api_params = {
            'msgtype': self.msgtype,
            'link': {
                'text': self.text,
                'title': self.title,
                'picUrl': self.picUrl,
                'messageUrl': self.messageUrl
            }
        }
