from airflow.plugins_manager import AirflowPlugin
from airflow.hooks.base_hook import BaseHook
import os
from aiohttp_retry import RetryClient
from aiohttp import ClientTimeout
import json
from http import HTTPStatus
import requests

CAS_ANALYSIS_BASE_URL = os.environ.get("CAS_ANALYSIS_BASE_URL", "http://localhost:9095")
CAS_TRAINING_BASE_URL = os.environ.get("CAS_TRAINING_BASE_URL", "http://localhost:9095")


class CasHook(BaseHook):
    def __init__(self, role='all'):
        super(CasHook, self).__init__()
        if role == 'analysis':
            conn_id = 'cas_analysis'
        elif role == 'training':
            conn_id = 'cas_training'
        else:
            conn_id = 'cas_server'

        self.conn_id = conn_id

        try:
            from airflow.models.connection import Connection
            self.connection = Connection.get_connection_from_secrets(conn_id)
        except Exception as e:
            self.log.error(e)
            self.connection = None
        if self.connection:
            self.extras = self.connection.extra_dejson.copy()
            self.uri = '{scheme}://{host}{port}'.format(
                scheme='http',
                host=self.connection.host,
                port='' if self.connection.port is None else ':{}'.format(self.connection.port)
            )
        else:
            if role == 'analysis':
                self.uri = CAS_ANALYSIS_BASE_URL
            elif role == 'training':
                self.uri = CAS_TRAINING_BASE_URL
            else:
                self.uri = CAS_ANALYSIS_BASE_URL

    @property
    def trigger_analyze_endpoint(self):
        return '{}/cas/analysis'.format(self.uri)

    @property
    def load_templates_endpoint(self):
        return '{}/cas/templates'.format(self.uri)

    @property
    def trigger_training_endpoint(self):
        return "{}/cas/invalid-curve".format(self.uri)

    async def trigger_analyze(self, params, timeout=ClientTimeout(total=30), retry_attempts=5):
        headers = {
            'Accept': 'application/json',
            'Content-type': 'application/json'
        }
        data = {
            'conf': params
        }
        try:
            url = self.trigger_analyze_endpoint
            if not params:
                raise Exception(u'数据为空')
            if not params.get('curve_param', None):
                raise Exception(u'未提供曲线参数')
            self.log.info('参数验证通过，触发分析...')
            async with RetryClient(timeout=timeout) as client:
                async with client.post(headers=headers, url=url, retry_attempts=retry_attempts, json=data) as r:
                    r.raise_for_status()
                    resp = await r.read()
                    self.log.debug("trigger training: {}, resp: {}".format(
                        json.dumps(data), resp))
                    return resp
        except BaseException as e:
            self.log.error(
                "push_result_to_training_server except: {}".format(repr(e)))
            raise e

    async def training_server_update_templates(self, timeout=ClientTimeout(total=300), retry_attempts=5):
        headers = {
            'Accept': 'application/json',
            'Content-type': 'application/json'
        }
        async with RetryClient(timeout=timeout) as client:
            async with client.post(headers=headers, url=self.load_templates_endpoint,
                                   retry_attempts=retry_attempts) as r:
                r.raise_for_status()
                resp = await r.read()
                self.log.debug(
                    "training_server_update_templates called, url: {} resp: {}".format(self.load_templates_endpoint,
                                                                                       resp))

    def trigger_training(self, data):
        json_data = {
            'conf': data
        }
        try:
            self.log.info('posting to training server')
            self.log.debug('data:{}'.format(json.dumps(json_data, indent=4)))
            resp = requests.post(headers={'Content-Type': 'application/json'}, url=self.trigger_training_endpoint,
                                 json=json_data,
                                 timeout=(3.05, 27))
            self.log.info('training server response')
            if resp.status_code != HTTPStatus.OK:
                raise Exception(resp.content)
        except Exception as e:
            self.log.error(repr(e))
            raise Exception(str(e))


class CasPlugin(AirflowPlugin):
    name = "cas_plugin"

    hooks = [CasHook]

    @classmethod
    def on_load(cls, *args, **kwargs):
        pass
