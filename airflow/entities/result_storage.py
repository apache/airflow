# -*- coding:utf-8 -*-
from typing import Dict, Optional
from influxdb_client import InfluxDBClient, Point, WriteApi
from .entity import ClsEntity
from airflow.utils.logger import generate_logger
import os

RUNTIME_ENV = os.environ.get('RUNTIME_ENV', 'dev')

_logger = generate_logger(__name__)
IS_DEBUG = RUNTIME_ENV != 'prod'


class ClsResultStorage(ClsEntity):
    def __init__(self, url, bucket, token, ou, write_options):
        super(ClsResultStorage, self).__init__()
        self._token = token
        self._ou = ou
        self._client = None
        self._bucket = bucket
        self._url = url
        self.write_options = write_options

    @property
    def endpoint(self):
        return self._url

    def ensure_connect(self):
        if self._client:
            return
        self.connect()
        if self._client:
            return
        raise BaseException(u'{} 无法创建连接'.format(__class__.__name__))

    def connect(self):
        if not self.endpoint:
            raise BaseException(u'{} 地址未定义'.format(__class__.__name__))
        self._client = InfluxDBClient(
            self.endpoint,
            org=self._ou,
            token=self._token,
            debug=IS_DEBUG)

    @property
    def write_api(self) -> Optional[WriteApi]:
        if not self._client or not self.write_options:
            return None
        return self._client.write_api(write_options=self.write_options)

    @property
    def query_api(self) -> Optional[WriteApi]:
        if not self._client:
            return None
        return self._client.query_api()

    def _write(self, data: Point) -> None:
        if not self.write_api or not self._bucket:
            raise BaseException(u'请先进行连接')
        return self.write_api.write(bucket=self._bucket, record=data)

    def _query(self, query_str) -> Point:
        if not self.query_api:
            raise BaseException(u'请先进行连接')
        result = self.query_api.query(query_str)
        return result

    def package_result_point(self, data: Dict) -> Optional[Point]:
        sn = data.pop('tool_sn') if data.get('tool_sn', None) else None
        if not sn:
            raise BaseException(u'未定义工具序列号')
        p = Point('results').tag('tool_sn', sn).tag('entity_id', self.entity_id)
        for key, value in data.items():
            p.field(key, value)
        return p

    @staticmethod
    def result_pkg_validator(data: Optional[Dict]) -> bool:
        ret = False
        if not data:
            return ret
        result_body: Optional[Dict] = data.get('result', None)
        curveFile: str = data.get('curveFile', None)
        if not curveFile or not result_body:
            return False
        return True

    def write_result(self, data: Optional[Dict]):
        if not self.result_pkg_validator(data):
            return
        entity_id = self.entity_id
        if not entity_id:
            _logger.error("entity id Is Required!")
            return
        result_body: Optional[Dict] = data.get('result')  # 之前验证过了 无需再验证有效性
        self.ensure_connect()
        result_body.update({"entity_id": entity_id})  # 将 entity id 也保存到数据库中
        result = self.package_result_point(result_body)
        return self._write(result)

    def query_result(self):
        self.ensure_connect()
        if not self.entity_id:
            raise BaseException(u'entity_id未指定')
        if not self._bucket:
            raise BaseException(u'_bucket未指定')
        query_str = 'from(bucket:"{}")' \
                    ' |> range(start: 0, stop: now())' \
                    ' |> filter(fn: (r) => r.entity_id == "{}")'.format(self._bucket, self.entity_id)
        result = self._query(query_str)
        for table in result:
            for record in table.records:
                print(record.values)
        return result
