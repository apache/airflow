# -*- coding:utf-8 -*-

import io
from typing import Dict, Optional
from tablib import Dataset
import uuid
from minio import Minio
from minio.error import (ResponseError, BucketAlreadyOwnedByYou,
                         BucketAlreadyExists)
from .entity import ClsEntity
from airflow.utils.logger import generate_logger

_logger = generate_logger(__name__)


class ClsCurveStorage(ClsEntity):
    def __init__(self, endpoint, access_key, secret_key, secure, bucket):
        super(ClsCurveStorage, self).__init__()
        self._access_key = access_key
        self._secret_key = secret_key
        self._client = None  # type: Optional[Minio]
        self._secure = secure
        self._url = endpoint
        self._bucket = bucket
        self._fileName = ""  # type: str
        self._headersMap = {
            "cur_w": u'角度',
            "cur_m": u'扭矩',
            "cur_t": u'时间'
        }

    @property
    def ObjectName(self):
        entity_id = self.entity_id
        if self._fileName:
            return self._fileName
        if entity_id:
            self._fileName = "{}.csv".format(entity_id)
        else:
            self._fileName = "{}.csv".format(str(uuid.uuid4()))
        return self._fileName

    @property
    def endpoint(self):
        return self._url

    def connect(self):
        if not self.endpoint:
            raise BaseException(u'{} 地址未定义'.format(__class__.__name__))
        self._client = Minio(self.endpoint,
                             access_key=self._access_key,
                             secret_key=self._secret_key,
                             secure=self._secure)

    def ensure_bucket(self, bucket):
        self.ensure_connect()
        try:
            self._client.make_bucket(bucket)
        except BucketAlreadyOwnedByYou:
            pass
        except BucketAlreadyExists:
            pass
        except ResponseError as err:
            raise err

    def convertCSVData(self, curve: Dict):
        data = Dataset()
        data.headers = self._headersMap.values()
        datamap = [curve.get(key, []) for key in self._headersMap.keys()]
        if len(datamap) != 3:
            raise BaseException(u'数据长度不对')
        zipData = zip(datamap[0], datamap[1], datamap[2])
        for d in zipData:
            data.append(d)
        return data.export('csv').encode('utf-8')

    def ensure_connect(self):
        if not self._client:
            self.connect()

    def write_curve(self, data: Optional[Dict] = None) -> None:
        if not data:
            _logger.error(u"未传入数据!")
            return
        curve = data.get('curve', None)
        if not curve:
            _logger.error(u"未传入曲线!")
            return
        try:
            self.ensure_bucket(self._bucket)
            data = self.convertCSVData(curve)
            f = io.BytesIO(data)  # 必须转换成rawIO数据
            self._client.put_object(
                self._bucket, self.ObjectName, f, length=len(data))

        except Exception as err:
            _logger.error(u"写入曲线 失败: {}".format(err))

