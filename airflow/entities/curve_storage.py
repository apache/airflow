# -*- coding:utf-8 -*-

import io
from typing import Dict, Optional, List
from tablib import Dataset
import uuid
from minio import Minio
from minio.error import (ResponseError, BucketAlreadyOwnedByYou,
                         BucketAlreadyExists)
from .entity import ClsEntity
from airflow.utils.logger import generate_logger
import threading

_logger = generate_logger(__name__)


class ClsCurveStorage(ClsEntity):
    _instance_lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not hasattr(ClsCurveStorage, "_instance"):
            with ClsCurveStorage._instance_lock:
                if not hasattr(ClsCurveStorage, "_instance"):
                    ClsCurveStorage._instance = object.__new__(cls)
        return ClsCurveStorage._instance

    def __init__(self, endpoint, access_key, secret_key, secure, bucket):
        super(ClsCurveStorage, self).__init__()
        if not self.is_config_changed(endpoint, access_key, secret_key, secure, bucket):
            return
        self._access_key = access_key
        self._secret_key = secret_key
        self._secure = secure
        self._url = endpoint
        self._bucket = bucket
        self._fileName = ""  # type: str
        self._headersMap = {
            "cur_w": u'角度',
            "cur_m": u'扭矩',
            "cur_t": u'时间',
            "cur_s": u'转速'
        }
        self._client = None  # type: Optional[Minio]

    def is_config_changed(self, endpoint, access_key, secret_key, secure, bucket):
        try:
            if self._access_key != access_key:
                return True
            if self._secret_key != secret_key:
                return True
            if self._secure != secure:
                return True
            if self._url != endpoint:
                return True
            if self._bucket != bucket:
                return True
            return False
        except Exception as e:
            return True

    @property
    def ObjectName(self):
        entity_id = self.entity_id
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
        if len(datamap) < 4:
            raise BaseException(u'数据缺失')
        zipData = zip(datamap[0], datamap[1], datamap[2], datamap[3])
        for d in zipData:
            data.append(d)
        return data.export('csv').encode('utf-8')

    def ensure_connect(self):
        if not self._client:
            self.connect()

    def remove_curves(self, curve_files: Optional[List] = None) -> bool:
        ret = False
        if not curve_files:
            return ret
        try:
            self.ensure_bucket(self._bucket)
            ret = self._client.remove_objects(self._bucket, curve_files)
            if not ret:
                raise Exception('Remove Object: {} Error'.format(','.join(curve_files)))
        except Exception as e:
            raise e
        return ret

    def write_curve(self, data: Optional[Dict] = None) -> None:
        if not data:
            raise Exception(u"未传入数据!")
        curve = data.get('curve', None)
        if not curve:
            raise Exception(u"未传入曲线!")
        try:
            self.ensure_bucket(self._bucket)
            data = self.convertCSVData(curve)
            _logger.info(data)
            f = io.BytesIO(data)  # 必须转换成rawIO数据
            self._client.put_object(
                self._bucket, self.ObjectName, f, length=len(data))

        except Exception as err:
            raise Exception(u"写入曲线失败: {}".format(repr(err)))

    def csv_data_to_dict(self, data):
        data_set = Dataset()
        f = io.StringIO(data)
        data_set.headers = self._headersMap.values()
        headers = f.readline()
        ret = {
            'cur_w': [],
            'cur_m': [],
            'cur_t': [],
            'cur_s': []
        }
        for row in f.readlines():
            row_data = row.split('\r\n')[0].split(',')
            ret['cur_w'].append(float(row_data[0]))
            ret['cur_m'].append(float(row_data[1]))
            ret['cur_t'].append(float(row_data[2]))
            ret['cur_s'].append(float(row_data[3]))
        return ret

    def query_curve(self):
        self.ensure_bucket(self._bucket)
        resp = self._client.get_object(self._bucket, self.ObjectName)
        csv_data = resp.data.decode('utf-8')

        dict_data = self.csv_data_to_dict(csv_data)
        return dict_data
