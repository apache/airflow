# -*- coding:utf-8 -*-
import os
import json
import pytz
import datetime
import threading
from typing import Dict, Optional
from dateutil.parser import parse
from distutils.util import strtobool
from sqlalchemy.engine.base import Engine
from sqlalchemy import text
from airflow.utils.db import create_session
from airflow.utils.logger import generate_logger

from .entity import ClsEntity
from plugins.result_storage.base import Base
from plugins.result_storage.model import ResultModel


RUNTIME_ENV = os.environ.get('RUNTIME_ENV', 'dev')
ENV_TIMESCALE_ENABLE = strtobool(os.environ.get('ENV_TIMESCALE_ENABLE', 'false'))

_logger = generate_logger(__name__)
IS_DEBUG = RUNTIME_ENV != 'prod'


class ClsResultStorage(ClsEntity):
    _instance_lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not hasattr(ClsResultStorage, "_instance"):
            with ClsResultStorage._instance_lock:
                if not hasattr(ClsResultStorage, "_instance"):
                    ClsResultStorage._instance = object.__new__(cls)
        return ClsResultStorage._instance

    def __init__(self, engine: Engine):
        super(ClsResultStorage, self).__init__()
        self.engine = engine
        self.create_table_if_existed()

    def create_table_if_existed(self):
        if not self.engine.dialect.has_table(self.engine, ResultModel.__tablename__):
            Base.metadata.create_all(self.engine)
            if not ENV_TIMESCALE_ENABLE:
                return
            with self.engine.connect().execution_options(autocommit=True) as conn:
                conn.execute(text(f'''SELECT create_hypertable('{ResultModel.__tablename__}', 'update_time','tool_sn', 4, chunk_time_interval => INTERVAL '1 month', migrate_data => TRUE);'''))

    def _write(self, data: Optional[Dict]):
        try:
            with create_session() as session:
                result = ResultModel(**data)
                session.add(result)
        except Exception as e:
            _logger.error("Error: {}".format(e))

    def _filter(self, params) -> list:
        try:
            with create_session() as session:
                return session.query(ResultModel).filter(params).all()
        except Exception as e:
            _logger.error("Error: {}".format(e))

    @staticmethod
    def result_pkg_validator(data: Optional[Dict]):
        if not data:
            raise Exception('empty result package data')
        result_body: Optional[Dict] = data.get('result', None)
        curveFile: str = data.get('curveFile', None)
        if curveFile and result_body:
            return
        msg = ''
        if not curveFile:
            msg += 'empty curve data;'
        if not result_body:
            msg += 'empty result data;'
        raise Exception(msg)

    def write_result(self, data: Optional[Dict]):
        try:
            self.result_pkg_validator(data)
            entity_id = self.entity_id
            if not entity_id:
                raise Exception("entity id Is Required!")
            result_body: Optional[Dict] = data.get('result')  # 之前验证过了 无需再验证有效性
            step_results = data.get("step_results")
            if step_results and (isinstance(step_results, list) or isinstance(step_results, dict)):
                step_results = json.dumps(step_results, ensure_ascii=False)
            result_body.update({"entity_id": entity_id, "step_results": step_results})
            return self._write(result_body)
        except Exception as err:
            raise Exception(u"写入结果失败: {}, result: {}".format(repr(err), repr(data)))

    def query_results(self):
        if not self.entity_id:
            raise BaseException(u'entity_id未指定')
        if not isinstance(self.entity_id, list):
            raise BaseException(u'entity_id必须是列表')

        data = self._filter(ResultModel.entity_id.in_(self.entity_id))
        ret = []
        if not data:
            return ret
        for table in data:
            if isinstance(table, ResultModel):
                ret.append(table.as_dict())
        return ret

    def query_result(self):
        if not self.entity_id:
            raise BaseException(u'entity_id未指定')
        data = self._filter(ResultModel.entity_id == self.entity_id)
        for table in data:
            ret = table.as_dict()
            if ret.get('step_results'):
                ret['step_results'] = json.loads(ret['step_results'])
            return ret
        return None
