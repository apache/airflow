# -*- coding:utf-8 -*-
import datetime
import os
import json
import threading
from typing import Dict, Optional
from airflow.utils.db import create_session
from plugins.utils.logger import generate_logger
from plugins.entities.entity import ClsEntity
from plugins.models.result import ResultModel
from airflow.utils.db import provide_session
from psycopg2 import errors
from sqlalchemy.exc import IntegrityError
RUNTIME_ENV = os.environ.get('RUNTIME_ENV', 'dev')

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

    def _write(self, data: Optional[Dict]):
        try:
            with create_session() as session:
                result = ResultModel(**data)
                session.add(result)
        except IntegrityError as e:
            assert isinstance(e.orig, errors.UniqueViolation)  # proves the original exception
            _logger.error("Error: {}".format(e))
        except Exception as e:
            _logger.error("Error: {}".format(e))
            raise e

    def update(self, **data):
        try:
            with create_session() as session:
                session.query(ResultModel) \
                    .filter(ResultModel.entity_id == self.entity_id) \
                    .update(data)
        except Exception as e:
            _logger.error("Error: {}".format(e))
            raise e

    def _filter(self, params) -> list:
        try:
            with create_session() as session:
                return session.query(ResultModel).filter(params).all()
        except Exception as e:
            _logger.error("Error: {}".format(e))

    @staticmethod
    def result_pkg_validator(data: Optional[Dict]):
        if not data:
            raise Exception('empty result data')

    def write_result(self, data: Optional[Dict]):
        try:
            self.result_pkg_validator(data)
            entity_id = self.entity_id
            if not entity_id:
                raise Exception("entity id Is Required!")
            result_body: Optional[Dict] = data  # 之前验证过了 无需再验证有效性
            step_results = data.get("step_results")
            if step_results and (isinstance(step_results, list) or isinstance(step_results, dict)):
                step_results = json.dumps(step_results, ensure_ascii=False)
            update_time = data.get("update_time")
            if update_time and isinstance(update_time, str):
                update_time = update_time.replace('Z', '+00:00')
                update_time = datetime.datetime.fromisoformat(update_time)
            result_body.update({
                "entity_id": entity_id,
                "step_results": step_results,
                "update_time": update_time
            })
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
        if not data:
            return None
        for table in data:
            ret = table.as_dict()
            if ret.get('step_results'):
                ret['step_results'] = json.loads(ret['step_results'])
            return ret
        return None

    @staticmethod
    @provide_session
    def modify_result(entity_id, modifier=None, session=None):
        if not modifier:
            return
        results = session.query(ResultModel).filter(
            ResultModel.entity_id == entity_id)
        result_altered = results.with_for_update().first()
        if not result_altered:
            return
        modifier(result_altered)
