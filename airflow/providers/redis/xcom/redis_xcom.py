import json
import logging
import hashlib
from json import JSONDecodeError
import pendulum
from typing import Any, Iterable, Optional, Union

from airflow.configuration import conf
from airflow.models.xcom import BaseXCom
from airflow.providers.redis.hooks.redis import RedisHook

log = logging.getLogger(__name__)
redis_conn_id = conf.get('xcom', 'redis_conn_id', fallback=RedisHook.default_conn_name)

class RedisXCom(BaseXCom):

    @classmethod
    def delete(cls, xcoms):
        redis_hook = RedisHook(redis_conn_id=redis_conn_id)
        """Delete Xcom"""
        if isinstance(xcoms, RedisXCom):
            xcoms = [xcoms]
        for xcom in xcoms:
            if not isinstance(xcom, RedisXCom):
                raise TypeError(f'Expected RedisXCom; received {xcom.__class__.__name__}')
            redis_hook.get_conn().delete(key=xcom.value)

    @staticmethod
    def serialize_value(value: Any):
        key = f'airflow.xcom.{hashlib.md5(value.encode()).hexdigest()}'
        redis_hook = RedisHook(redis_conn_id=redis_conn_id)
        log.info('Setting XCom key %s to Redis', key)

        result = redis_hook.get_conn().set(key=key, value=value)

        log.info('Result of publishing %s', result)
        return key

    @staticmethod
    def deserialize_value(result: "XCom") -> Any:
        redis_hook = RedisHook(redis_conn_id=redis_conn_id)
        result = redis_hook.get_conn().get(key=result.value)
        return result
