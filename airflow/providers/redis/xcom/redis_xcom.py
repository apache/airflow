import json
import logging
import hashlib
from typing import Any

from airflow.configuration import conf
from airflow.models.xcom import BaseXCom
from airflow.providers.redis.hooks.redis import RedisHook

log = logging.getLogger(__name__)
redis_conn_id = conf.get('xcom', 'redis_conn_id', fallback=RedisHook.default_conn_name)
redis_hook = RedisHook(redis_conn_id=redis_conn_id)


class RedisXCom(BaseXCom):

    @staticmethod
    def _gen_key(val: Any):
        return f'airflow.xcom.{hashlib.md5(val).hexdigest()}'

    @classmethod
    def delete(cls, xcom):
        """Delete XCom value from Redis"""
        val = json.dumps(xcom.value).encode('UTF-8')
        redis_hook.get_conn().delete(RedisXCom._gen_key(val))

    @staticmethod
    def serialize_value(value: Any):
        val = json.dumps(value).encode('UTF-8')
        key = RedisXCom._gen_key(val)

        log.debug('Setting XCom key %s to Redis', key)
        result = redis_hook.get_conn().set(key, val)
        log.debug('Result of publishing to Redis %s', result)
        return key.encode()

    @staticmethod
    def deserialize_value(result: "XCom") -> Any:
        result = redis_hook.get_conn().get(result.value)
        return json.loads(result.decode('UTF-8')) if result is not None \
            else '** XCom not found in Redis **'
