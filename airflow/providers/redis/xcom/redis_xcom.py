import json
import logging
import pendulum
from typing import Any

from airflow.configuration import conf
from airflow.models.xcom import BaseXCom
from airflow.providers.redis.hooks.redis import RedisHook

log = logging.getLogger(__name__)
redis_conn_id = conf.get('xcom', 'redis_conn_id', fallback=RedisHook.default_conn_name)
redis_hook = RedisHook(redis_conn_id=redis_conn_id)


class RedisXCom(BaseXCom):

    @staticmethod
    def _gen_key(key: str, execution_date: pendulum.DateTime, task_id: str, dag_id: str):
        """
        Generate a Redis key from XCom Candidate keys
        ex "example_xcom.push.valuefrompusher1202011301618318228340000"
        """
        key = f'{key}{execution_date}'
        key = ''.join(filter(str.isalnum, key))
        return f'{dag_id}.{task_id}.{key}'

    @classmethod
    def delete(cls, xcom):
        """Delete XCom value from Redis"""
        key = RedisXCom._gen_key(xcom.key, xcom.execution_date, xcom.task_id, xcom.dag_id)
        log.info(key)
        result = redis_hook.get_conn().delete(key)
        log.info("result %s", result)

    @staticmethod
    def serialize_value(key: str, value: Any, execution_date: pendulum.DateTime, task_id: str, dag_id: str):
        val = json.dumps(value).encode('UTF-8')
        key = RedisXCom._gen_key(key, execution_date, task_id, dag_id)

        log.debug('Setting XCom key %s to Redis', key)
        result = redis_hook.get_conn().set(key, val)
        log.debug('Result of publishing to Redis %s', result)
        return key.encode()

    @staticmethod
    def deserialize_value(result: "XCom") -> Any:
        result = redis_hook.get_conn().get(result.value)
        return json.loads(result.decode('UTF-8')) if result is not None \
            else '** XCom not found in Redis **'
