import json
from redis import Redis
from airflow.contrib.hooks.redis_hook import RedisHook
from typing import Dict
from .entity import ClsEntity
from airflow.utils.logger import generate_logger
import os
import signal
from redis.client import Pipeline
import time
from airflow.utils.curve import gen_template_key, trigger_push_templates_dict_dag

RUNTIME_ENV = os.environ.get('RUNTIME_ENV', 'dev')

_logger = generate_logger(__name__)
IS_DEBUG = RUNTIME_ENV != 'prod'


class ClsRedisConnection(ClsEntity):
    _channels = {}

    def __init__(self):
        super(ClsEntity, self).__init__()
        self.msg_type = 'pmessage'  # for pmessage 模式匹配
        self.redis_conn_id = 'qcos_redis'
        self._redis: Redis = RedisHook(redis_conn_id=self.redis_conn_id).get_conn()
        self._pubsub = self._redis.pubsub()
        self.end = False
        self.poke_interval = 5

    @property
    def redis(self):
        if not self._redis:
            raise Exception('请先初始化redis连接')
        return self._redis

    @property
    def pubsub(self):
        return self._pubsub

    @property
    def pipeline(self) -> Pipeline:
        return self.redis.pipeline()

    def doSubscribe(self):
        if not self.pubsub:
            return
        self.pubsub.psubscribe(**self.channels)

    def doUnsubscribe(self):
        if not self.pubsub:
            return
        self.pubsub.punsubscribe()

    def signal_handler(self, sig, frame):
        _logger.info('redis connection ended by signal {}, frame: {}'.format(sig, frame))
        self.end = True

    def set_signal_handler(self):
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGCHLD, self.signal_handler)
        signal.signal(signal.SIGHUP, self.signal_handler)

    def store_templates(self, templates: Dict):
        # 将模板存储到redis中
        _logger.debug("storing templates...")
        # _logger.info("Template Data: {}".format(pprint.pformat(templates, indent=4)))
        # 触发模板推送到rabbit mq
        trigger_push_templates_dict_dag(templates)
        with self.pipeline as p:
            for key, val in templates.items():
                _logger.debug("storing template, key: {} ...".format(key))
                channel = gen_template_key(key)
                p.set(channel, value=val)
            p.execute()

    def remove_templates(self, template_keys: list):
        with self.pipeline as p:
            _logger.info(template_keys)
            p.delete(*template_keys)
            p.execute()

    def read_template_keys(self):
        template_keys = self.redis.keys(gen_template_key('*'))
        return template_keys

    def run(self):
        self.set_signal_handler()
        while not self.end:
            if not self.pubsub:
                time.sleep(self.poke_interval)
                continue
            msg = self.pubsub.get_message(timeout=self.poke_interval)
            # if msg and msg.get('type', '') == self.msg_type and self.handler:
            #     data = msg.get('data')
            #     self.handler(context=self._context, message=data, channel=msg.get('channel'))  # 回调
            #     self.store_template(message=data, channel=msg.get('channel'))
