# -*- coding:utf-8 -*-


from unittest import TestCase
import aioredis
from aioredis import Redis
import json
import time
import asyncio
import os

# 曲线模板数据
tempalte_data = {
    'nut_no': 'dummy_nut',
    'craft_type': 0,
    'curve_mode': 1,
    'curve_param': {
        "torque_min": 0.0,
        "torque_up_limit_min": 0,
        "torque_up_limit_max": 0,
        "compare_threshold": 0,
        "slope_threshold": 0,
        "torque_diff_threshold": 0,

        # 角度对比阈值
        "angle_threshold": 0,

        # 停止模式 （0: 正常停止 1:软停止）
        "stop_mode": 0,

    },
    'template_cluster': {
        'algorithm_version': 0,
        'curve_template_group_array': [{
            'template_centroid_index': 0,
            'template_data_array': [
                {
                    # 曲线角度模板
                    'template_angle': [0.0],

                    # 曲线扭矩模板
                    'template_torque': [0.0],

                    # 曲线启动点
                    'start_point': 0
                }
            ]
        }]
    }
}


class TestPublishCurveTemplate(TestCase):

    async def do_publish(self):
        redis: Redis = await aioredis.create_redis_pool('redis://localhost', db=1, password=None)
        time.sleep(1)
        i = 0
        template_prefix = os.environ.get('TEMPLATE_PREFIX', 'qcos_templates')
        while True:
            i += 1
            body = json.dumps(tempalte_data)
            key = '{}.{}'.format(template_prefix, str(i))
            await redis.publish(key, body)
            time.sleep(1)

    def test_publish(self):
        asyncio.run(self.do_publish())
