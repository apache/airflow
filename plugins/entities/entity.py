# -*- coding:utf-8 -*-

from typing import Dict, Optional, Any
from plugins.utils.logger import generate_logger

_logger = generate_logger(__name__)


class ClsEntity(object):
    def __init__(self, *args, **kwargs):
        self._metadata = None  # type: Optional[Dict]
        self._channels = dict()  # type: Dict[Any]

    @property
    def channels(self):
        return self._channels

    def set_channel_handler(self, channel: str, handlerFn):
        self._channels[channel] = handlerFn

    @property
    def metadata(self):
        return self._metadata

    @metadata.setter
    def metadata(self, data):
        self._metadata = data

    def update_meta_data(self, data):
        self._metadata.update(data)

    @property
    def entity_id(self) -> Optional[str]:
        if not self._metadata:
            return None
        return self._metadata.get('entity_id', None)
