# -*- coding: utf-8 -*-
#
# The MIT License (MIT)
#
# Copyright (c) 2016 Marcos Cardoso
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from functools import wraps
from typing import Dict

from elasticsearch.client import _normalize_hosts
from tests.compat import patch

from .fake_elasticsearch import FakeElasticsearch

ELASTIC_INSTANCES = {}  # type: Dict[str, FakeElasticsearch]


def _get_elasticmock(hosts=None, *args, **kwargs):
    host = _normalize_hosts(hosts)[0]
    elastic_key = '{0}:{1}'.format(
        host.get('host', 'localhost'), host.get('port', 9200)
    )

    if elastic_key in ELASTIC_INSTANCES:
        connection = ELASTIC_INSTANCES.get(elastic_key)
    else:
        connection = FakeElasticsearch()
        ELASTIC_INSTANCES[elastic_key] = connection
    return connection


def elasticmock(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        ELASTIC_INSTANCES.clear()
        with patch('elasticsearch.Elasticsearch', _get_elasticmock):
            result = f(*args, **kwargs)
        return result
    return decorated
