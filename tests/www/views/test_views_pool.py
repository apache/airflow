#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import flask
import markupsafe
import pytest

from airflow.models import Pool
from airflow.utils.session import create_session
from tests.test_utils.www import check_content_in_response, check_content_not_in_response

POOL = {
    'pool': 'test-pool',
    'slots': 777,
    'description': 'test-pool-description',
}


@pytest.fixture(autouse=True)
def clear_pools():
    with create_session() as session:
        session.query(Pool).delete()


@pytest.fixture()
def pool_factory(session):
    def factory(**values):
        pool = Pool(**{**POOL, **values})  # Passed in values override defaults.
        session.add(pool)
        session.commit()
        return pool

    return factory


def test_create_pool_with_same_name(admin_client):
    # create test pool
    resp = admin_client.post('/pool/add', data=POOL, follow_redirects=True)
    check_content_in_response('Added Row', resp)

    # create pool with the same name
    resp = admin_client.post('/pool/add', data=POOL, follow_redirects=True)
    check_content_in_response('Already exists.', resp)


def test_create_pool_with_empty_name(admin_client):
    resp = admin_client.post(
        '/pool/add',
        data={**POOL, "pool": ""},
        follow_redirects=True,
    )
    check_content_in_response('This field is required.', resp)


def test_odd_name(admin_client, pool_factory):
    pool_factory(pool="test-pool<script></script>")
    resp = admin_client.get('/pool/list/')
    check_content_in_response('test-pool&lt;script&gt;', resp)
    check_content_not_in_response('test-pool<script>', resp)


def test_list(app, admin_client, pool_factory):
    pool_factory(pool="test-pool")

    resp = admin_client.get('/pool/list/')
    # We should see this link
    with app.test_request_context():
        url = flask.url_for('TaskInstanceModelView.list', _flt_3_pool='test-pool', _flt_3_state='running')
        used_tag = markupsafe.Markup("<a href='{url}'>{slots}</a>").format(url=url, slots=0)

        url = flask.url_for('TaskInstanceModelView.list', _flt_3_pool='test-pool', _flt_3_state='queued')
        queued_tag = markupsafe.Markup("<a href='{url}'>{slots}</a>").format(url=url, slots=0)

        url = flask.url_for('TaskInstanceModelView.list', _flt_3_pool='test-pool', _flt_3_state='scheduled')
        scheduled_tag = markupsafe.Markup("<a href='{url}'>{slots}</a>").format(url=url, slots=0)

    check_content_in_response(used_tag, resp)
    check_content_in_response(queued_tag, resp)
    check_content_in_response(scheduled_tag, resp)


def test_pool_muldelete(session, admin_client, pool_factory):
    pool = pool_factory()

    resp = admin_client.post(
        "/pool/action_post",
        data={"action": "muldelete", "rowid": [pool.id]},
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert session.query(Pool).filter(Pool.id == pool.id).count() == 0


def test_pool_muldelete_default(session, admin_client, pool_factory):
    pool = pool_factory(pool="default_pool")

    resp = admin_client.post(
        "/pool/action_post",
        data={"action": "muldelete", "rowid": [pool.id]},
        follow_redirects=True,
    )
    check_content_in_response("default_pool cannot be deleted", resp)
    assert session.query(Pool).filter(Pool.id == pool.id).count() == 1
