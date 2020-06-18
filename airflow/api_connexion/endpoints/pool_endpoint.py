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
from flask import request
from sqlalchemy import func

from airflow.api_connexion import parameters
from airflow.api_connexion.exceptions import NotFound
from airflow.api_connexion.schemas.pool_schema import PoolCollection, pool_collection_schema, pool_schema
from airflow.models.pool import Pool
from airflow.utils.session import provide_session


def delete_pool():
    """
    Delete a pool
    """
    raise NotImplementedError("Not implemented yet.")


@provide_session
def get_pool(pool_name, session):
    """
    Get a pool
    """
    pool_id = pool_name
    query = session.query(Pool)
    obj = query.filter(Pool.pool == pool_id).one_or_none()

    if obj is None:
        raise NotFound("Pool not found")
    return pool_schema.dump(obj)


@provide_session
def get_pools(session):
    """
    Get all pools
    """
    offset = request.args.get(parameters.page_offset, 0)
    limit = min(int(request.args.get(parameters.page_limit, 100)), 100)

    total_entries = session.query(func.count(Pool.id)).scalar()
    query = session.query(Pool).order_by(Pool.id).offset(offset).limit(limit)

    pools = query.all()
    return pool_collection_schema.dump(PoolCollection(pools=pools, total_entries=total_entries)).data


def patch_pool():
    """
    Update a pool
    """
    raise NotImplementedError("Not implemented yet.")


def post_pool():
    """
    Create aa pool
    """
    raise NotImplementedError("Not implemented yet.")
