# -*- coding: utf-8 -*-
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

from sqlalchemy import Column, String, Integer, Boolean, ForeignKey, Text
from sqlalchemy.orm import relationship

from airflow.models.base import Base, ID_LEN
from airflow.models.user import User
from airflow.utils.sqlalchemy import UtcDateTime
from airflow.utils import timezone


class Chart(Base):
    __tablename__ = "chart"

    id = Column(Integer, primary_key=True)
    label = Column(String(200))
    conn_id = Column(String(ID_LEN), nullable=False)
    user_id = Column(Integer(), ForeignKey('users.id'), nullable=True)
    chart_type = Column(String(100), default="line")
    sql_layout = Column(String(50), default="series")
    sql = Column(Text, default="SELECT series, x, y FROM table")
    y_log_scale = Column(Boolean)
    show_datatable = Column(Boolean)
    show_sql = Column(Boolean, default=True)
    height = Column(Integer, default=600)
    default_params = Column(String(5000), default="{}")
    owner = relationship(
        User, cascade=False, cascade_backrefs=False, backref='charts')
    x_is_date = Column(Boolean, default=True)
    iteration_no = Column(Integer, default=0)
    last_modified = Column(UtcDateTime, default=timezone.utcnow)

    def __repr__(self):
        return self.label
