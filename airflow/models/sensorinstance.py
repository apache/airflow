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

import json

from sqlalchemy import BigInteger, Column, Index, Integer, String, Text

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models.base import ID_LEN, Base
from airflow.utils import timezone
from airflow.utils.session import provide_session
from airflow.utils.sqlalchemy import UtcDateTime
from airflow.utils.state import State


class SensorInstance(Base):
    """
    SensorInstance support the smart sensor service. It stores the sensor task states
    and context that required for poking include poke context and execution context.
    In sensor_instance table we also save the sensor operator classpath so that inside
    smart sensor there is no need to import the dagbag and create task object for each
    sensor task.

    SensorInstance include another set of columns to support the smart sensor shard on
    large number of sensor instance. The key idea is to generate the hash code from the
    poke context and use it to map to a shorter shard code which can be used as an index.
    Every smart sensor process takes care of tasks whose `shardcode` are in a certain range.

    """

    __tablename__ = "sensor_instance"

    id = Column(Integer, primary_key=True)
    task_id = Column(String(ID_LEN), nullable=False)
    dag_id = Column(String(ID_LEN), nullable=False)
    execution_date = Column(UtcDateTime, nullable=False)
    state = Column(String(20))
    _try_number = Column('try_number', Integer, default=0)
    start_date = Column(UtcDateTime)
    operator = Column(String(1000), nullable=False)
    op_classpath = Column(String(1000), nullable=False)
    hashcode = Column(BigInteger, nullable=False)
    shardcode = Column(Integer, nullable=False)
    poke_context = Column(Text, nullable=False)
    execution_context = Column(Text)
    created_at = Column(UtcDateTime, default=timezone.utcnow(), nullable=False)
    updated_at = Column(UtcDateTime,
                        default=timezone.utcnow(),
                        onupdate=timezone.utcnow(),
                        nullable=False)

    __table_args__ = (
        Index('ti_primary_key', dag_id, task_id, execution_date, unique=True),

        Index('si_hashcode', hashcode),
        Index('si_shardcode', shardcode),
        Index('si_state_shard', state, shardcode),
        Index('si_updated_at', updated_at),
    )

    def __init__(self, ti):
        self.dag_id = ti.dag_id
        self.task_id = ti.task_id
        self.execution_date = ti.execution_date

    @staticmethod
    def get_classpath(obj):
        """
        Get the object dotted class path. Used for getting operator classpath.

        :param obj:
        :type obj:
        :return: The class path of input object
        :rtype: str
        """
        module_name, class_name = obj.__module__, obj.__class__.__name__

        return module_name + "." + class_name

    @classmethod
    @provide_session
    def register(cls, ti, poke_context, execution_context, session=None):
        """
        Register task instance ti for a sensor in sensor_instance table. Persist the
        context used for a sensor and set the sensor_instance table state to sensing.

        :param ti: The task instance for the sensor to be registered.
        :type: ti:
        :param poke_context: Context used for sensor poke function.
        :type poke_context: dict
        :param execution_context: Context used for execute sensor such as timeout
            setting and email configuration.
        :type execution_context: dict
        :param session: SQLAlchemy ORM Session
        :type session: Session
        :return: True if the ti was registered successfully.
        :rtype: Boolean
        """
        if poke_context is None:
            raise AirflowException('poke_context should not be None')

        encoded_poke = json.dumps(poke_context)
        encoded_execution_context = json.dumps(execution_context)

        sensor = session.query(SensorInstance).filter(
            SensorInstance.dag_id == ti.dag_id,
            SensorInstance.task_id == ti.task_id,
            SensorInstance.execution_date == ti.execution_date
        ).with_for_update().first()

        if sensor is None:
            sensor = SensorInstance(ti=ti)

        sensor.operator = ti.operator
        sensor.op_classpath = SensorInstance.get_classpath(ti.task)
        sensor.poke_context = encoded_poke
        sensor.execution_context = encoded_execution_context

        sensor.hashcode = hash(encoded_poke)
        sensor.shardcode = sensor.hashcode % conf.getint('smart_sensor', 'shard_code_upper_limit')
        sensor.try_number = ti.try_number

        sensor.state = State.SENSING
        sensor.start_date = timezone.utcnow()
        session.add(sensor)
        session.commit()

        return True

    @property
    def try_number(self):
        """
        Return the try number that this task number will be when it is actually
        run.
        If the TI is currently running, this will match the column in the
        database, in all other cases this will be incremented.
        """
        # This is designed so that task logs end up in the right file.
        if self.state in State.running():
            return self._try_number
        return self._try_number + 1

    @try_number.setter
    def try_number(self, value):
        self._try_number = value

    def __repr__(self):
        return "<{self.__class__.__name__}: id: {self.id} poke_context: {self.poke_context} " \
               "execution_context: {self.execution_context} state: {self.state}>".format(
                   self=self)
