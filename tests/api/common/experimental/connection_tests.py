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

import unittest
from airflow import settings, models
from airflow.api.common.experimental import connections
from airflow.exceptions import MissingArgument, MultipleConnectionsFound, IncompatibleArgument
from airflow.models.connection import Connection
from airflow.settings import Session


class ConnectionTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super(ConnectionTests, cls).setUpClass()
        cls._cleanup()

    def setUp(self):
        super(ConnectionTests, self).setUp()
        # from airflow.www_rbac import app as application
        # configuration.load_test_config()
        # self.app, self.appbuilder = application.create_app(session=Session, testing=True)
        # self.app.config['TESTING'] = True

        # self.dagbag = models.DagBag(dag_folder=DEV_NULL, include_examples=True)
        settings.configure_orm()
        self.session = Session

    def tearDown(self):
        self._cleanup(session=self.session)
        super(ConnectionTests, self).tearDown()

    @staticmethod
    def _cleanup(session=None):
        if session is None:
            session = Session()

        session.query(models.Pool).delete()
        session.query(models.Variable).delete()
        session.query(Connection).delete()
        session.commit()
        session.close()

    def test_add_connection(self):
        uri = 'postgres://airflow:airflow@host:5432/airflow'
        # add a connection using conn_uri
        self.assertEqual(connections.add_connection(
            conn_id="new1",
            conn_uri=uri
        ), Connection(conn_id="new1", uri=uri))

        # add a connection with all params defined
        self.assertEqual(connections.add_connection(
            conn_id="new2",
            conn_type="postgres",
            conn_host="host",
            conn_login="airflow",
            conn_password="airflow",
            conn_schema="airflow",
            conn_port="5432"
        ), Connection(
            conn_id="new2",
            conn_type="postgres",
            host="host",
            login="airflow",
            password="airflow",
            schema="airflow",
            port="5432"))

        # add a connection with extra
        con3_test = Connection(conn_id="new3", uri=uri)
        con3_test.set_extra("{'foo':'bar'}")
        self.assertEqual(connections.add_connection(
            conn_id="new3",
            conn_uri=uri,
            conn_extra="{'foo':'bar'}"
        ), con3_test)

        # add a duplicate connection`
        self.assertEqual(connections.add_connection(
            conn_id="new3",
            conn_uri=uri,
            conn_extra="{'foo':'bar'}"
        ), con3_test)

        # add a connection with just type
        self.assertEqual(connections.add_connection(
            conn_id="new4",
            conn_type="google_cloud_platform",
            conn_uri="",
            conn_extra="{'extra':'yes'}"
        ), Connection(conn_id="new4",
                      conn_type="google_cloud_platform",
                      uri="",
                      extra="{'extra':'yes'}"))

        # Attempt to add without providing conn_id
        with self.assertRaises(MissingArgument) as ma:
            connections.add_connection(conn_id=None, conn_uri=uri)
            self.assertEqual(ma.exception,
                             "The following args are required to add a connection:" +
                             " ['conn_id']")

        # Attempt to add without providing conn_uri
        with self.assertRaises(MissingArgument) as ma:
            connections.add_connection(conn_id="new5")
            self.assertEqual(ma.exception,
                             "The following args are required to add a connection:" +
                             " ['conn_id or conn_type']")

        # Attempt to add duplicate connection with different type
        with self.assertRaises(IncompatibleArgument) as ia:
            connections.add_connection(conn_id="new2",
                                       conn_type="not_postgres",
                                       conn_host="host",
                                       conn_login="airflow",
                                       conn_password="airflow",
                                       conn_schema="airflow",
                                       conn_port="5432")
            self.assertEqual(ia.exception, "Connections with the same id must all be of the same type")

        # validate expected connections reached the DB
        extra = {'new1': None,
                 'new2': None,
                 'new3': "{'foo':'bar'}", }

        for index in range(1, 5):
            conn_id = 'new%s' % index
            result = (self.session
                      .query(Connection)
                      .filter(Connection.conn_id == conn_id)
                      .first())
            result = (result.conn_id, result.conn_type, result.host,
                      result.port, result.get_extra())
            if conn_id in ['new1', 'new2', 'new3']:
                self.assertEqual(result, (conn_id, 'postgres', 'host', 5432,
                                          extra[conn_id]))
            elif conn_id == 'new4':
                self.assertEqual(result, (conn_id, 'google_cloud_platform',
                                          None, None, "{'extra':'yes'}"))

        # validate duplicate connections made it to the db
        dup_conns = (self.session
                     .query(Connection)
                     .filter(Connection.conn_id == 'new3')).all()
        self.assertEqual(2, len(dup_conns))
        for conn in dup_conns:
            result = (conn.conn_id, conn.conn_type, conn.host,
                      conn.port, conn.get_extra())
            self.assertEqual(result, ('new3', 'postgres', 'host', 5432,
                                      extra['new3']))

    def test_delete_connection(self):
        # add some conns to delete
        uri = 'postgres://airflow:airflow@host:5432/airflow'

        self.assertEqual(connections.add_connection(
            conn_id="new1",
            conn_uri=uri
        ), Connection(conn_id="new1", uri=uri))

        self.assertEqual(connections.add_connection(
            conn_id="new2",
            conn_uri=uri
        ), Connection(conn_id="new2", uri=uri))

        self.assertEqual(connections.add_connection(
            conn_id="new2",
            conn_uri=uri
        ), Connection(conn_id="new2", uri=uri))

        # delete one
        self.assertEqual(connections.delete_connection(conn_id='new1'),
                         "Successfully deleted `conn_id`=new1")

        # try to delete multiple with delete_all=False
        self.assertEqual(connections.delete_connection(conn_id='new2'),
                         'Found 2 connection with ' +
                         '`conn_id`=new2. Specify `delete_all=True` to remove all')

        # make sure none were deleted
        self.assertEqual(2, len((self.session.query(Connection)
                                 .filter(Connection.conn_id == 'new2').all())))

        self.assertEqual(connections.delete_connection(conn_id='new2', delete_all=True),
                         "Successfully deleted 2 connections with `conn_id`=new2")

        # make sure none were deleted
        self.assertEqual(0, len((self.session.query(Connection).all())))

        # Attempt to delete a non-existing connnection
        self.assertEqual(connections.delete_connection(conn_id='non_existent'),
                         "Did not find a connection with `conn_id`=non_existent")

        # Attempt to delete a non-existing connnection
        self.assertRaises(MissingArgument, connections.delete_connection, None)

        self.session.close()

    def test_list_connections(self):

        uri = 'postgres://airflow:airflow@host:5432/airflow'

        # add a connection using conn_uri
        self.assertEqual(connections.add_connection(
            conn_id="new1",
            conn_uri=uri
        ), Connection(conn_id="new1", uri=uri))

        # add a connection with all params defined
        self.assertEqual(connections.add_connection(
            conn_id="new2",
            conn_type="postgres",
            conn_host="host",
            conn_login="airflow",
            conn_password="airflow",
            conn_schema="airflow",
            conn_port="5432"
        ), Connection(
            conn_id="new2",
            conn_type="postgres",
            host="host",
            login="airflow",
            password="airflow",
            schema="airflow",
            port="5432"
        ))

        # add a connection with extra
        con3 = Connection(conn_id="new3",
                          uri=uri, )
        con3.set_extra("{'bar':'foo'}")

        self.assertEqual(connections.add_connection(
            conn_id="new3",
            conn_uri=uri,
            conn_extra="{'bar':'foo'}"
        ), con3)

        # add a duplicate connection`
        self.assertEqual(connections.add_connection(
            conn_id="new3",
            conn_uri=uri,
            conn_extra="{'bar':'foo'}"
        ), con3)

        # add a connection with just type
        con4 = Connection(conn_id="new4",
                          conn_type="google_cloud_platform")
        con4.set_extra("{'extra':'yes'}")

        self.assertEqual(connections.add_connection(
            conn_id="new4",
            conn_type="google_cloud_platform",
            conn_uri="",
            conn_extra="{'extra':'yes'}"
        ), con4)

        conns = list(map(lambda conn: (conn.conn_id, conn.conn_type, conn.host,
                                       conn.port, conn.is_extra_encrypted),
                         connections.list_connections()))

        self.assertEqual(conns,
                         [('new1', 'postgres', 'host', 5432, False),
                          ('new2', 'postgres', 'host', 5432, False),
                          ('new3', 'postgres', 'host', 5432, True),
                          ('new3', 'postgres', 'host', 5432, True),
                          ('new4', 'google_cloud_platform', None, None, True)])

    def test_update_connection(self):
        uri = 'postgres://airflow:airflow@host:5432/airflow'

        # add a connection using conn_uri
        self.assertEqual(connections.add_connection(
            conn_id="new1",
            conn_uri=uri
        ), Connection(conn_id="new1", uri=uri))

        # add a connection with all params defined
        self.assertEqual(connections.add_connection(
            conn_id="new2",
            conn_type="postgres",
            conn_host="host",
            conn_login="airflow",
            conn_password="airflow",
            conn_schema="airflow",
            conn_port="5432"
        ), Connection(
            conn_id="new2",
            conn_type="postgres",
            host="host",
            login="airflow",
            password="airflow",
            schema="airflow",
            port="5432"
        ))

        # add a connection with extra
        con3 = Connection(conn_id="new3",
                          uri=uri, )
        con3.set_extra("{'bar':'foo'}")

        self.assertEqual(connections.add_connection(
            conn_id="new3",
            conn_uri=uri,
            conn_extra="{'bar':'foo'}"
        ), con3)

        # add a duplicate connection`
        self.assertEqual(connections.add_connection(
            conn_id="new3",
            conn_uri=uri,
            conn_extra="{'bar':'foo'}"
        ), con3)

        # add a connection with just type
        con4 = Connection(conn_id="new4",
                          conn_type="google_cloud_platform")
        con4.set_extra("{'extra':'yes'}")

        self.assertEqual(connections.add_connection(
            conn_id="new4",
            conn_type="google_cloud_platform",
            conn_uri="",
            conn_extra="{'extra':'yes'}"
        ), con4)

        # Update Connections
        new_uri = 'postgres://airflow:different_password@host:1234/airflow'
        # update connection using conn_uri
        self.assertEqual(connections.update_connection(
            conn_id="new1",
            conn_uri=new_uri
        ), Connection(conn_id="new1",
                      uri=new_uri))

        # update connection with all params defined
        self.assertEqual(connections.update_connection(
            conn_id="new2",
            conn_type="postgres",
            conn_host="host",
            conn_login="airflow",
            conn_password="different_password",
            conn_schema="airflow",
            conn_port="1234"
        ), Connection(conn_id="new2",
                      conn_type="postgres",
                      host="host",
                      login="airflow",
                      password="different_password",
                      schema="airflow",
                      port="1234"))

        # update connection with just type
        con4 = Connection(conn_id="new4",
                          conn_type="random_type")
        con4.set_extra("{'extra':'yes'}")

        self.assertEqual(connections.update_connection(
            conn_id="new4",
            conn_type="random_type",
            conn_uri="",
            conn_extra="{'extra':'yes'}"
        ), con4)

        # validate updates reached the DB
        extra = {'new1': None,
                 'new2': None,
                 'new3': "{'bar':'foo'}",
                 'new4': "{'extra': 'yes'}", }

        for index in range(1, 4):
            conn_id = 'new%s' % index
            result = (self.session
                      .query(Connection)
                      .filter(Connection.conn_id == conn_id)
                      .first())
            result = (result.conn_id, result.conn_type, result.host,
                      result.port, result.get_extra())
            if conn_id in ['new1', 'new2']:
                self.assertEqual(result, (conn_id, 'postgres', 'host', 1234,
                                          extra[conn_id]))
            elif conn_id == 'new4':
                self.assertEqual(result, (conn_id, 'random_type',
                                          None, None, extra[conn_id]))

        # try to update duplicate connection
        self.assertRaises(MultipleConnectionsFound, connections.update_connection, conn_id="new3",
                          conn_uri=new_uri,
                          conn_extra="{'foo':'bar'}")

        # validate duplicate connections did not update in db
        dup_conns = (self.session
                     .query(Connection)
                     .filter(Connection.conn_id == 'new3')).all()
        self.assertEqual(2, len(dup_conns))
        for conn in dup_conns:
            result = (conn.conn_id, conn.conn_type, conn.host,
                      conn.port, conn.get_extra())
            self.assertEqual(result, ('new3', 'postgres', 'host', 5432,
                                      extra['new3']))

        # Attempt to udpate without providing conn_uri
        self.assertRaises(MissingArgument, connections.update_connection, conn_id="new1")
