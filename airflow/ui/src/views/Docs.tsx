/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import React from 'react';
import {
  Box,
  Button,
  Flex,
  Heading,
  Icon,
  Link,
  List,
  ListItem,
  Text,
  useColorModeValue,
} from '@chakra-ui/react';
import { FaGithub } from 'react-icons/fa';
import { FiExternalLink, FiGlobe } from 'react-icons/fi';

import AppContainer from 'containers/AppContainer';

const Docs: React.FC = () => (
  <AppContainer>
    <Box mx="auto" my={8} maxWidth="900px">
      <Flex mt={8}>
        <Box flex="1">
          <Heading as="h1">Documentation</Heading>
          <Text mt={4}>
            Apache Airflow Core, which includes webserver, scheduler, CLI and other components that
            are needed for minimal Airflow&nbsp;installation.
          </Text>
          <Button
            as="a"
            href="https://airflow.apache.org/docs/apache-airflow/stable/index.html"
            variant="solid"
            rightIcon={<FiExternalLink />}
            mt={4}
            target="_blank"
            rel="noopener noreferrer"
          >
            Apache Airflow Docs
          </Button>
        </Box>
        <Box ml={8} p={4} bg={useColorModeValue('gray.100', 'gray.700')} borderRadius="md">
          <Heading as="h3" size="sm">Links</Heading>
          <List mt={4} spacing={2}>
            <ListItem>
              <Link href="https://airflow.apache.org/" isExternal color="teal.500">
                <Icon as={FiGlobe} mr={1} />
                Apache Airflow Website
              </Link>
            </ListItem>
            <ListItem>
              <Link href="https://github.com/apache/airflow" isExternal color="teal.500">
                <Icon as={FaGithub} mr={1} />
                apache/airflow on GitHub
              </Link>
            </ListItem>
          </List>
        </Box>
      </Flex>
      <Box mt={10}>
        <Heading as="h3" size="lg">REST API Reference</Heading>
        <Flex mt={4}>
          <Button
            as="a"
            href="http://127.0.0.1:28080/api/v1/ui/"
            variant="outline"
            rightIcon={<FiExternalLink />}
            mr={2}
          >
            Swagger
          </Button>
          <Button
            as="a"
            href="http://127.0.0.1:28080/redoc"
            variant="outline"
            rightIcon={<FiExternalLink />}
          >
            Redoc
          </Button>
        </Flex>
      </Box>
      <Box mt={10}>
        <Heading as="h3" size="lg">Providers Packages</Heading>
        <Text mt={4}>
          Providers packages include integrations with third party integrations.
          They are updated independently of the Apache Airflow&nbsp;core.
        </Text>

        <List spacing={2} mt={4} style={{ columns: 3 }}>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/index.html" isExternal color="teal.500">Amazon</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-apache-beam/stable/index.html" isExternal color="teal.500">Apache Beam</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-apache-cassandra/stable/index.html" isExternal color="teal.500">Apache Cassandra</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-apache-druid/stable/index.html" isExternal color="teal.500">Apache Druid</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-apache-hdfs/stable/index.html" isExternal color="teal.500">Apache HDFS</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-apache-hive/stable/index.html" isExternal color="teal.500">Apache Hive</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-apache-kylin/stable/index.html" isExternal color="teal.500">Apache Kylin</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-apache-livy/stable/index.html" isExternal color="teal.500">Apache Livy</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-apache-pig/stable/index.html" isExternal color="teal.500">Apache Pig</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-apache-pinot/stable/index.html" isExternal color="teal.500">Apache Pinot</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/index.html" isExternal color="teal.500">Apache Spark</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-apache-sqoop/stable/index.html" isExternal color="teal.500">Apache Sqoop</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-celery/stable/index.html" isExternal color="teal.500">Celery</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-cloudant/stable/index.html" isExternal color="teal.500">IBM Cloudant</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/index.html" isExternal color="teal.500">Kubernetes</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/index.html" isExternal color="teal.500">Databricks</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-datadog/stable/index.html" isExternal color="teal.500">Datadog</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-dingding/stable/index.html" isExternal color="teal.500">Dingding</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-discord/stable/index.html" isExternal color="teal.500">Discord</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-docker/stable/index.html" isExternal color="teal.500">Docker</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-elasticsearch/stable/index.html" isExternal color="teal.500">Elasticsearch</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-exasol/stable/index.html" isExternal color="teal.500">Exasol</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-facebook/stable/index.html" isExternal color="teal.500">Facebook</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-ftp/stable/index.html" isExternal color="teal.500">File Transfer Protocol (FTP)</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-google/stable/index.html" isExternal color="teal.500">Google</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-grpc/stable/index.html" isExternal color="teal.500">gRPC</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-hashicorp/stable/index.html" isExternal color="teal.500">Hashicorp</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-http/stable/index.html" isExternal color="teal.500">Hypertext Transfer Protocol (HTTP)</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-imap/stable/index.html" isExternal color="teal.500">Internet Message Access Protocol (IMAP)</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-jdbc/stable/index.html" isExternal color="teal.500">Java Database Connectivity (JDBC)</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-jenkins/stable/index.html" isExternal color="teal.500">Jenkins</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-jira/stable/index.html" isExternal color="teal.500">Jira</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-microsoft-azure/stable/index.html" isExternal color="teal.500">Microsoft Azure</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-microsoft-mssql/stable/index.html" isExternal color="teal.500">Microsoft SQL Server (MSSQL)</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-microsoft-winrm/stable/index.html" isExternal color="teal.500">Windows Remote Management (WinRM)</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-mongo/stable/index.html" isExternal color="teal.500">MongoDB</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-mysql/stable/index.html" isExternal color="teal.500">MySQL</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-neo4j/stable/index.html" isExternal color="teal.500">Neo4J</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-odbc/stable/index.html" isExternal color="teal.500">ODBC</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-openfaas/stable/index.html" isExternal color="teal.500">OpenFaaS</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-opsgenie/stable/index.html" isExternal color="teal.500">Opsgenie</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-oracle/stable/index.html" isExternal color="teal.500">Oracle</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-pagerduty/stable/index.html" isExternal color="teal.500">Pagerduty</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-papermill/stable/index.html" isExternal color="teal.500">Papermill</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-plexus/stable/index.html" isExternal color="teal.500">Plexus</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/index.html" isExternal color="teal.500">PostgreSQL</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-presto/stable/index.html" isExternal color="teal.500">Presto</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-qubole/stable/index.html" isExternal color="teal.500">Qubole</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-redis/stable/index.html" isExternal color="teal.500">Redis</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-salesforce/stable/index.html" isExternal color="teal.500">Salesforce</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-samba/stable/index.html" isExternal color="teal.500">Samba</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-segment/stable/index.html" isExternal color="teal.500">Segment</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-sendgrid/stable/index.html" isExternal color="teal.500">Sendgrid</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-sftp/stable/index.html" isExternal color="teal.500">SFTP</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-singularity/stable/index.html" isExternal color="teal.500">Singularity</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-slack/stable/index.html" isExternal color="teal.500">Slack</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/index.html" isExternal color="teal.500">Snowflake</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-sqlite/stable/index.html" isExternal color="teal.500">SQLite</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-ssh/stable/index.html" isExternal color="teal.500">SSH</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-telegram/stable/index.html" isExternal color="teal.500">Telegram</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-vertica/stable/index.html" isExternal color="teal.500">Vertica</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-yandex/stable/index.html" isExternal color="teal.500">Yandex</Link></ListItem>
          <ListItem><Link href="https://airflow.apache.org/docs/apache-airflow-providers-zendesk/stable/index.html" isExternal color="teal.500">Zendesk</Link></ListItem>
        </List>
      </Box>
    </Box>
  </AppContainer>
);

export default Docs;
