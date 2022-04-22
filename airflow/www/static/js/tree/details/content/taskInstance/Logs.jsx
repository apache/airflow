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
  Text,
  Box,
  Button,
  Flex,
  Link,
  Divider,
} from '@chakra-ui/react';

import { getMetaValue } from '../../../../utils';

const logsWithMetadataUrl = getMetaValue('logs_with_metadata_url');
const showExternalLogRedirect = getMetaValue('show_external_log_redirect') === 'True';
const externalLogUrl = getMetaValue('external_log_url');
const externalLogName = getMetaValue('external_log_name');

const LinkButton = ({ children, ...rest }) => (<Button as={Link} variant="ghost" colorScheme="blue" {...rest}>{children}</Button>);

const Logs = ({
  dagId,
  taskId,
  executionDate,
  tryNumber,
}) => {
  const externalLogs = [];

  const logAttempts = [...Array(tryNumber + 1 || 0)].map((_, index) => {
    if (index === 0 && tryNumber < 2) return null;

    const isExternal = index !== 0 && showExternalLogRedirect;

    if (isExternal) {
      const fullExternalUrl = `${externalLogUrl
      }?dag_id=${encodeURIComponent(dagId)
      }&task_id=${encodeURIComponent(taskId)
      }&execution_date=${encodeURIComponent(executionDate)
      }&try_number=${index}`;
      externalLogs.push(
        <LinkButton
          // eslint-disable-next-line react/no-array-index-key
          key={index}
          href={fullExternalUrl}
          target="_blank"
        >
          {index}
        </LinkButton>,
      );
    }

    const fullMetadataUrl = `${logsWithMetadataUrl
    }?dag_id=${encodeURIComponent(dagId)
    }&task_id=${encodeURIComponent(taskId)
    }&execution_date=${encodeURIComponent(executionDate)
    }&format=file${index > 0 && `&try_number=${index}`}`;

    return (
      <LinkButton
        // eslint-disable-next-line react/no-array-index-key
        key={index}
        href={fullMetadataUrl}
      >
        {index === 0 ? 'All' : index}
      </LinkButton>
    );
  });

  return (
    <>
      {tryNumber > 0 && (
      <>
        <Box>
          <Text>Download Log (by attempts):</Text>
          <Flex flexWrap="wrap">
            {logAttempts}
          </Flex>
        </Box>
        <Divider my={2} />
      </>
      )}
      {externalLogName && externalLogs.length > 0 && (
      <>
        <Box>
          <Text>
            View Logs in
            {' '}
            {externalLogName}
            {' '}
            (by attempts):
          </Text>
          <Flex flexWrap="wrap">
            {externalLogs}
          </Flex>
        </Box>
        <Divider my={2} />
      </>
      )}
    </>
  );
};

export default Logs;
