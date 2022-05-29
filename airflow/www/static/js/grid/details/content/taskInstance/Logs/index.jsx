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
  Flex,
  Divider,
} from '@chakra-ui/react';

import { getMetaValue } from '../../../../../utils';
import { LogInternalLink, LogExternalLink } from './LogLinks';

const showExternalLogRedirect = getMetaValue('show_external_log_redirect') === 'True';
const externalLogName = getMetaValue('external_log_name');

const getLinkIndexes = (tryNumber) => {
  const internalIndexes = [];
  const externalIndexes = [];

  [...Array(tryNumber + 1 || 0)].forEach((_, index) => {
    if (index === 0 && tryNumber < 2) return;
    const isExternal = index !== 0 && showExternalLogRedirect;
    if (isExternal) {
      externalIndexes.push(index);
    } else {
      internalIndexes.push(index);
    }
  });

  return [internalIndexes, externalIndexes];
};

const Logs = ({
  dagId,
  taskId,
  executionDate,
  tryNumber,
}) => {
  const [internalIndexes, externalIndexes] = getLinkIndexes(tryNumber);

  return (
    <>
      {tryNumber > 0 && (
      <>
        <Box>
          <Text>Download Log (by attempts):</Text>
          <Flex flexWrap="wrap">
            {
              internalIndexes.map(
                (index) => (
                  <LogInternalLink
                    key={index}
                    index={index}
                    dagId={dagId}
                    taskId={taskId}
                    executionDate={executionDate}
                  />
                ),
              )
            }
          </Flex>
        </Box>
        <Divider my={2} />
      </>
      )}
      {externalLogName && externalIndexes.length > 0 && (
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
            {
              externalIndexes.map(
                (index) => (
                  <LogExternalLink
                    key={index}
                    index={index}
                    dagId={dagId}
                    taskId={taskId}
                    executionDate={executionDate}
                  />
                ),
              )
            }
          </Flex>
        </Box>
        <Divider my={2} />
      </>
      )}
    </>
  );
};

export default Logs;
