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

import React, { useRef, useEffect } from 'react';
import {
  Table,
  Tbody,
  Box,
  Switch,
  FormControl,
  FormLabel,
  Spinner,
  Text,
  Thead,
} from '@chakra-ui/react';

import useTreeData from './useTreeData';
import renderTaskRows from './renderTaskRows';
import DagRuns from './dagRuns';

const Tree = () => {
  const containerRef = useRef();
  const scrollRef = useRef();
  const tableRef = useRef();
  const { data: { groups = {}, dagRuns = [] }, isRefreshOn, onToggleRefresh } = useTreeData();
  const dagRunIds = dagRuns.map((dr) => dr.runId);

  const tableWidth = tableRef && tableRef.current ? tableRef.current.offsetWidth : '100%';

  useEffect(() => {
    // Set initial scroll to far right if it is scrollable
    const runsContainer = scrollRef.current;
    if (runsContainer && runsContainer.scrollWidth > runsContainer.clientWidth) {
      runsContainer.scrollBy(runsContainer.clientWidth, 0);
    }
  }, [tableWidth]);

  return (
    <Box position="relative" ref={containerRef}>
      <FormControl display="flex" alignItems="center" justifyContent="flex-end" width="100%">
        {isRefreshOn && <Spinner color="blue.500" speed="1s" mr="4px" />}
        <FormLabel htmlFor="auto-refresh" mb={0} fontSize="12px" fontWeight="normal">
          Auto-refresh
        </FormLabel>
        <Switch id="auto-refresh" onChange={onToggleRefresh} isChecked={isRefreshOn} size="lg" />
      </FormControl>
      <Text transform="rotate(-90deg)" position="absolute" left="-6px" top="130px">Runs</Text>
      <Text transform="rotate(-90deg)" position="absolute" left="-6px" top="190px">Tasks</Text>
      <Box px="24px">
        <Box position="relative" width="100%" overflow="auto" ref={scrollRef}>
          <Table>
            <Thead display="block" pr="10px" position="sticky" top={0} zIndex={2} bg="white">
              <DagRuns containerRef={containerRef} tableWidth={tableWidth} />
            </Thead>
            {/* eslint-disable-next-line max-len */}
            {/* TODO: don't use hardcoded values. 665px is roughly the normal total header and footer heights */}
            <Tbody display="block" width="100%" maxHeight="calc(100vh - 665px)" minHeight="500px" ref={tableRef} pr="10px">
              {renderTaskRows({
                task: groups, containerRef, dagRunIds, tableWidth,
              })}
            </Tbody>
          </Table>
        </Box>
      </Box>
    </Box>
  );
};

export default Tree;
