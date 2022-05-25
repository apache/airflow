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

/* global localStorage, ResizeObserver */

import React, { useState, useRef, useEffect } from 'react';
import {
  Table,
  Tbody,
  Box,
  Thead,
  Flex,
} from '@chakra-ui/react';

import { useGridData } from './api';
import renderTaskRows from './renderTaskRows';
import ResetRoot from './ResetRoot';
import DagRuns from './dagRuns';
import ToggleGroups from './ToggleGroups';
import { getMetaValue } from '../utils';
import AutoRefresh from './AutoRefresh';

const dagId = getMetaValue('dag_id');

const Grid = ({ isPanelOpen = false, hoveredTaskState }) => {
  const scrollRef = useRef();
  const tableRef = useRef();

  const { data: { groups, dagRuns } } = useGridData();
  const dagRunIds = dagRuns.map((dr) => dr.runId);

  const openGroupsKey = `${dagId}/open-groups`;
  const storedGroups = JSON.parse(localStorage.getItem(openGroupsKey)) || [];
  const [openGroupIds, setOpenGroupIds] = useState(storedGroups);

  const onToggleGroups = (groupIds) => {
    localStorage.setItem(openGroupsKey, JSON.stringify(groupIds));
    setOpenGroupIds(groupIds);
  };

  const scrollOnResize = new ResizeObserver(() => {
    const runsContainer = scrollRef.current;
    // Set scroll to top right if it is scrollable
    if (runsContainer && runsContainer.scrollWidth > runsContainer.clientWidth) {
      runsContainer.scrollBy(tableRef.current.offsetWidth, 0);
    }
  });

  useEffect(() => {
    if (tableRef && tableRef.current) {
      const table = tableRef.current;

      scrollOnResize.observe(table);
      return () => {
        scrollOnResize.unobserve(table);
      };
    }
    return () => {};
  }, [tableRef, scrollOnResize]);

  return (
    <Box
      position="relative"
      m={3}
      mt={0}
      overflow="auto"
      ref={scrollRef}
      flexGrow={1}
      minWidth={isPanelOpen && '300px'}
    >
      <Flex alignItems="center" position="sticky" top={0} left={0}>
        <AutoRefresh />
        <ToggleGroups
          groups={groups}
          openGroupIds={openGroupIds}
          onToggleGroups={onToggleGroups}
        />
        <ResetRoot />
      </Flex>
      <Table>
        <Thead display="block" pr="10px" position="sticky" top={0} zIndex={2} bg="white">
          <DagRuns />
        </Thead>
        {/* TODO: remove hardcoded values. 665px is roughly the total heade+footer height */}
        <Tbody
          display="block"
          width="100%"
          maxHeight="calc(100vh - 665px)"
          minHeight="500px"
          ref={tableRef}
          pr="10px"
        >
          {renderTaskRows({
            task: groups, dagRunIds, openGroupIds, onToggleGroups, hoveredTaskState,
          })}
        </Tbody>
      </Table>
    </Box>
  );
};

export default Grid;
